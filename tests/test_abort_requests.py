import asyncio

import pytest

from arq.abort import cleanup_abort_requests
from arq.constants import abort_jobs_ss, in_progress_key_prefix
from arq.jobs import Job, JobStatus
from arq.utils import timestamp_ms
from arq.worker import func


async def test_abort_propagates_caller_cancellation(arq_redis):
    job = await arq_redis.enqueue_job('pending', _job_id='pending')
    task = asyncio.create_task(job.abort(poll_delay=0.001))
    try:
        while await arq_redis.zscore(abort_jobs_ss, job.job_id) is None:
            await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert await job.status() == JobStatus.queued
        assert await arq_redis.zscore(abort_jobs_ss, job.job_id) is not None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cleanup_preserves_live_and_recent_requests(arq_redis, worker):
    async def must_not_run(ctx):
        pytest.fail('The pending abort must be handled before starting the function')

    queued = await arq_redis.enqueue_job('must_not_run', _job_id='queued')
    await arq_redis.enqueue_job('must_not_run', _job_id='other-queue', _queue_name='other')
    await arq_redis.set(in_progress_key_prefix + 'running', '1')
    now = timestamp_ms()
    await arq_redis.zadd(
        abort_jobs_ss,
        {
            'orphan': now - 61_000,
            'queued': now - 61_000,
            'other-queue': now - 61_000,
            'running': now - 61_000,
            'recent': now - 100,
            'future': now + 1000,
        },
    )

    await cleanup_abort_requests(arq_redis, 0)
    assert set(await arq_redis.zrange(abort_jobs_ss, 0, -1)) == {
        b'queued',
        b'other-queue',
        b'running',
        b'recent',
        b'future',
    }

    w = worker(functions=[func(must_not_run, name='must_not_run')], allow_abort_jobs=True)
    await w.main()
    info = await queued.result_info()
    assert info.function == 'must_not_run'
    assert isinstance(info.result, asyncio.CancelledError)
    assert await queued.abort(timeout=0) is True
    assert w.jobs_failed == 1


async def test_cleanup_advances_past_live_requests(arq_redis):
    now = timestamp_ms()
    await arq_redis.enqueue_job('pending', _job_id='live')
    await arq_redis.zadd(abort_jobs_ss, {'live': now - 120_000})
    await arq_redis.zadd(abort_jobs_ss, {f'orphan-{i}': now - 61_000 for i in range(1200)})

    cursor = 0
    for _ in range(100):
        cursor = await cleanup_abort_requests(arq_redis, cursor)
        if cursor == 0:
            break
    else:
        pytest.fail('Cleanup did not finish its incremental scan')

    assert await arq_redis.zrange(abort_jobs_ss, 0, -1) == [b'live']


async def test_cleanup_does_not_delete_a_refreshed_request(arq_redis):
    await arq_redis.zadd(abort_jobs_ss, {'refreshed': timestamp_ms() - 61_000})
    now = timestamp_ms()
    await asyncio.gather(
        cleanup_abort_requests(arq_redis, 0),
        arq_redis.zadd(abort_jobs_ss, {'refreshed': now}),
    )
    assert await arq_redis.zscore(abort_jobs_ss, 'refreshed') == now


async def test_abort_normal_failure_is_not_cancellation(arq_redis, worker):
    async def fail(ctx):
        raise ValueError('failure')

    job = await arq_redis.enqueue_job('fail')
    await worker(functions=[func(fail, name='fail')]).main()
    with pytest.raises(ValueError, match='failure'):
        await job.abort(timeout=0)


async def test_abort_without_retained_result_is_unconfirmed(arq_redis, worker):
    async def must_not_run(ctx):
        pytest.fail('Aborted jobs must not run')

    job = await arq_redis.enqueue_job('must_not_run')
    with pytest.raises(asyncio.TimeoutError):
        await job.abort(timeout=0)

    await worker(functions=[func(must_not_run, name='must_not_run')], allow_abort_jobs=True, keep_result=0).main()
    assert await job.abort(timeout=0) is False
    assert await Job(job.job_id, arq_redis).result_info() is None


async def test_reused_job_id_can_abort_after_suppressed_cancellation(arq_redis, worker):
    started = asyncio.Event()
    runs = 0

    async def sometimes_suppress(ctx):
        nonlocal runs
        runs += 1
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            if runs != 1:
                raise

    w = worker(functions=[func(sometimes_suppress, name='job')], allow_abort_jobs=True, keep_result=0)
    for _ in range(2):
        started.clear()
        await arq_redis.enqueue_job('job', _job_id='reused')
        run = asyncio.create_task(w.main())
        await started.wait()
        await arq_redis.zadd(abort_jobs_ss, {'reused': timestamp_ms()})
        await run
        assert w.aborting_tasks == set()

    assert w.jobs_complete == 1
    assert w.jobs_failed == 1


async def test_fresh_request_can_cancel_a_task_that_suppressed_one_request(arq_redis, worker):
    started = asyncio.Event()
    suppressed = asyncio.Event()

    async def suppress_once(ctx):
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            suppressed.set()
            await asyncio.sleep(3600)

    await arq_redis.enqueue_job('job', _job_id='running')
    w = worker(functions=[func(suppress_once, name='job')], allow_abort_jobs=True)
    run = asyncio.create_task(w.main())
    await started.wait()
    await arq_redis.zadd(abort_jobs_ss, {'running': timestamp_ms()})
    await suppressed.wait()
    await arq_redis.zadd(abort_jobs_ss, {'running': timestamp_ms()})
    await run

    assert w.jobs_failed == 1
    assert w.aborting_tasks == set()
