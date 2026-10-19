import asyncio

import pytest

from arq.constants import abort_jobs_ss, default_queue_name, in_progress_key_prefix, job_key_prefix
from arq.jobs import JobStatus
from arq.utils import timestamp_ms
from arq.worker import func


async def test_bulk_abort_defers_no_work_and_confirms_via_results(arq_redis, worker):
    async def must_not_run(ctx):
        pytest.fail('Cancellation must precede execution')

    jobs = [await arq_redis.enqueue_job('job', _defer_by=3600) for _ in range(3)]
    await arq_redis.request_abort_jobs(job.job_id for job in jobs)
    assert [await job.status() for job in jobs] == [JobStatus.queued] * 3
    await worker(functions=[func(must_not_run, name='job')], allow_abort_jobs=True).main()
    assert [await job.abort(timeout=0) for job in jobs] == [True] * 3
    assert await arq_redis.zcard(abort_jobs_ss) == 0


async def test_bulk_deduplicates_and_bounds_submission(arq_redis, mocker):
    ids = [f'job-{i}' for i in range(201)]
    await arq_redis.zadd(default_queue_name, dict.fromkeys(ids, timestamp_ms()))
    mock_eval = mocker.spy(arq_redis, 'eval')
    await arq_redis.request_abort_jobs(ids + ids)
    assert mock_eval.call_count == 3
    assert await arq_redis.zcard(abort_jobs_ss) == 201

    mock_eval.reset_mock()
    await arq_redis.request_abort_jobs([])
    mock_eval.assert_not_called()


async def test_bulk_uses_custom_queue_without_deserializing(arq_redis):
    arq_redis.default_queue_name = 'custom'
    await arq_redis.zadd('custom', {'corrupt': timestamp_ms() + 60_000})
    await arq_redis.set(job_key_prefix + 'corrupt', b'not pickle')
    await arq_redis.zadd(default_queue_name, {'untouched': timestamp_ms() + 60_000})

    await arq_redis.request_abort_jobs(['corrupt', 'untouched', 'missing'])
    assert await arq_redis.zrange(abort_jobs_ss, 0, -1) == [b'corrupt']
    assert await arq_redis.zscore('custom', 'corrupt') == 1
    assert await arq_redis.zscore('custom', 'missing') is None
    assert await arq_redis.zscore(default_queue_name, 'untouched') > timestamp_ms()

    await arq_redis.request_abort_jobs(['untouched'], queue_name=default_queue_name)
    assert await arq_redis.zscore(default_queue_name, 'untouched') == 1


async def test_bulk_can_cancel_an_in_progress_job_without_queue_entry(arq_redis):
    await arq_redis.set(in_progress_key_prefix + 'running', '1')
    await arq_redis.request_abort_jobs(['running'])
    assert await arq_redis.zscore(abort_jobs_ss, 'running') is not None
    assert await arq_redis.zscore(default_queue_name, 'running') is None


async def test_bulk_completion_race_never_recreates_queue_entry(arq_redis):
    job = await arq_redis.enqueue_job('job', _defer_by=3600)

    async def finish():
        async with arq_redis.pipeline(transaction=True) as pipe:
            pipe.zrem(default_queue_name, job.job_id)
            pipe.zrem(abort_jobs_ss, job.job_id)
            pipe.delete(job_key_prefix + job.job_id)
            await pipe.execute()

    await asyncio.gather(arq_redis.request_abort_jobs([job.job_id]), finish())
    assert await arq_redis.zcard(default_queue_name) == 0
    assert await arq_redis.zcard(abort_jobs_ss) == 0


async def test_bulk_rejects_bare_string(arq_redis):
    with pytest.raises(TypeError, match='single string'):
        await arq_redis.request_abort_jobs('job-id')
    assert await arq_redis.zcard(abort_jobs_ss) == 0


async def test_bulk_failure_preserves_submitted_batches(arq_redis, mocker):
    ids = [f'job-{i}' for i in range(101)]
    await arq_redis.zadd(default_queue_name, dict.fromkeys(ids, timestamp_ms()))
    evaluate = arq_redis.eval
    calls = 0

    async def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise asyncio.CancelledError()
        return await evaluate(*args)

    mocker.patch.object(arq_redis, 'eval', side_effect=fail_second)
    with pytest.raises(asyncio.CancelledError):
        await arq_redis.request_abort_jobs(ids)
    assert await arq_redis.zcard(abort_jobs_ss) == 100
