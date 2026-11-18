from datetime import datetime, timezone

import pytest

from arq import Retry, cron
from arq.constants import in_progress_key_prefix, job_key_prefix, retry_key_prefix
from arq.schedules import CronSchedule, schedule_applied_key, schedule_key, update_cron_schedule
from arq.worker import Worker


async def transfer(ctx):
    pass


async def test_live_schedule_replaces_waiting_run(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    first = CronSchedule(1, frozenset({5, 10}))
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=first)
    worker = Worker(
        cron_jobs=[cron(transfer, name='transfer', job_id=job_id, dynamic=True)],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
    )
    worker._pool = arq_redis
    now = datetime(2030, 1, 1, 0, 4, 59, tzinfo=timezone.utc)
    await worker.run_cron(now, 1)
    await worker.run_cron(now, 1)
    assert await arq_redis.zcard(queue) == 1

    second = CronSchedule(2, frozenset({20}))
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=second)
    await worker.run_cron(now, 1)
    assert worker.cron_jobs[0].next_run.minute == 20
    assert await arq_redis.zcard(queue) == 0

    await worker.run_cron(now.replace(minute=19), 1)
    assert await arq_redis.zcard(queue) == 1


async def test_stale_replica_cannot_enqueue_or_replace_new_schedule(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    old = CronSchedule(1, frozenset({5}))
    new = CronSchedule(2, frozenset({10}))
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=new)
    assert not await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=old)
    result = await arq_redis.enqueue_job(
        'transfer',
        _job_id=job_id,
        _queue_name=queue,
        _cron_schedule=(schedule_key(queue, job_id), old.encode()),
    )
    assert result is None
    assert await arq_redis.get(schedule_key(queue, job_id)) == new.encode()


async def test_disabling_preserves_running_copy_and_reenable(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    await arq_redis.enqueue_job('transfer', _job_id=job_id, _queue_name=queue)
    await arq_redis.set(in_progress_key_prefix + job_id, b'1')
    await arq_redis.set(retry_key_prefix + job_id, b'2')
    disabled = CronSchedule(1, frozenset())
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=disabled)
    assert await arq_redis.exists(job_key_prefix + job_id)
    assert await arq_redis.exists(in_progress_key_prefix + job_id)
    assert await arq_redis.get(retry_key_prefix + job_id) == b'2'

    with pytest.raises(ValueError, match='Conflicting'):
        await update_cron_schedule(
            arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(1, frozenset({10}))
        )

    await arq_redis.delete(in_progress_key_prefix + job_id)
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(2, frozenset({10})))
    assert CronSchedule.decode(await arq_redis.get(schedule_key(queue, job_id))).minutes == {10}


async def test_completed_run_marker_does_not_preserve_waiting_work(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    await arq_redis.enqueue_job('transfer', _job_id=job_id, _queue_name=queue)
    await arq_redis.set(in_progress_key_prefix + job_id, b'complete', ex=60)
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(1, frozenset({10})))
    assert await arq_redis.zcard(queue) == 0
    assert await arq_redis.get(in_progress_key_prefix + job_id) == b'complete'


async def test_defaults_and_lost_override_recovery(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    worker = Worker(
        cron_jobs=[cron(transfer, name='transfer', job_id=job_id, minute={5}, dynamic=True)],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
    )
    worker._pool = arq_redis
    now = datetime(2030, 1, 1, 0, 4, 59, tzinfo=timezone.utc)
    await worker.run_cron(now, 1)
    assert worker.cron_jobs[0].next_run.minute == 5

    updated = CronSchedule(1, frozenset({10}))
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=updated)
    await worker.run_cron(now, 1)
    assert worker.cron_jobs[0].next_run.minute == 10

    await arq_redis.delete(schedule_key(queue, job_id))
    await worker.run_cron(now.replace(minute=9), 1)
    assert await arq_redis.zcard(queue) == 0

    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=updated)
    await worker.run_cron(now.replace(minute=9), 1)
    assert await arq_redis.zcard(queue) == 1


async def test_replicas_acknowledge_updates_and_share_unique_due_job(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    seen = []

    async def changed(ctx):
        seen.append((ctx['cron_job_id'], ctx['cron_schedule_revision']))

    workers = [
        Worker(
            cron_jobs=[cron(transfer, name='transfer', job_id=job_id, minute={5}, dynamic=True)],
            redis_pool=arq_redis,
            queue_name=queue,
            handle_signals=False,
            on_cron_schedule_change=changed,
        )
        for _ in range(2)
    ]
    schedule = CronSchedule(1, frozenset({10}))
    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=schedule)
    now = datetime(2030, 1, 1, 0, 9, 59, tzinfo=timezone.utc)
    for worker in workers:
        worker._pool = arq_redis
        await worker.run_cron(now, 1)
        await worker.run_cron(now, 1)

    assert seen == [(job_id, 1), (job_id, 1)]
    assert await arq_redis.zcard(queue) == 1


@pytest.mark.parametrize('run_at_startup', [False, True])
async def test_schedule_change_reports_applied_minutes_and_next_occurrence(arq_redis, run_at_startup):
    queue, job_id = 'test:dynamic', 'transfer'
    seen = []

    async def changed(ctx):
        seen.append((ctx['cron_schedule_revision'], ctx['cron_schedule_minutes'], ctx['cron_next_run']))
        assert ctx['cron_next_run'] == worker.cron_jobs[0].next_run

    worker = Worker(
        cron_jobs=[
            cron(transfer, job_id=job_id, minute=set(range(0, 60, 2)), dynamic=True, run_at_startup=run_at_startup)
        ],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
        on_cron_schedule_change=changed,
    )
    worker._pool = arq_redis
    now = datetime(2030, 1, 1, 13, 3, tzinfo=timezone.utc)
    await worker.run_cron(now, 0.5)
    initial_next = now if run_at_startup else now.replace(minute=4, microsecond=123456)
    assert seen == [(0, frozenset(range(0, 60, 2)), initial_next)]

    for revision, minutes, expected_next in [
        (1, {0, 15, 30, 45}, now.replace(minute=15, microsecond=123456)),
        (2, set(), None),
        (3, {0}, now.replace(hour=14, minute=0, microsecond=123456)),
    ]:
        await update_cron_schedule(
            arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(revision, frozenset(minutes))
        )
        await worker.run_cron(now, 0.5)
        await worker.run_cron(now, 0.5)
        assert seen[-1] == (revision, frozenset(minutes), expected_next)
        assert len(seen) == revision + 1


async def test_acknowledgements_preserve_distinct_job_schedules(arq_redis):
    queue = 'test:dynamic'
    schedules = {'foo': CronSchedule(1, frozenset({5})), 'foo:applied': CronSchedule(2, frozenset({10}))}
    for job_id, schedule in schedules.items():
        await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=schedule)
    worker = Worker(
        cron_jobs=[cron(transfer, name=job_id, job_id=job_id, dynamic=True) for job_id in schedules],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
    )
    worker._pool = arq_redis
    await worker.run_cron(datetime(2030, 1, 1, tzinfo=timezone.utc), 1)
    for job, (job_id, schedule) in zip(worker.cron_jobs, schedules.items()):
        assert job.minute == schedule.minutes
        assert await arq_redis.get(schedule_key(queue, job_id)) == schedule.encode()
        assert await arq_redis.ttl(schedule_key(queue, job_id)) == -1
        assert await arq_redis.get(schedule_applied_key(queue, job_id)) == schedule.encode()


@pytest.mark.parametrize('initially_disabled', [False, True])
async def test_revisions_schedule_future_runs_without_replaying_startup(arq_redis, initially_disabled):
    queue, job_id = 'test:dynamic', 'transfer'
    calls = []

    async def run(ctx):
        calls.append(ctx['job_try'])

    worker = Worker(
        cron_jobs=[
            cron(
                run,
                name=job_id,
                job_id=job_id,
                minute=set() if initially_disabled else {5},
                dynamic=True,
                run_at_startup=True,
            )
        ],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
    )
    worker._pool = arq_redis
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    await worker.run_cron(now, 1)
    if not initially_disabled:
        await worker.run_job(job_id, 0)
        assert calls == [1]

    for revision, minutes in [(1, {10}), (2, set()), (3, {20})]:
        await update_cron_schedule(
            arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(revision, frozenset(minutes))
        )
        await worker.run_cron(now, 1)
        if minutes:
            assert worker.cron_jobs[0].next_run.minute == min(minutes)
        assert await arq_redis.zcard(queue) == 0

    await worker.run_cron(now.replace(minute=19, second=59), 1)
    assert await arq_redis.zcard(queue) == 1


async def test_replacement_occurrence_gets_a_fresh_retry_budget(arq_redis):
    queue, job_id = 'test:dynamic', 'transfer'
    attempts = []

    async def run(ctx):
        attempts.append(ctx['job_try'])
        if len(attempts) == 1:
            raise Retry(defer=60)

    worker = Worker(
        cron_jobs=[cron(run, name=job_id, job_id=job_id, minute={5}, dynamic=True, max_tries=1)],
        redis_pool=arq_redis,
        queue_name=queue,
        handle_signals=False,
    )
    worker._pool = arq_redis
    now = datetime(2030, 1, 1, 0, 4, 59, tzinfo=timezone.utc)
    await worker.run_cron(now, 1)
    await worker.run_cron(now, 1)
    await worker.run_job(job_id, 0)
    assert attempts == [1]
    assert await arq_redis.get(retry_key_prefix + job_id) == b'1'

    await update_cron_schedule(arq_redis, queue_name=queue, job_id=job_id, schedule=CronSchedule(1, frozenset({10})))
    await worker.run_cron(now, 1)
    await worker.run_cron(now.replace(minute=9), 1)
    await worker.run_job(job_id, 0)
    assert attempts == [1, 1]
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
