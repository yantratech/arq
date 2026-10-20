import asyncio
import logging

import pytest

from arq.connections import ArqRedis
from arq.worker import Retry, RetryJob, func


@pytest.mark.parametrize(
    'scenario,expected',
    [
        ('handled', [('succeeded', None)]),
        ('failure', [('terminal', 'ValueError')]),
        ('timeout', [('terminal', 'TimeoutError')]),
        ('retry_success', [('retrying', 'Retry'), ('succeeded', None)]),
        ('retry_job_success', [('retrying', 'RetryJob'), ('succeeded', None)]),
        ('cancel_retry_success', [('retrying', 'CancelledError'), ('succeeded', None)]),
        ('retry_exhausted', [('retrying', 'Retry'), ('retrying', 'Retry'), ('terminal', 'JobExecutionFailed')]),
        ('cancelled_unknown', [('cancelled_unknown', 'CancelledError')]),
    ],
)
async def test_worker_classifies_final_outcomes(arq_redis: ArqRedis, worker, caplog, scenario, expected):
    """Only final failures carry terminal metadata, including when results are not retained."""
    caplog.set_level(logging.INFO, logger='arq.worker')

    async def task(ctx):
        if scenario == 'handled':
            try:
                raise ValueError('handled exception')
            except ValueError:
                return
        if scenario == 'failure':
            raise ValueError('private exception text')
        if scenario == 'timeout':
            await asyncio.Future()
        if scenario == 'retry_exhausted' or (scenario == 'retry_success' and ctx['job_try'] == 1):
            raise Retry()
        if scenario == 'retry_job_success' and ctx['job_try'] == 1:
            raise RetryJob()
        if scenario == 'cancelled_unknown' or (scenario == 'cancel_retry_success' and ctx['job_try'] == 1):
            raise asyncio.CancelledError()

    await arq_redis.enqueue_job('task')
    instance = worker(
        functions=[func(task, name='task')],
        keep_result=0,
        max_tries=2,
        job_timeout=0.01 if scenario == 'timeout' else 10,
        retry_jobs=scenario != 'cancelled_unknown',
    )
    await instance.main()

    assert [
        (record.arq_job_outcome, getattr(record, 'arq_exception_type', None))
        for record in caplog.records
        if hasattr(record, 'arq_job_outcome')
    ] == expected


async def test_requested_abort_is_not_terminal(arq_redis: ArqRedis, worker, caplog):
    """An acknowledged cancellation before execution remains distinct from terminal failure."""
    caplog.set_level(logging.INFO, logger='arq.worker')

    async def task(ctx):
        raise AssertionError('aborted work must not run')

    await arq_redis.enqueue_job('task', _job_id='abort-me')
    await arq_redis.request_abort_jobs(['abort-me'])
    instance = worker(functions=[func(task, name='task')], allow_abort_jobs=True)
    await instance.main()

    assert [
        (record.arq_job_outcome, getattr(record, 'arq_exception_type', None))
        for record in caplog.records
        if hasattr(record, 'arq_job_outcome')
    ] == [('cancelled', 'CancelledError')]
