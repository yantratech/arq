"""Revisioned minute schedules shared by workers of one queue."""

from __future__ import annotations

import json
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import WatchError

from .constants import in_progress_key_prefix, job_key_prefix


def schedule_key(queue_name: str, job_id: str) -> str:
    return f'{queue_name}:cron-schedule:{job_id}'


def schedule_applied_key(queue_name: str, job_id: str) -> str:
    return schedule_key(queue_name, job_id) + ':applied'


@dataclass(frozen=True)
class CronSchedule:
    revision: int
    minutes: frozenset[int]

    def __post_init__(self) -> None:
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError('revision must be a nonnegative integer')
        if any(type(minute) is not int or not 0 <= minute <= 59 for minute in self.minutes):
            raise ValueError('minutes must contain integers from 0 to 59')

    def encode(self) -> bytes:
        return json.dumps([self.revision, sorted(self.minutes)], separators=(',', ':')).encode()

    @classmethod
    def decode(cls, data: bytes) -> CronSchedule:
        revision, minutes = json.loads(data)
        return cls(revision, frozenset(minutes))


async def update_cron_schedule(redis: Redis[bytes], *, queue_name: str, job_id: str, schedule: CronSchedule) -> bool:
    """Apply a newer revision, withdrawing waiting work while preserving running work.

    An empty minute set disables scheduling. Revisions must increase for each
    change; equal revisions are idempotent only when their contents agree.
    """
    key = schedule_key(queue_name, job_id)
    job_key = job_key_prefix + job_id
    progress_key = in_progress_key_prefix + job_id
    encoded = schedule.encode()

    while True:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key, progress_key, job_key)
            if current := await pipe.get(key):
                previous = CronSchedule.decode(current)
                if previous.revision > schedule.revision:
                    return False
                if previous.revision == schedule.revision:
                    if current != encoded:
                        raise ValueError('Conflicting cron schedules have the same revision')
                    return False

            running = await pipe.get(progress_key) == b'1'
            pipe.multi()
            pipe.set(key, encoded)
            if not running:
                pipe.delete(job_key)
                pipe.zrem(queue_name, job_id)

            try:
                await pipe.execute()
                return True
            except WatchError:
                continue
