"""Redis operations shared by cancellation producers and workers."""

from collections.abc import Iterable

from redis.asyncio import Redis

from .constants import abort_job_max_age, abort_jobs_ss, in_progress_key_prefix, job_key_prefix
from .utils import timestamp_ms

# The live keys and age are checked in the same Redis operation as removal.
_CLEANUP_ABORT_REQUESTS = """
local page = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', 100)
for i = 1, #page[2], 2 do
    local id = page[2][i]
    local score = tonumber(page[2][i + 1])
    if score < tonumber(ARGV[2])
        and redis.call('EXISTS', ARGV[3] .. id, ARGV[4] .. id) == 0 then
        redis.call('ZREM', KEYS[1], id)
    end
end
return page[1]
"""


async def cleanup_abort_requests(redis: 'Redis[bytes]', cursor: int) -> int:
    return int(
        await redis.eval(  # type: ignore[no-untyped-call]  # Redis 4's stubs leave EVAL untyped.
            _CLEANUP_ABORT_REQUESTS,
            1,
            abort_jobs_ss,
            cursor,
            timestamp_ms() - abort_job_max_age * 1000,
            job_key_prefix,
            in_progress_key_prefix,
        )
    )


_REMOVE_ABORT_REQUESTS = """
for i = 1, #ARGV, 2 do
    local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
    if score and tonumber(score) == tonumber(ARGV[i + 1]) then
        redis.call('ZREM', KEYS[1], ARGV[i])
    end
end
"""


async def remove_abort_requests(redis: 'Redis[bytes]', requests: dict[str, float]) -> None:
    if requests:
        args = [value for request in requests.items() for value in request]
        await redis.eval(  # type: ignore[no-untyped-call]  # Redis 4's stubs leave EVAL untyped.
            _REMOVE_ABORT_REQUESTS, 1, abort_jobs_ss, *args
        )


_REQUEST_ABORTS = """
local now = tonumber(ARGV[1])
for i = 3, #ARGV do
    local id = ARGV[i]
    local score = redis.call('ZSCORE', KEYS[1], id)
    if score or redis.call('EXISTS', ARGV[2] .. id) == 1 then
        redis.call('ZADD', KEYS[2], now, id)
        if score and tonumber(score) > now then
            redis.call('ZADD', KEYS[1], 'XX', 1, id)
        end
    end
end
"""


async def request_abort_jobs(redis: 'Redis[bytes]', job_ids: Iterable[str], queue_name: str) -> None:
    if isinstance(job_ids, (str, bytes)):
        raise TypeError('job_ids must be an iterable of job IDs, not a single string')

    unique_ids = list(dict.fromkeys(job_ids))
    for start in range(0, len(unique_ids), 100):
        await redis.eval(  # type: ignore[no-untyped-call]  # Redis 4's stubs leave EVAL untyped.
            _REQUEST_ABORTS,
            2,
            queue_name,
            abort_jobs_ss,
            timestamp_ms(),
            in_progress_key_prefix,
            *unique_ids[start : start + 100],
        )
