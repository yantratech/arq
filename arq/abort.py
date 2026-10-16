"""Redis operations shared by cancellation producers and workers."""

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


_TAKE_ABORT_REQUESTS = """
local requested = {}
for _, id in ipairs(ARGV) do
    if redis.call('ZREM', KEYS[1], id) == 1 then
        table.insert(requested, id)
    end
end
return requested
"""


async def take_abort_requests(redis: 'Redis[bytes]', job_ids: list[str]) -> list[str]:
    requested: list[str] = []
    for start in range(0, len(job_ids), 100):
        ids = await redis.eval(  # type: ignore[no-untyped-call]  # Redis 4's stubs leave EVAL untyped.
            _TAKE_ABORT_REQUESTS, 1, abort_jobs_ss, *job_ids[start : start + 100]
        )
        requested.extend(job_id.decode() for job_id in ids)

    return requested
