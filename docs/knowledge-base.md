# ARQ maintainer knowledge base

## Ownership and distribution

The repository is maintained by Yantra Engineering. The Python package and import name remain `arq`, so applications install either this fork or upstream in an environment. Consumers must use a full Git commit SHA from `https://github.com/yantratech/arq` and record the same source in their lockfile. A dependency’s uv source override is not inherited by its consumers; each application must declare its own source override.

The fork has no automatic package or documentation publication. GitHub Actions validates pull requests and main with formatting, typing, documentation builds and real Redis tests. Changes require review and explicit owner approval before merging.

## Development

Use `uv sync --group testing --group linting --extra watch` to prepare the environment. Focused tests run with `uv run pytest tests/test_jobs.py -k abort`. Tests use disposable Redis containers through Docker; `ARQ_TEST_REDIS_VERSION` selects the image version. The CI matrix covers Python 3.9–3.14 and Redis 5–8.

## Cancellation contract

Workers must set `allow_abort_jobs=True` to process abort requests. `Job.abort()` requests cancellation and waits for a retained result. Its `True` outcome requires a stored cancellation exception. A timeout leaves the request pending, and a missing result is not proof that the job was cancelled. Applications requiring confirmation must retain results long enough for their observers and distinguish unknown outcomes from failures.

`ArqRedis.request_abort_jobs(ids, queue_name=...)` submits deduplicated cancellation requests in batches of at most 100 without waiting for results. Each batch atomically registers requests and promotes deferred entries. It skips absent jobs and does not recreate queue entries. Submission can partially succeed if a later batch fails or the caller is cancelled; callers must not reuse IDs for unrelated work while cancellation is pending. `Job.abort()` uses the same request protocol before waiting for confirmation.

Workers check abort requests for their own active tasks and incrementally scan for orphaned requests. Requests at least 60 seconds old are eligible for removal only when both the payload and in-progress key are absent. Live requests remain pending regardless of age, including jobs owned by another queue.
