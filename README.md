# arq — Yantra fork

This is [Yantra’s fork](https://github.com/yantratech/arq) of [python-arq/arq](https://github.com/python-arq/arq), maintained for Yantra applications. The MIT license and upstream attribution apply. Engineering has write access; changes are reviewed through pull requests.

See the [maintainer knowledge base](docs/knowledge-base.md) for development, dependency pins and cancellation semantics. The badges below describe the upstream package.

[![CI](https://img.shields.io/github/actions/workflow/status/samuelcolvin/arq/ci.yml?branch=main&logo=github&label=CI)](https://github.com/samuelcolvin/arq/actions?query=event%3Apush+branch%3Amain+workflow%3ACI)
[![Coverage](https://codecov.io/gh/samuelcolvin/arq/branch/main/graph/badge.svg)](https://codecov.io/gh/samuelcolvin/arq)
[![downloads](https://static.pepy.tech/badge/arq/month)](https://pepy.tech/project/arq)
[![pypi](https://img.shields.io/pypi/v/arq.svg)](https://pypi.python.org/pypi/arq)
[![versions](https://img.shields.io/pypi/pyversions/arq.svg)](https://github.com/samuelcolvin/arq)
[![license](https://img.shields.io/github/license/samuelcolvin/arq.svg)](https://github.com/samuelcolvin/arq/blob/main/LICENSE)

**In maintenance only mode**: arq is in maintenance only mode, see [#510](https://github.com/python-arq/arq/issues/510).

Job queues in python with asyncio and redis.

See [documentation](https://arq-docs.helpmanual.io/) for more details.

## Live minute schedules

Register a cron job with `dynamic=True` and a fixed `job_id` to allow live minute-of-hour updates. Its configured minute field is the default until an override is published. An empty minute set disables scheduling while retaining the callable registration.

```python
from arq import cron
from arq.schedules import CronSchedule, update_cron_schedule

cron_jobs = [cron(sync_files, job_id="sync-files", minute={0, 15, 30, 45}, dynamic=True)]

await update_cron_schedule(
    redis,
    queue_name="arq:files",
    job_id="sync-files",
    schedule=CronSchedule(revision=1, minutes=frozenset({0, 30})),
)
```

The application owns durable configuration and increasing revisions. Equal revisions must have identical contents; older updates are ignored. Workers reload the shared schedule on their normal scheduler tick, calculate the next matching time and enqueue only due work. Updates withdraw waiting work and preserve running work. Enqueue transactions reject stale schedule snapshots, including snapshots from other worker replicas. The worker's job-claim transaction protects waiting-work removal from racing with execution.

Load durable overrides before worker startup completes. If Redis loses an override a running worker has observed, that worker suspends its schedule until the application restores the override. Reconcile from durable storage after reconnects and Redis loss.

`on_cron_schedule_change(ctx)` runs when a worker observes a different schedule; its context includes `cron_job_id` and `cron_schedule_revision`. `schedule_applied_key(queue_name, job_id)` holds the encoded acknowledged schedule for ten seconds, refreshed by scheduler ticks. Compare it with the desired schedule to distinguish saved configuration from a live worker acknowledgement. Hooks that publish best-effort UI events should handle their own delivery errors.
