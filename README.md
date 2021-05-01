# beatlock

Celerybeat with failover on Redis-Sentinel


## Installation

```pip install beatlock```


## Configuration

```python
# to set this scheduler instead default one
CELERY_BEAT_SCHEDULER = "beatlock.schedulers.BeatLockScheduler"

# for disable locking mechanism
CELERY_BEATLOCK_LOCK_DISABLED = True  # default: False
```
