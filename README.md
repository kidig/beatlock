# Beatlock

Celery Beat failover for multi-cluster infrastructure. 
Beatlock is a Celerybeat scheduler that works with Redis-Sentinel to provide failover capabilities. 
It integrates seamlessly with [django_celery_beat](https://github.com/celery/django-celery-beat).

## Features

* **Failover Support**: Uses Redis-Sentinel for high availability.
* **Integration with Django**: Compatible with `django_celery_beat`.
* **Configurable Locking Mechanism**: Option to disable the locking mechanism if needed.

## Installation

To install Beatlock, run:

```
pip install beatlock
```


## Configuration

To set Beatlock as the scheduler instead of the default one, add the following configuration to your Celery settings:

```python
# Set Beatlock as the scheduler
CELERY_BEAT_SCHEDULER = "beatlock.schedulers.BeatLockScheduler"

# Disable the locking mechanism (optional)
CELERY_BEATLOCK_LOCK_DISABLED = True  # default: False
```


## Usage

After configuring, Beatlock will automatically handle the scheduling of tasks with failover capabilities using Redis-Sentinel. 
Ensure that your Redis-Sentinel setup is correctly configured and accessible by your Celery workers.


## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.


## License

This project is licensed under the MIT License. See the LICENSE file for more details.
