import logging
import warnings

import redis.exceptions
from celery import VERSION as CELERY_VERSION
from celery.app import app_or_default
from celery.signals import beat_init
from celery.utils.log import get_logger
from celery.utils.time import humanize_seconds
from django_celery_beat.schedulers import DEFAULT_MAX_INTERVAL, DatabaseScheduler
from kombu.utils.url import maybe_sanitize_url
from redis.client import StrictRedis
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential

logger = get_logger(__name__)

CELERY_4_OR_GREATER = CELERY_VERSION[0] >= 4


class RetryingConnection(object):
    """A proxy for the Redis connection that delegates all the calls to
    underlying Redis connection while retrying on connection or time-out error.
    """
    RETRY_MAX_WAIT = 30

    def __init__(self, retry_period, wrapped_connection):
        self.wrapped_connection = wrapped_connection
        self.retry_kwargs = {
            'retry': (
                retry_if_exception_type(redis.exceptions.ConnectionError) |
                retry_if_exception_type(redis.exceptions.TimeoutError)
            ),
            'reraise': True,
            'wait': wait_exponential(multiplier=1, max=self.RETRY_MAX_WAIT),
            'before_sleep': self._log_retry_attempt
        }
        if retry_period >= 0:
            self.retry_kwargs.update({'stop': stop_after_delay(retry_period)})

    def __getattr__(self, item):
        method = getattr(self.wrapped_connection, item)

        # we don't want to deal attributes or properties
        if not callable(method):
            return method

        @retry(**self.retry_kwargs)
        def retrier(*args, **kwargs):
            return method(*args, **kwargs)

        return retrier

    @staticmethod
    def _log_retry_attempt(retry_state):
        """Log when next reconnection attempt is about to be made."""
        logger.log(logging.WARNING,
                   "Retrying connection in %s seconds...",
                   retry_state.next_action.sleep)


def ensure_conf(app):
    """
    Ensure for the given app the the beatlock_conf
    attribute is set to an instance of the BeatLockConfig
    class.
    """
    name = 'beatlock_conf'
    app = app_or_default(app)
    try:
        config = getattr(app, name)
    except AttributeError:
        config = BeatLockConfig(app)
        setattr(app, name, config)
    return config


def get_redis(app=None):
    app = app_or_default(app)
    conf = ensure_conf(app)
    if not hasattr(app, 'beatlock_redis') or app.beatlock_redis is None:
        redis_options = conf.app.conf.get(
            'BEATLOCK_REDIS_OPTIONS',
            conf.app.conf.get('BROKER_TRANSPORT_OPTIONS', {})
        )
        retry_period = redis_options.get('retry_period')

        if conf.redis_url.startswith('redis-sentinel') and 'sentinels' in redis_options:
            from redis.sentinel import Sentinel
            sentinel = Sentinel(redis_options['sentinels'],
                                socket_timeout=redis_options.get('socket_timeout'),
                                password=redis_options.get('password'),
                                decode_responses=True)
            connection = sentinel.master_for(redis_options.get('service_name', 'master'))
        else:
            connection = StrictRedis.from_url(conf.redis_url, decode_responses=True)

        if retry_period is None:
            app.beatlock_redis = connection
        else:
            app.beatlock_redis = RetryingConnection(retry_period, connection)

    return app.beatlock_redis


class BeatLockConfig(object):
    def __init__(self, app=None):
        self.app = app_or_default(app)
        self.key_prefix = self.either_or('beatlock_key_prefix', 'beatlock:')
        self.lock_disabled = self.either_or('beatlock_lock_disabled', False)
        self.lock_key = self.either_or('beatlock_lock_key', self.key_prefix + ':lock')
        self.lock_timeout = self.either_or('beatlock_lock_timeout', None)
        self.redis_url = self.either_or('beatlock_redis_url', app.conf['BROKER_URL'])

        if self.lock_disabled:
            self.lock_key = None

    def either_or(self, name, default=None):
        if CELERY_4_OR_GREATER and name == name.upper():
            warnings.warn(
                'Celery v4 installed, but detected Celery v3 '
                'configuration %s (use %s instead).' % (name, name.lower()),
                UserWarning
            )
        return self.app.conf.first(name, name.upper()) or default


class BeatLockScheduler(DatabaseScheduler):

    lock = None

    #: The default lock timeout in seconds.
    lock_timeout = DEFAULT_MAX_INTERVAL * 5

    def __init__(self, app, lock_key=None, lock_timeout=None, **kwargs):
        ensure_conf(app)
        self.lock_key = lock_key or app.beatlock_conf.lock_key
        self.lock_timeout = (lock_timeout or
                             app.beatlock_conf.lock_timeout or
                             self.lock_timeout)

        super().__init__(app, **kwargs)

    def tick(self, *args, **kwargs):
        if self.lock:
            logger.debug('beat: Extending lock...')
            get_redis(self.app).pexpire(self.lock_key, int(self.lock_timeout * 1000))

        return super().tick(*args, **kwargs)

    def close(self):
        if self.lock:
            logger.debug('beat: Releasing Lock')
            self.lock.release()
            self.lock = None
        super().close()

    @property
    def info(self):
        info = ['    . redis -> {}'.format(maybe_sanitize_url(self.app.beatlock_conf.redis_url))]
        if self.lock_key:
            info.append('    . lock -> `{}` {} ({}s)'.format(
                self.lock_key, humanize_seconds(self.lock_timeout), self.lock_timeout))
        return '\n'.join(info)


@beat_init.connect
def acquire_distributed_beat_lock(sender=None, **kwargs):
    scheduler = sender.scheduler
    if not scheduler.lock_key:
        return

    logger.debug('beat: Acquiring lock...')

    lock = get_redis(scheduler.app).lock(
        scheduler.lock_key,
        timeout=scheduler.lock_timeout,
        sleep=scheduler.max_interval,
    )
    lock.acquire()
    scheduler.lock = lock
