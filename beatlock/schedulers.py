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

logger = get_logger('celery.beat')

CELERY_4_OR_GREATER = CELERY_VERSION[0] >= 4

# Copy from:
# https://github.com/andymccurdy/redis-py/blob/master/redis/lock.py#L33
# KEYS[1] - lock name
# ARGS[1] - token
# ARGS[2] - additional milliseconds
# return 1 if the locks time was extended, otherwise 0
LUA_EXTEND_TO_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    local expiration = redis.call('pttl', KEYS[1])
    if not expiration then
        expiration = 0
    end
    if expiration < 0 then
        return 0
    end
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 1
"""

class RetryingConnection:
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
        redis_options = conf.beatlock_redis_options
        retry_period = redis_options.get('retry_period')

        if conf.redis_url.startswith('redis-sentinel') and 'sentinels' in redis_options:
            from redis.sentinel import Sentinel

            sentinel = Sentinel(
                redis_options['sentinels'],
                socket_timeout=redis_options.get('socket_timeout'),
                password=redis_options.get('password'),
                db=redis_options.get('db', 0),
                decode_responses=True,
                sentinel_kwargs=redis_options.get('sentinel_kwargs'),
            )
            connection = sentinel.master_for(redis_options.get('service_name', 'master'))
        else:
            connection = StrictRedis.from_url(conf.redis_url, decode_responses=True)

        if retry_period is None:
            app.beatlock_redis = connection
        else:
            app.beatlock_redis = RetryingConnection(retry_period, connection)

    return app.beatlock_redis


class BeatLockConfig:
    def __init__(self, app=None):
        self.app = app_or_default(app)
        self.key_prefix = self.either_or('beatlock_key_prefix', 'beatlock:')
        self.lock_disabled = self.either_or('beatlock_lock_disabled', False)
        self.lock_key = self.either_or('beatlock_lock_key', self.key_prefix + ':lock')
        self.lock_timeout = self.either_or('beatlock_lock_timeout', None)
        self.redis_url = self.either_or('beatlock_redis_url', app.conf['BROKER_URL'])
        self.redis_use_ssl = self.either_or('beatlock_redis_use_ssl', app.conf['BROKER_USE_SSL'])
        self.beatlock_redis_options = self.either_or(
            'beatlock_redis_options', app.conf['BROKER_TRANSPORT_OPTIONS']
        )

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
        ensure_conf(app)  # set app.beatlock_conf
        self.lock_key = lock_key or app.beatlock_conf.lock_key
        self.lock_timeout = (
            lock_timeout
            or app.beatlock_conf.lock_timeout
            or self.max_interval * 5
            or self.lock_timeout
        )

        super().__init__(app, **kwargs)

    def tick(self, *args, **kwargs):
        if self.lock:
            logger.debug('beat: Extending lock...')
            self.lock.extend(int(self.lock_timeout))

        return super().tick(*args, **kwargs)

    def close(self):
        if self.lock:
            logger.info('beat: Releasing lock')
            self.lock.release()
            self.lock = None
        super().close()

    @property
    def info(self):
        info = ['    . redis -> {}'.format(maybe_sanitize_url(self.app.beatlock_conf.redis_url))]
        if self.lock_key:
            info.append(
                '       . lock -> `{}` {} ({}s)'.format(
                    self.lock_key, humanize_seconds(self.lock_timeout), self.lock_timeout
                )
            )
        return '\n'.join(info)


@beat_init.connect
def acquire_distributed_beat_lock(sender=None, **kwargs):
    scheduler = sender.scheduler
    if not scheduler.lock_key:
        return

    logger.debug('beat: Acquiring lock...')
    redis_client = get_redis(scheduler.app)

    lock = redis_client.lock(
        scheduler.lock_key,
        timeout=scheduler.lock_timeout,
        sleep=scheduler.max_interval,
    )
    # overwrite redis-py's extend script
    # which will add additional timeout instead of extend to a new timeout
    lock.lua_extend = redis_client.register_script(LUA_EXTEND_TO_SCRIPT)
    lock.acquire()
    logger.info('beat: Acquired lock')

    scheduler.lock = lock
