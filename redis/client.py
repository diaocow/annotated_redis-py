from __future__ import with_statement
from itertools import chain, starmap
import datetime
import sys
import warnings
import time as mod_time
from redis._compat import (b, izip, imap, iteritems, iterkeys, itervalues,
                           basestring, long, nativestr, urlparse, bytes)
from redis.connection import ConnectionPool, UnixDomainSocketConnection
from redis.exceptions import (
    ConnectionError,
    DataError,
    RedisError,
    ResponseError,
    WatchError,
    NoScriptError,
    ExecAbortError,
)

SYM_EMPTY = b('')


def list_or_args(keys, args):
    # returns a single list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (basestring, bytes)):
            keys = [keys]
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


def parse_debug_object(response):
    "Parse the results of Redis's DEBUG OBJECT command into a Python dict"
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = nativestr(response)
    response = 'type:' + response
    response = dict([kv.split(':') for kv in response.split()])

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ('refcount', 'serializedlength', 'lru', 'lru_seconds_idle')
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_object(response, infotype):
    "Parse the results of an OBJECT command"
    if infotype in ('idletime', 'refcount'):
        return int(response)
    return response


def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    response = nativestr(response)

    def get_value(value):
        if ',' not in value or '=' not in value:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(','):
                k, v = item.rsplit('=', 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            key, value = line.split(':', 1)
            info[key] = get_value(value)
    return info


SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'port': int,
    'quorum': int,
    's-down-time': int,
    'slave-priority': int,
}


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result['flags'].split(','))
    for name, flag in (('is_master', 'master'), ('is_slave', 'slave'),
                       ('is_sdown', 's_down'), ('is_odown', 'o_down'),
                       ('is_sentinel', 'sentinel'),
                       ('is_disconnected', 'disconnected'),
                       ('is_master_down', 'master_down')):
        result[name] = flag in flags
    return result


def parse_sentinel(response, **options):
    "Parse the result of Redis's SENTINEL command"
    parse = options.get('parse')
    if parse == 'SENTINEL_INFO':
        return [parse_sentinel_state(item) for item in response]
    elif parse == 'SENTINEL_INFO_MASTERS':
        result = {}
        for item in response:
            state = parse_sentinel_state(item)
            result[state['name']] = state
        return result
    elif parse == 'SENTINEL_ADDR_PORT':
        if response is None:
            return
        return response[0], int(response[1])
    return response


def pairs_to_dict(response):
    "Create a dict given a list of key/value pairs"
    it = iter(response)
    return dict(izip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in izip(it, it):
        if key in type_info:
            value = type_info[key](value)
        result[key] = value
    return result


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options['withscores']:
        return response
    score_cast_func = options.get('score_cast_func', float)
    it = iter(response)
    return list(izip(it, imap(score_cast_func, it)))


def sort_return_tuples(response, **options):
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not options['groups']:
        return response
    n = options['groups']
    return list(izip(*[response[i::n] for i in range(n)]))


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def parse_client(response, **options):
    parse = options['parse']
    if parse == 'LIST':
        clients = []
        for c in nativestr(response).splitlines():
            clients.append(dict([pair.split('=') for pair in c.split(' ')]))
        return clients
    elif parse == 'KILL':
        return bool(response)
    elif parse == 'GETNAME':
        return response and nativestr(response)
    elif parse == 'SETNAME':
        return nativestr(response) == 'OK'


def parse_config(response, **options):
    if options['parse'] == 'GET':
        response = [nativestr(i) if i is not None else None for i in response]
        return response and pairs_to_dict(response) or {}
    return nativestr(response) == 'OK'


def parse_script(response, **options):
    parse = options['parse']
    if parse in ('FLUSH', 'KILL'):
        return response == 'OK'
    if parse == 'EXISTS':
        return list(imap(bool, response))
    return response

# Redis 通用客户端实现类
# 
# Examples:
#
# >>> client = redis.StrictRedis()
# >>> client.set('name', 'diaocow')
# True
# >>> client.get('name')
# diaocow
# >>> client.bgsave()
# True
# >>> client.info()
# {'aof_rewrite_in_progress': 0, 'role': 'master', 'used_memory_rss': 1974272, ...} 
#
# StrictRedis是线程安全的，可以在多个线程中使用同一个实例
#
class StrictRedis(object):

    # RESPONSE_CALLBACKS 属性作用：
    #
    # redis有些命令的执行结果对用户来说是不友好的，譬如：
    # BGSAVE命令的返回结果是：Background saving started
    # 如果我们直接把这样的结果返回给用户，那么用户就必须了解redis更多的细节，
    # 否则它无法知道返回结果的含义，也就无法知道命令是否执行成功; 
    #
    # 为了解决这个问题，客户端就必须对这样命令的返回结果做二次处理，譬如BGSAVE命令，
    # 若返回结果为：Background saving started，则二次加工后返回'True'给用户，告诉用户命令执行成功，否则返回'False'；
    #
    # RESPONSE_CALLBACKS 属性就是用来解决刚才所说问题的，它是一个字典，
    # 其中的键为要对返回结果做二次处理的命令，值为二次处理方法
    #
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'AUTH EXISTS EXPIRE EXPIREAT HEXISTS HMSET MOVE MSETNX PERSIST '
            'PSETEX RENAMENX SISMEMBER SMOVE SETEX SETNX',
            bool
        ),
        string_keys_to_dict(
            'BITCOUNT DECRBY DEL GETBIT HDEL HLEN INCRBY LINSERT LLEN LPUSHX '
            'RPUSHX SADD SCARD SDIFFSTORE SETBIT SETRANGE SINTERSTORE SREM '
            'STRLEN SUNIONSTORE ZADD ZCARD ZREM ZREMRANGEBYRANK '
            'ZREMRANGEBYSCORE',
            int
        ),
        string_keys_to_dict('INCRBYFLOAT HINCRBYFLOAT', float),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: isinstance(r, long) and r or nativestr(r) == 'OK'
        ),
        string_keys_to_dict('SORT', sort_return_tuples),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET RENAME '
            'SAVE SELECT SHUTDOWN SLAVEOF WATCH UNWATCH',
            lambda r: nativestr(r) == 'OK'
        ),
        string_keys_to_dict('BLPOP BRPOP', lambda r: r and tuple(r) or None),
        string_keys_to_dict(
            'SDIFF SINTER SMEMBERS SUNION',
            lambda r: r and set(r) or set()
        ),
        string_keys_to_dict(
            'ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE',
            zset_score_pairs
        ),
        string_keys_to_dict('ZRANK ZREVRANK', int_or_none),
        {
            'BGREWRITEAOF': (
                lambda r: nativestr(r) == ('Background rewriting of AOF '
                                           'file started')
            ),
            'BGSAVE': lambda r: nativestr(r) == 'Background saving started',
            'CLIENT': parse_client,
            'CONFIG': parse_config,
            'DEBUG': parse_debug_object,
            'HGETALL': lambda r: r and pairs_to_dict(r) or {},
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'OBJECT': parse_object,
            'PING': lambda r: nativestr(r) == 'PONG',
            'RANDOMKEY': lambda r: r and r or None,
            'SCRIPT': parse_script,
            'SET': lambda r: r and nativestr(r) == 'OK',
            'TIME': lambda x: (int(x[0]), int(x[1])),
            'SENTINEL': parse_sentinel
        }
    )

    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        """
        Return a Redis client object configured from the given URL.

        For example::

            redis://username:password@localhost:6379/0

        If ``db`` is None, this method will attempt to extract the database ID
        from the URL path component.

        Any additional keyword arguments will be passed along to the Redis
        class's initializer.
        """
        url = urlparse(url)

        # We only support redis:// schemes.
        assert url.scheme == 'redis' or not url.scheme

        # Extract the database ID from the path component if hasn't been given.
        if db is None:
            try:
                db = int(url.path.replace('/', ''))
            except (AttributeError, ValueError):
                db = 0

        return cls(host=url.hostname, port=int(url.port or 6379), db=db,
                   password=url.password, **kwargs)

    # StrictRedis初始化方法
    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 connection_pool=None, charset='utf-8',
                 errors='strict', decode_responses=False,
                 unix_socket_path=None):

        # 若未指定连接池，则使用默认实现
        if not connection_pool:
            kwargs = {
                'db': db,
                'password': password,
                'socket_timeout': socket_timeout,
                'encoding': charset,
                'encoding_errors': errors,
                'decode_responses': decode_responses,
            }
            # 若指定了unix_socket_path，则client与redis之间连接采用UnixDomain协议，否则采用TCP协议
            if unix_socket_path:
                kwargs.update({
                    'path': unix_socket_path,
                    'connection_class': UnixDomainSocketConnection
                })
            else:
                kwargs.update({
                    'host': host,
                    'port': port
                })
	    # 创建连接池
            connection_pool = ConnectionPool(**kwargs)

        self.connection_pool = connection_pool
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    # 自定义命令返回结果处理方法
    # @see StrictRedis.RESPONSE_CALLBACKS
    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        self.response_callbacks[command] = callback

    # 获取Pipeline实例，该实例用来批量执行命令
    # @see BasePipeline
    def pipeline(self, transaction=True, shard_hint=None):
        return StrictPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

	# 执行事务的另一种调用方式
	#
	# >>> def incr_visitors(pipe):
	# ...	current_value = pipe.get('visitors')	
	# ...	next_value = int(current_value) + 1
	# ...	pipe.multi()
	# ...	pipe.set('visitors', next_value)   <= 原子性递增值
	# ...	
	# >>> client = redis.StrictRedis() 
	# >>> client.transaction(incr_visitors, 'visitors')
	# [True]
	#
	# @see BasePipeline.watch
    def transaction(self, func, *watches, **kwargs):
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        with self.pipeline(True, shard_hint) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    continue

    def lock(self, name, timeout=None, sleep=0.1):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.
        """
        return Lock(self, name, timeout=timeout, sleep=sleep)

    # 获取一个PubSub实例，通过该实例可以使用redis的发布/订阅功能
    # @see PubSub
    def pubsub(self, shard_hint=None):
        return PubSub(self.connection_pool, shard_hint)

    # 执行命令并返回结果
    def execute_command(self, *args, **options):
        pool = self.connection_pool
        # 命令名（譬如set，get， hset，lpush等等） 
        command_name = args[0]
        # 从连接池中获取一个连接
        connection = pool.get_connection(command_name, **options)
        try:
            # 向redis发出command命令
            connection.send_command(*args)
            # 返回command执行结果
            return self.parse_response(connection, command_name, **options)
        except ConnectionError:
            # socket连接错误，retry
            connection.disconnect()
            connection.send_command(*args) 
            return self.parse_response(connection, command_name, **options)
        finally: 
	    # 释放连接 
	    pool.release(connection) 

    # 获取命令的执行结果
    #
    # 特别要注意的是：对于出现在response_callbacks中的命令，会把结果二次加工后返回，譬如：
    # BGSAVE命令的返回结果是'Background saving started'，但由于BGSAVE这个命令出现在response_callbacks中，
    # 所以会对结果二次加工，即调用self.response_callbacks['BGSAVE']('Background saving started', **options)方法处理，
    # 然后把加工后的结果(这里是'True')返回给客户端; 而对与其他命令(不在response_callbacks)，直接返回解析结果
    #
    def parse_response(self, connection, command_name, **options):
        "Parses a response from the Redis server"
        response = connection.read_response()
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    # BGREWRITEAOF命令：AOF重写
    def bgrewriteaof(self):
        return self.execute_command('BGREWRITEAOF')

    # BGSAVE命令：保存数据库快照=>RDB文件
    def bgsave(self):
        return self.execute_command('BGSAVE')

    def client_kill(self, address):
        "Disconnects the client at ``address`` (ip:port)"
        return self.execute_command('CLIENT', 'KILL', address, parse='KILL')

    def client_list(self):
        "Returns a list of currently connected clients"
        return self.execute_command('CLIENT', 'LIST', parse='LIST')

    def client_getname(self):
        "Returns the current connection name"
        return self.execute_command('CLIENT', 'GETNAME', parse='GETNAME')

    def client_setname(self, name):
        "Sets the current connection name"
        return self.execute_command('CLIENT', 'SETNAME', name, parse='SETNAME')

    def config_get(self, pattern="*"):
        "Return a dictionary of configuration based on the ``pattern``"
        return self.execute_command('CONFIG', 'GET', pattern, parse='GET')

    def config_set(self, name, value):
        "Set config item ``name`` with ``value``"
        return self.execute_command('CONFIG', 'SET', name, value, parse='SET')

    def config_resetstat(self):
        "Reset runtime statistics"
        return self.execute_command('CONFIG', 'RESETSTAT', parse='RESETSTAT')

    # DBSIZE命令：获取数据库中键数量
    def dbsize(self):
        return self.execute_command('DBSIZE')

    def debug_object(self, key):
        "Returns version specific metainformation about a give key"
        return self.execute_command('DEBUG', 'OBJECT', key)

    def echo(self, value):
        "Echo the string back from the server"
        return self.execute_command('ECHO', value)

    def flushall(self):
        "Delete all keys in all databases on the current host"
        return self.execute_command('FLUSHALL')

    def flushdb(self):
        "Delete all keys in the current database"
        return self.execute_command('FLUSHDB')

    # INFO命令：获取数据库当前状态信息
    def info(self, section=None):
        if section is None:
            return self.execute_command('INFO')
        else:
            return self.execute_command('INFO', section)

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command('LASTSAVE')

    def object(self, infotype, key):
        "Return the encoding, idletime, or refcount about the key"
        return self.execute_command('OBJECT', infotype, key, infotype=infotype)

    def ping(self):
        "Ping the Redis server"
        return self.execute_command('PING')

    # 执行SAVE命令（同步保存RDB文件，此时redis无法处理其客户端请求）
    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')

    def sentinel(self, *args):
        "Redis Sentinel's SENTINEL command"
        if args[0] in ['masters', 'slaves', 'sentinels']:
            parse = 'SENTINEL_INFO'
        else:
            parse = 'SENTINEL'
        return self.execute_command('SENTINEL', *args, **{'parse': parse})

    def sentinel_masters(self):
        "Returns a dictionary containing the master's state."
        return self.execute_command('SENTINEL', 'masters',
                                    parse='SENTINEL_INFO_MASTERS')

    def sentinel_slaves(self, service_name):
        "Returns a list of slaves for ``service_name``"
        return self.execute_command('SENTINEL', 'slaves', service_name,
                                    parse='SENTINEL_INFO')

    def sentinel_sentinels(self, service_name):
        "Returns a list of sentinels for ``service_name``"
        return self.execute_command('SENTINEL', 'sentinels', service_name,
                                    parse='SENTINEL_INFO')

    def sentinel_get_master_addr_by_name(self, service_name):
        "Returns a (host, port) pair for the given ``service_name``"
        return self.execute_command('SENTINEL', 'get-master-addr-by-name',
                                    service_name, parse='SENTINEL_ADDR_PORT')

    def shutdown(self):
        "Shutdown the server"
        try:
            self.execute_command('SHUTDOWN')
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    # SLAVEOF命令：请求主从复制
    def slaveof(self, host=None, port=None):
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguements, the
        instance is promoted to a master instead.
        """
        if host is None and port is None:
            return self.execute_command("SLAVEOF", "NO", "ONE")
        return self.execute_command("SLAVEOF", host, port)

    def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return self.execute_command('TIME')

    #### BASIC KEY COMMANDS ####
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command('APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or \
                (end is not None and start is None):
            raise RedisError("Both start and end must be specified")
        return self.execute_command('BITCOUNT', *params)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return self.execute_command('BITOP', operation, dest, *keys)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.execute_command('DECRBY', name, amount)

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('DEL', *names)
    __delitem__ = delete

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return self.execute_command('DUMP', name)

    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('EXISTS', name)
    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            when = int(mod_time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.execute_command('GET', name)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return self.execute_command('GETBIT', name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.execute_command('GETRANGE', key, start, end)

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        return self.execute_command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.execute_command('INCRBY', name, amount)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """

        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.incr(name, amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return self.execute_command('INCRBYFLOAT', name, amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        args = list_or_args(keys, args)
        return self.execute_command('MGET', *args)

    def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.execute_command('MSET', *items)

    def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSETNX requires **kwargs or a single '
                                 'dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.execute_command('MOVE', name, db)

    def persist(self, name):
        "Removes an expiration on ``name``"
        return self.execute_command('PERSIST', name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            ms = int(time.microseconds / 1000)
            time = (time.seconds + time.days * 24 * 3600) * 1000 + ms
        return self.execute_command('PEXPIRE', name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(mod_time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command('PEXPIREAT', name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            ms = int(time_ms.microseconds / 1000)
            time_ms = (time_ms.seconds + time_ms.days * 24 * 3600) * 1000 + ms
        return self.execute_command('PSETEX', name, time_ms, value)

    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return self.execute_command('PTTL', name)

    def randomkey(self):
        "Returns the name of a random key"
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.execute_command('RENAMENX', src, dst)

    def restore(self, name, ttl, value):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        return self.execute_command('RESTORE', name, ttl, value)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
        pieces = [name, value]
        if ex:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = ex.seconds + ex.days * 24 * 3600
            pieces.append(ex)
        if px:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                ms = int(px.microseconds / 1000)
                px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
            pieces.append(px)

        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        return self.execute_command('SET', *pieces)
    __setitem__ = set

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command('SETBIT', name, offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.execute_command('SETEX', name, time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('SETNX', name, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return self.execute_command('SETRANGE', name, offset, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.execute_command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.execute_command('TYPE', name)

    def watch(self, *names):
        """
        Watches the values at keys ``names``, or None if the key doesn't exist
        """
        warnings.warn(DeprecationWarning('Call WATCH from a Pipeline object'))

    def unwatch(self):
        """
        Unwatches the value at key ``name``, or None of the key doesn't exist
        """
        warnings.warn(
            DeprecationWarning('Call UNWATCH from a Pipeline object'))

    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BRPOP', *keys)

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        if timeout is None:
            timeout = 0
        return self.execute_command('BRPOPLPUSH', src, dst, timeout)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command('LINDEX', name, index)

    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return self.execute_command('LINSERT', name, where, refvalue, value)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.execute_command('LLEN', name)

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.execute_command('LPOP', name)

    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"
        return self.execute_command('LPUSH', name, *values)

    def lpushx(self, name, value):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return self.execute_command('LPUSHX', name, value)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LRANGE', name, start, end)

    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.execute_command('LREM', name, count, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.execute_command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LTRIM', name, start, end)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.execute_command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command('RPOPLPUSH', src, dst)

    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"
        return self.execute_command('RPUSH', name, *values)

    def rpushx(self, name, value):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return self.execute_command('RPUSHX', name, value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append('BY')
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append('LIMIT')
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, basestring):
                pieces.append('GET')
                pieces.append(get)
            else:
                for g in get:
                    pieces.append('GET')
                    pieces.append(g)
        if desc:
            pieces.append('DESC')
        if alpha:
            pieces.append('ALPHA')
        if store is not None:
            pieces.append('STORE')
            pieces.append(store)

        if groups:
            if not get or isinstance(get, basestring) or len(get) < 2:
                raise DataError('when using "groups" the "get" argument '
                                'must be specified and contain at least '
                                'two keys')

        options = {'groups': len(get) if groups else None}
        return self.execute_command('SORT', *pieces, **options)

    #### SET COMMANDS ####
    def sadd(self, name, *values):
        "Add ``value(s)`` to set ``name``"
        return self.execute_command('SADD', name, *values)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.execute_command('SCARD', name)

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SDIFF', *args)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SDIFFSTORE', dest, *args)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SINTER', *args)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SINTERSTORE', dest, *args)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.execute_command('SISMEMBER', name, value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.execute_command('SMEMBERS', name)

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.execute_command('SMOVE', src, dst, value)

    def spop(self, name):
        "Remove and return a random member of set ``name``"
        return self.execute_command('SPOP', name)

    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        args = number and [number] or []
        return self.execute_command('SRANDMEMBER', name, *args)

    def srem(self, name, *values):
        "Remove ``values`` from set ``name``"
        return self.execute_command('SREM', name, *values)

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SUNION', *args)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SUNIONSTORE', dest, *args)

    #### SORTED SET COMMANDS ####
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(args)
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command('ZADD', name, *pieces)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.execute_command('ZCOUNT', name, min, max)

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores, 'score_cast_func': score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores, 'score_cast_func': score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"
        return self.execute_command('ZREM', name, *values)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.execute_command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores, 'score_cast_func': score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        options = {
            'withscores': withscores, 'score_cast_func': score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def _zaggregate(self, command, dest, keys, aggregate=None):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            keys, weights = iterkeys(keys), itervalues(keys)
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append('WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            pieces.append('AGGREGATE')
            pieces.append(aggregate)
        return self.execute_command(*pieces)

    #### HASH COMMANDS ####
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return self.execute_command('HDEL', name, *keys)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.execute_command('HEXISTS', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.execute_command('HGET', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.execute_command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.execute_command('HINCRBY', name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return self.execute_command('HINCRBYFLOAT', name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.execute_command('HKEYS', name)

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.execute_command('HLEN', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.execute_command('HSET', name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(self, name, mapping):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.execute_command('HMSET', name, *items)

    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('HMGET', name, *args)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.execute_command('HVALS', name)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.execute_command('PUBLISH', channel, message)

    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the LUA ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVAL', script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a LUA script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        options = {'parse': 'EXISTS'}
        return self.execute_command('SCRIPT', 'EXISTS', *args, **options)

    def script_flush(self):
        "Flush all scripts from the script cache"
        options = {'parse': 'FLUSH'}
        return self.execute_command('SCRIPT', 'FLUSH', **options)

    def script_kill(self):
        "Kill the currently executing LUA script"
        options = {'parse': 'KILL'}
        return self.execute_command('SCRIPT', 'KILL', **options)

    def script_load(self, script):
        "Load a LUA ``script`` into the script cache. Returns the SHA."
        options = {'parse': 'LOAD'}
        return self.execute_command('SCRIPT', 'LOAD', script, **options)

    def register_script(self, script):
        """
        Register a LUA ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with LUA scripts.
        """
        return Script(self, script)

# 老的Redis客户端，仅为了向下兼容，新代码都应该使用StrictRedis，所以对该类不在注释
class Redis(StrictRedis):
    """
    Provides backwards compatibility with older versions of redis-py that
    changed arguments to some commands to be more Pythonic, sane, or by
    accident.
    """

    # Overridden callbacks
    RESPONSE_CALLBACKS = dict_merge(
        StrictRedis.RESPONSE_CALLBACKS,
        {
            'TTL': lambda r: r != -1 and r or None,
            'PTTL': lambda r: r != -1 and r or None,
        }
    )

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def setex(self, name, value, time):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.execute_command('SETEX', name, time, value)

    def lrem(self, name, value, num=0):
        """
        Remove the first ``num`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The ``num`` argument influences the operation in the following ways:
            num > 0: Remove elements equal to value moving from head to tail.
            num < 0: Remove elements equal to value moving from tail to head.
            num = 0: Remove all elements equal to value.
        """
        return self.execute_command('LREM', name, num, value)

    def zadd(self, name, *args, **kwargs):
        """
        NOTE: The order of arguments differs from that of the official ZADD
        command. For backwards compatability, this method accepts arguments
        in the form of name1, score1, name2, score2, while the official Redis
        documents expects score1, name1, score2, name2.

        If you're looking to use the standard syntax, consider using the
        StrictRedis class. See the API Reference section of the docs for more
        information.

        Set any number of element-name, score pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: name1, score1, name2, score2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 'name1', 1.1, 'name2', 2.2, name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(reversed(args))
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command('ZADD', name, *pieces)


# 订阅功能实现类 
#
# Examples:
#
# >>> client = redis.StrictRedis().pubsub()
# >>> client.subscribe("hello")
# >>> for msg in client.listen():
# ...    if msg[type] == 'message':
# ...    	do_action(msg[data])
#
# 关于redis发布订阅功能实现原理，可以参看：http://diaocow.iteye.com/blog/1935094
#
# 另外，PubSub不是线程安全的，因为它是有状态的
# 若在线程A和线程B使用了同一个实例，那么就可能发生消息紊乱情况：A收到B订阅频道发来的消息，B收到A订阅频道发来的消息
#
class PubSub(object):

    def __init__(self, connection_pool, shard_hint=None):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.connection = None
        # 当前订阅的所有频道
        self.channels = set()
        # 当前订阅的所有模式
        self.patterns = set()
        # 当前订阅的频道&模式总数
        self.subscription_count = 0
        self.subscribe_commands = set(
            ('subscribe', 'psubscribe', 'unsubscribe', 'punsubscribe')
        )

    def __del__(self):
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            if self.connection and (self.channels or self.patterns):
                self.connection.disconnect()
            self.reset()
        except Exception:
            pass

    def reset(self):
        if self.connection:
            self.connection.disconnect()
            self.connection_pool.release(self.connection)
            self.connection = None

    # 关闭与redis服务器之间的连接
    def close(self):
        self.reset()

    # 执行一个订阅或者取消订阅命令
    def execute_command(self, *args, **kwargs):
        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'pubsub',
                self.shard_hint
            ) # get_connection的方法参数目前都是被无视的 
        connection = self.connection

        try:
	    # 发送订阅相关命令(PSUBSCRIBE, SUBSCRIBE, PUNSUBSCRIBE, UNSUBSCRIBE)
            connection.send_command(*args)
        except ConnectionError:
	    # 若socket连接出现错误，则retry：
	    # a. 重新连接redis服务器
	    # b. 订阅之前所有的频道
	    # c. 订阅之前多有的模式
	    # d. 重新发送刚才未成功发送的命令
            connection.disconnect()
            connection.connect()
            for channel in self.channels:
                self.subscribe(channel)
            for pattern in self.patterns:
                self.psubscribe(pattern)
            connection.send_command(*args)

    # 获取redis服务器返回内容
    #
    # 特别要注意的是：对于位于subscribe_commands中的命令(eg. PSUBSCRIBE, SUBSCRIBE, PUNSUBSCRIBE, UNSUBSCRIBE)
    # PubSub会额外对response进行解析来获取：当前取客户端当前订阅频道&模式总数 
    def parse_response(self):
        "Parse the response from a publish/subscribe command"
        response = self.connection.read_response()
        if nativestr(response[0]) in self.subscribe_commands:
            self.subscription_count = response[2]
	    # 若当前客户端没有订阅任何频道或者模式，则断开与redis服务器之间的连接(ps: 个人觉得这步不是很有必要)
            if not self.subscription_count:
                self.reset()
        return response

    # 订阅模式 
    def psubscribe(self, patterns):
        if isinstance(patterns, basestring):
            patterns = [patterns]
        for pattern in patterns:
            self.patterns.add(pattern)
        return self.execute_command('PSUBSCRIBE', *patterns)

    # 取消订阅模式
    def punsubscribe(self, patterns=[]):
        if isinstance(patterns, basestring):
            patterns = [patterns]
        for pattern in patterns:
            try:
                self.patterns.remove(pattern)
            except KeyError:
                pass
        return self.execute_command('PUNSUBSCRIBE', *patterns)

    # 订阅频道
    def subscribe(self, channels):
        if isinstance(channels, basestring):
            channels = [channels]
        for channel in channels:
            self.channels.add(channel)
        return self.execute_command('SUBSCRIBE', *channels)

    # 取消订阅频道
    def unsubscribe(self, channels=[]):
        if isinstance(channels, basestring):
            channels = [channels]
        for channel in channels:
            try:
                self.channels.remove(channel)
            except KeyError:
                pass
        return self.execute_command('UNSUBSCRIBE', *channels)

    # 接受消息 
    #
    # 若客户端A执行了psubscribe h*(订阅模式h*)，客户端B执行了subscribe hello(订阅频道hello)，那么当客户端C执行了publish hello diaocow时:
    #
    # 客户端A收到：
    #  "pmessage"
    #  "h*"
    #  "hello"
    #  "diaocow"
    #
    # 客户端B收到：
    #  "message"
    #  "hello"
    #  "diaocow"
    #
    def listen(self):
        while self.subscription_count or self.channels or self.patterns:
            r = self.parse_response()
	    # 收到的消息类型，基本上按照type可以分为三类：
	    # a. pmessage: 模式消息
	    # b. message: 频道消息
	    # c. 命令消息: 执行PSUBSCRIBE，SUBSCRIBE等命令返回的消息
            msg_type = nativestr(r[0])
	    # 模式消息
            if msg_type == 'pmessage':
                msg = {
                    'type': msg_type,
                    'pattern': nativestr(r[1]),
                    'channel': nativestr(r[2]),
                    'data': r[3]
                }
    	    # 频道消息或者命令消息
            else:
                msg = {
                    'type': msg_type,
                    'pattern': None,
                    'channel': nativestr(r[1]),
                    'data': r[2]
                }
    	    # 返回消息
            yield msg


# 批量执行命令实现类, 它有两种模式：
#
# 1. 事务模式： 命令组中所有命令是一个原子操作---要么都执行，要么都不执行
#
# >>> client = redis.StrictRedis().pipeline(transaction=True)
# >>> client.set("name", "jack")
# >>> client.set("age", "25")
# >>> client.mget("name", "age")
# >>> client.execute()
# [True, True, ['jack', 25]]
#
# 2. 普通模式： 与单个命令分别执行效果基本相同，但改用批量执行可以提高信道的使用率
#
# >>> client = redis.StrictRedis().pipeline(transaction=False)
# >>> client.set("name", "jack")
# >>> client.set("age", "25")
# >>> client.mget("name", "age")
# >>> client.execute()
# [True, True, ['jack', 25]]
#
# 调用multi()方法，可以强制从普通模式切换到事务模式
#
# 这两种模式在写法上没有太多的区别(仅仅是一个参数值的差异)，但对Redis来说两种方式执行过程就完全不同了
#
# 关于redis事务实现原理，可以参看：http://diaocow.iteye.com/blog/1935092
#
# BasePipeline不是线程安全的
#
class BasePipeline(object):

    UNWATCH_COMMANDS = set(('DISCARD', 'EXEC', 'UNWATCH'))

    def __init__(self, connection_pool, response_callbacks, transaction,
                 shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        # 是否以事务方式批量执行命令
        self.transaction = transaction
        self.shard_hint = shard_hint
        # 是否正在监视某些键
        self.watching = False
        # 重置Pipeline状态
        self.reset()

	# 覆写__enter__，__exit__方法，支持with语法：
	#
	# >>> with redis.StrictRedis().pipeline as pipe:
	# ...	do_action(pipe)
	#
	# 等价与
	#
	# >>> pipe = redis.StrictRedis().pipeline
	# >>> try:
	# ...	do_action(pipe)
	# >>> finally:
	# ...	pipe.reset()
	#
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)


	# 重置pipeline状态
    def reset(self):
        self.command_stack = []
        self.scripts = set() 
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command('UNWATCH')
                self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    # 强制切换到事务模式
    def multi(self):
        if self.explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise RedisError('Commands without an initial WATCH have already '
                             'been issued')
        self.explicit_transaction = True

    # 覆写StrictRedis.execute_command方法
    #
    # 该方法执行过程：
    # 1. 若处于"显示事务"状态（explicit_transaction为True)，则把命令添加到command_stack，稍后批量执行；
    # 2. 若不处于"显示事务"状态，又分为下面三种情况：
    #	2.1 目标命令为WATCH，则发送给redis立即执行；
    #	2.2 目标命令不为WATCH，但之前已经执行过WATCH命令(watching属性为True)，则同样把立即执行(若想使后面的命令批量执行，则必须调用multi方法)
    #	2.3 其余情况都按照情况1处理---把命令添加到command_stack，稍后批量执行；
    #
    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == 'WATCH') and \
                not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    # 立即执行命令 
    # @see BasePipeline.execute_command
    def immediate_execute_command(self, *args, **options):
        command_name = args[0]
        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection(command_name,
                                                       self.shard_hint)
            self.connection = conn
        try:
	    # 发送命令
            conn.send_command(*args)
            return self.parse_response(conn, command_name, **options)
        except ConnectionError:
            conn.disconnect()
            if not self.watching:
                conn.send_command(*args)
                return self.parse_response(conn, command_name, **options)
            self.reset()
            raise

    # 不立即执行命令：把命令添加到command_stack 
    # @see BasePipeline.execute_command
    def pipeline_execute_command(self, *args, **options):
        self.command_stack.append((args, options))
    	# 返回self本身，用来支持链式调用，eg:
    	# pipe.set('name', 'diaocow').set('age', 25).mget('name', 'age').execute()
        return self 
	
    # 批量执行命令 or 执行事务
    def execute(self, raise_on_error=True):
        stack = self.command_stack
        if not stack:
            return []

        if self.scripts:
            self.load_scripts()
        	
        # 若事务选项打开，则以事务的方式执行命令，否则以普通方式批量执行命令
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

    	# 获取连接
        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('MULTI',
                                                       self.shard_hint)
            self.connection = conn

        try:
	    # 批量执行命令
            return execute(conn, stack, raise_on_error)
        except ConnectionError:
            conn.disconnect()
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issue
            # will fail, propegating the real ConnectionError
            if self.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return execute(conn, stack, raise_on_error)
        finally:
            self.reset()

    # 以事务方式批量执行命令 
    # @see BasePipeline.execute
    def _execute_transaction(self, connection, commands, raise_on_error):
        # 把命令组中的命令置于MULTI和EXEC之间
        cmds = chain([(('MULTI', ), {})], commands, [(('EXEC', ), {})])
        # 把命令按照Redis Command协议转换
        all_cmds = SYM_EMPTY.join(
            starmap(connection.pack_command,
                    [args for args, options in cmds]))
	    # 发送命令
        connection.send_packed_command(all_cmds)
        errors = []
    
        # 解析MULTI命令的执行结果(若redis执行正确，返回'OK')
        try:
            self.parse_response(connection, '_')
        except ResponseError:
	        # 若出现错误，则添加仅errors列表(该命令基本不会出错)
            errors.append((0, sys.exc_info()[1]))

        # 循环解析命令组中命令执行结果(若redis执行正确，返回'QUEUED')
        for i, _ in enumerate(commands):
            try:
                self.parse_response(connection, '_')
            except ResponseError:
	            # 若出现错误，则添加仅errors列表(对于命令组中的命令，redis会做命令参数个数检查，由于客户端保证了这点，所以这边也基本不会出错)
                errors.append((i, sys.exc_info()[1]))

        # 解析EXEC命令的执行结果
        #
        # 注意，redis其实刚才并没有真正执行命令组中的命令，而只是缓存起来(QUEUED)，当它收到EXEC命令后，才把刚才缓存中的命令逐一执行
        try:
	    # reponse中结果是一个list, 最终样子会是：
	    # response[0]: 命令组中第一个命令执行的结果或是异常
	    # response[1]: 命令组中第二个命令执行的结果或是异常
	    # ...
	    # response[n-1]: 命令组中最后一个命令执行的结果或是异常
	    #
            response = self.parse_response(connection, '_')
        except ExecAbortError: 
	        # FIXME 
            if self.explicit_transaction:
                self.immediate_execute_command('DISCARD') 
            if errors:
                raise errors[0][1]
            raise sys.exc_info()[1]

	# 事务执行失败，原因：在事务执行期间，监视的键被修改
        if response is None:
            raise WatchError("Watched variable changed.")

	# 顺便说一下，导致事务执行失败一共有三个原因：
	#
	# 1. 命令参数个数不正确
	#	 譬如使用set命令：set name diaocow jack (客户端可以避免这种错误类型）
	#
	# 2. 命令类型不正确
	# 	 譬如对string类型的键，使用list类型的命令：set name diaocow; lpush name 25
	# 	 这种错误属于运行时错误，reponse中的结果类似：[ResponseError('ERR Operation against a key holding the wrong kind of value',)]
	#
	# 3. 监视的键被修改
	#

        for i, e in errors:  
            response.insert(i, e)

        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")

        # 1.若raise_on_error中True(默认值)
        #	当命令组中的有未正确执行的命令，抛出异常(事务执行失败)Examples:
        #
        #	>>> pipeline.set('name', 'diaocow')
        #	>>> pipeline.get('name')
        #	>>> pipeline.lpush('name', 25)     <== 命令类型错误
        #	>>> pipeline.execute()
        #	Traceback (most recent call last):	
        #   ....
        #	....
        #	redis.exceptions.ResponseError: ERR Operation against a key holding the wrong kind of value
        #
        # 2.若raise_on_error中False
        #	不抛异常(无视事务执行失败) ，返回命令组执行结果，Examples:
        #
        #	>>> pipeline.set('name', 'diaocow')
        #	>>> pipeline.get('name')
        #	>>> pipeline.lpush('name', 25)     <== 命令类型错误
        #	>>> pipeline.execute(raise_on_error=False)
        #	[True, 'diaocow', ResponseError('ERR Operation against a key holding the wrong kind of value',)]'')'']
        #
        if raise_on_error:
            self.raise_first_error(response)

        data = []
        for r, cmd in izip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)
        return data

    # 以普通方式批量执行命令
    # @see BasePipeline.execute
    def _execute_pipeline(self, connection, commands, raise_on_error):
        # 把命令组按照Redis Command协议转换
        all_cmds = SYM_EMPTY.join(
            starmap(connection.pack_command,
                    [args for args, options in commands]))
        # 向redis服务器发送命令
        connection.send_packed_command(all_cmds)

        # 1.若raise_on_error中True(默认值)
        #   当命令组中的有未正确执行的命令，抛出异常
        #
        # 2.若raise_on_error中False
        #   不抛异常
        #
        response = []
        for args, options in commands:
            try:
                response.append(
                    self.parse_response(connection, args[0], **options))
            except ResponseError:
                response.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(response)
        return response

    def raise_first_error(self, response):
        for r in response:
            if isinstance(r, ResponseError):
                raise r

    # 获取命令执行结果
    def parse_response(self, connection, command_name, **options):
        result = StrictRedis.parse_response(
            self, connection, command_name, **options)
        # 若执行的命令为：UNWATCH，EXEC，DISCARD，则重置watching状态
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        # 若执行的命令为：WATCH，则将watching状态置为True
        elif command_name == 'WATCH':
            self.watching = True
        return result

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        exists = immediate('SCRIPT', 'EXISTS', *shas, **{'parse': 'EXISTS'})
        if not all(exists):
            for s, exist in izip(scripts, exists):
                if not exist:
                    immediate('SCRIPT', 'LOAD', s.script, **{'parse': 'LOAD'})


    # 监视某些键，若在事务执行期间，监视的键被修改，则事务执行失败
    #
    # Examples: 原子性递增值
    #
    # >>> with r.pipeline() as pipe:
    # ...     while True:
    # ...         try:
    # ...             # 监视键 visitors
    # ...             current_value = pipe.get('visitors')
    # ...             next_value = int(current_value) + 1     
    # ...             # 原子性递增visitors          
    # ...             pipe.multi()                                      
    # ...             pipe.set('visitors', next_value)          
    # ...             pipe.execute()
    # ...             # 递增visitors值成功，break
    # ...             break                                                                                                           
    # ...         except WatchError:
    # ...             # 若因WathError导致事务执行失败，说明在执行 set visitors, next_value命令之前，visitor的值已经被其他客户端修改，只能retry 
    # ...             continue
    #
    def watch(self, *names):
        # WATCH命令只能在执行事务之前使用 
        if self.explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')
        return self.execute_command('WATCH', *names)

    # 取消该客户端监视的所有键
    def unwatch(self):
        return self.watching and self.execute_command('UNWATCH') or True
    	
    def script_load_for_pipeline(self, script):
        "Make sure scripts are loaded prior to pipeline execution"
        self.scripts.add(script)


class StrictPipeline(BasePipeline, StrictRedis):
    "Pipeline for the StrictRedis class"
    pass


class Pipeline(BasePipeline, Redis):
    "Pipeline for the Redis class"
    pass


class Script(object):
    "An executable LUA script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = registered_client.script_load(script)

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, BasePipeline):
            # make sure this script is good to go on pipeline
            client.script_load_for_pipeline(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)


class LockError(RedisError):
    "Errors thrown from the Lock"
    pass


class Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """

    LOCK_FOREVER = float(2 ** 31 + 1)  # 1 past max unix time

    def __init__(self, redis, name, timeout=None, sleep=0.1):
        """
        Create a new Lock instnace named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        Note: If using ``timeout``, you should make sure all the hosts
        that are running clients have their time synchronized with a network
        time service like ntp.
        """
        self.redis = redis
        self.name = name
        self.acquired_until = None
        self.timeout = timeout
        self.sleep = sleep
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=True):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.
        """
        sleep = self.sleep
        timeout = self.timeout
        while 1:
            unixtime = mod_time.time()
            if timeout:
                timeout_at = unixtime + timeout
            else:
                timeout_at = Lock.LOCK_FOREVER
            timeout_at = float(timeout_at)
            if self.redis.setnx(self.name, timeout_at):
                self.acquired_until = timeout_at
                return True
            # We want blocking, but didn't acquire the lock
            # check to see if the current lock is expired
            existing = float(self.redis.get(self.name) or 1)
            if existing < unixtime:
                # the previous lock is expired, attempt to overwrite it
                existing = float(self.redis.getset(self.name, timeout_at) or 1)
                if existing < unixtime:
                    # we successfully acquired the lock
                    self.acquired_until = timeout_at
                    return True
            if not blocking:
                return False
            mod_time.sleep(sleep)

    def release(self):
        "Releases the already acquired lock"
        if self.acquired_until is None:
            raise ValueError("Cannot release an unlocked lock")
        existing = float(self.redis.get(self.name) or 1)
        # if the lock time is in the future, delete the lock
        if existing >= self.acquired_until:
            self.redis.delete(self.name)
        self.acquired_until = None
