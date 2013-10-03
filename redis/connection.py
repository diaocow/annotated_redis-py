from itertools import chain
import os
import socket
import sys


from redis._compat import (b, xrange, imap, byte_to_chr, unicode, bytes, long,
                           BytesIO, nativestr, basestring,
                           LifoQueue, Empty, Full)
from redis.exceptions import (
    RedisError,
    ConnectionError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
)
from redis.utils import HIREDIS_AVAILABLE
if HIREDIS_AVAILABLE:
    import hiredis


SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_LF = b('\n')
SYM_EMPTY = b('')


class PythonParser(object):
    "Plain Python parsing class"
    MAX_READ_LENGTH = 1000000
    encoding = None

    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
    }

    def __init__(self):
        self._fp = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._fp = connection._sock.makefile('rb')
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def read(self, length=None):
        """
        Read a line from the socket if no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        try:
            if length is not None:
                bytes_left = length + 2  # read the line ending
                if length > self.MAX_READ_LENGTH:
                    # apparently reading more than 1MB or so from a windows
                    # socket can cause MemoryErrors. See:
                    # https://github.com/andymccurdy/redis-py/issues/205
                    # read smaller chunks at a time to work around this
                    try:
                        buf = BytesIO()
                        while bytes_left > 0:
                            read_len = min(bytes_left, self.MAX_READ_LENGTH)
                            buf.write(self._fp.read(read_len))
                            bytes_left -= read_len
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()
                return self._fp.read(bytes_left)[:-2]

            # no length, read a full line
            return self._fp.readline()[:-2]
        except (socket.error, socket.timeout):
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            return self.EXCEPTION_CLASSES[error_code](response)
        return ResponseError(response)

    def read_response(self):
        response = self.read()
        if not response:
            raise ConnectionError("Socket closed on remote end")

        byte, response = byte_to_chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error")

        # server returned an error
        if byte == '-':
            response = nativestr(response)
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == '+':
            pass
        # int value
        elif byte == ':':
            response = long(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = self.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response


class HiredisParser(object):
    "Parser class for connections using Hiredis"
    def __init__(self):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        self._sock = connection._sock
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': ResponseError,
        }
        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def read_response(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")
        response = self._reader.gets()
        while response is False:
            try:
                buffer = self._sock.recv(4096)
            except (socket.error, socket.timeout):
                e = sys.exc_info()[1]
                raise ConnectionError("Error while reading from socket: %s" %
                                      (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # proactively, but not conclusively, check if more data is in the
            # buffer. if the data received doesn't end with \n, there's more.
            if not buffer.endswith(SYM_LF):
                continue
            response = self._reader.gets()
        return response

# 如果安装了hiredis，则使用HiredisParser作为DefaultParser，否则使用PythonParser作为默认DefaultParser
if HIREDIS_AVAILABLE:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser


# TCP连接实现类（默认连接实现类）
class Connection(object):
	
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser):
        self.pid = os.getpid()
		# redis服务器的host/port
        self.host = host
        self.port = port
		# 使用哪个数据库，默认使用编号为0的数据库（一个redis实例默认有16个数据库）
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
		# response解析器，优先选择HiredisParser 
        self._parser = parser_class()

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

	# 连接Redis服务器
    def connect(self):
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.error:
            e = sys.exc_info()[1]
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        try:
            self.on_connect()
        except RedisError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock

    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            if nativestr(self.read_response()) != 'OK':
                raise AuthenticationError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if nativestr(self.read_response()) != 'OK':
                raise ConnectionError('Invalid Database')

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

	# 关闭与Redis服务器之间的连接
    def disconnect(self):
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

	       raise

	# 向redis服务器发送命令
    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

	# 把命令按照Redis协议转换，譬如： 
	# set name diaocow =>
	# *3\r\n$3\r\nset\r\n$4\r\nname\r\n$7\r\ndiaocow\r\n FIXME
    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        args_output = SYM_EMPTY.join([
            SYM_EMPTY.join((SYM_DOLLAR, b(str(len(k))), SYM_CRLF, k, SYM_CRLF))
            for k in imap(self.encode, args)])
        output = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF, args_output))
        return output

	# @see send_command
    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        try:
            self._sock.sendall(command)
        except socket.error:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (_errno, errmsg))
        except:
            self.disconnect()
     
	# 读取命令执行结果
    def read_response(self):
        try:
            response = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response

    def encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, bytes):
            return value
        if isinstance(value, float):
            value = repr(value)
        if not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value


# UnixDomain连接实现类
class UnixDomainSocketConnection(Connection):
    def __init__(self, path='', db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser):
		# 参数信息基本与Connection类相同，不再赘述
        self.pid = os.getpid()
        self.path = path
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = parser_class()

    def _connect(self):
        "Create a Unix domain socket connection"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect(self.path)
        return sock

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to unix socket: %s. %s." % \
                (self.path, exception.args[0])
        else:
            return "Error %s connecting to unix socket: %s. %s." % \
                (exception.args[0], self.path, exception.args[1])


# redis-py 默认连接池
class ConnectionPool(object):

    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        self.pid = os.getpid()
		# 连接池中连接的实现类（默认为Connection类）
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
		# 连接池中允许的最大连接数
        self.max_connections = max_connections or 2 ** 31
		# 目前连接池中已经创建的连接数
        self._created_connections = 0
		# 连接池中的空闲连接
        self._available_connections = []
		# 连接池中正在被使用的连接
        self._in_use_connections = set()

	# 检测连接池有效性，不允许跨进程使用 
    def _checkpid(self):
		# 若连接池所属进程不同于当前进程，则重新实例化连接池 
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.connection_class, self.max_connections,
                          **self.connection_kwargs)

    # 从连接池从获取一个连接
    def get_connection(self, command_name, *keys, **options):
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
			# 若没有可用的空闲连接，则创建一个新连接
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

	# 创建一个新连接
    def make_connection(self):
		# 若当前连接池中的连接数已经超过max_connections，则raise exception
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
		# ps: 其实这里不是线程安全的，存在race condition
		self._created_connections += 1 
        return self.connection_class(**self.connection_kwargs)

	# 释放一个连接（放回连接池）
    def release(self, connection):
        self._checkpid()
        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)
	
	# 关闭连接池中的所有连接
    def disconnect(self):
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()


# 另一个版本的连接池实现， 与redis-py 默认连接池ConnectionPool主要区别在于：
# 当连接池中没有可用的空闲连接，并且此时连接数也已经到达了上限，若此时有客户端尝试获取一个连接，它不会抛出异常，而是根据timeout的值采取相应的措施：
# 	a. timeout == None: 一直阻塞客户端，直到有一个连接可用
# 	b. timeout == numsec: 在numsec时间范围内阻塞客户端，若还是没有可用连接，则抛出异常
#
class BlockingConnectionPool(object):

    def __init__(self, max_connections=50, timeout=20, connection_class=None,
                 queue_class=None, **connection_kwargs):
		# 连接池中连接默认实现类：Connection
        if connection_class is None:
            connection_class = Connection
		# 连接池队列默认实现类：LifoQueue
        if queue_class is None:
            queue_class = LifoQueue

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.queue_class = queue_class
        self.max_connections = max_connections
        self.timeout = timeout
        self.pid = os.getpid()

		# 检测max_connections有效性
        is_valid = isinstance(max_connections, int) and max_connections > 0
        if not is_valid:
            raise ValueError('``max_connections`` must be a positive integer')

        # 初始化连接池队列，默认用None对象填充 
        self.pool = self.queue_class(max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break
        # Keep a list of actual connection instances so that we can disconnect them later.
        self._connections = []

	# 检测连接池有效性，不允许跨进程使用 
    def _checkpid(self):
        pid = os.getpid()
        if self.pid == pid:
            return
		# 若连接池所属进程不同于当前进程，则重新实例化连接池 
        self.disconnect()
        self.reinstantiate()

	# 创建一个新连接
    def make_connection(self):
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

	# 获取一个可用连接
    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        self._checkpid()
        connection = None
        try:
			# 从连接池队列中获取一个连接
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            raise ConnectionError("No connection available.")

		# 若获取的connection对象为None，则创建一个新的连接
        if connection is None:
            connection = self.make_connection()
        return connection

	# 释放连接（放回连接池）
    def release(self, connection):
        self._checkpid()
        try:
            self.pool.put_nowait(connection)
        except Full:
            # This shouldn't normally happen but might perhaps happen after a
            # reinstantiation. So, we can handle the exception by not putting
            # the connection back on the pool, because we definitely do not
            # want to reuse it.
            pass

	# 关闭连接池中所有连接
    def disconnect(self):
        for connection in self._connections:
            connection.disconnect()

	# 重新实例化连接池
    def reinstantiate(self):
        self.__init__(max_connections=self.max_connections,
                      timeout=self.timeout,
                      connection_class=self.connection_class,
                      queue_class=self.queue_class, **self.connection_kwargs)
