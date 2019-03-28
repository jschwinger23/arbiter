import socket
import functools
from logging import getLogger

from . import coroutine
from .coroutine import Coroutine
from .popen import Popen

logger = getLogger(__name__)


class Worker(object):
	def __init__(self, entry_func, socket=None):
		self.entry_func = entry_func
		self.wrapper = self._wrap(entry_func)

	def __call__(self):
		return self.wrapper()

	def _wrap(self, func):
		@functools.wraps(func)
		def wrapper():
			func(socket)

		return wrapper


class WorkerManager(object):
	def __init__(
		self,
		entry_func,
		replicas,
		graceful_timeout=30,
		bind_address=None,
		heartbeat_timeout=30
	):
		self.entry_func = entry_func
		self.replicas = replicas
		self.graceful_timeout = graceful_timeout
		self.bind_address = bind_address
		self.heartbeat_timeout = heartbeat_timeout

		self.sock = None
		self._popens = {}
		self._running = False

		if bind_address:
			family = socket.AF_INET
			if bind_address.startswith('unix:'):
				family = socket.AF_UNIX
				bind_address = bind_address[5:]
			self.sock = socket.socket(family, socket.SOCK_STREAM)
			self.bind

		self.worker = Worker(entry_func, self.sock)

	def run(self):

		if self.sock:
			self.sock.bind(self.bind_address)
			self.sock.listen(2048)

		self._running = True
		while self._running:
			Coroutine(target=self.maintain).start()
			coroutine.sleep(1)

	def stop(self):
		self._running = False
		self.purge(scorch=True)

	def purge(self, scorch=False):
		logger.info('kill all children')

		if scorch:
			self.replicas = 0

		coros = []
		while self._popens:
			pid, popen = self._popens.popitem()
			coro = Coroutine(target=popen.terminate, args=(self.graceful_timeout,))
			coro.start()
			coros.append(coro)

		for coro in coros:
			coro.join()

	def maintain(self):
		logger.debug('maintaining workers')
		self._repopulate()
		self._depopulate()

	def reap_exited(self, pid):
		if pid not in self._popens:
			# terminated by manager
			return

		del self._popens[pid]
		logger.info('worker %s reaped' % pid)

	def _depopulate(self):
		coros = []
		while len(self._popens) > self.replicas:
			pid, popen = self._popens.popitem()
			coro = Coroutine(target=popen.terminate, args=(self.graceful_timeout,))
			coro.start()
			coros.append(coro)

		for coro in coros:
			coro.join()

	def _repopulate(self):
		shortage = self.replicas - len(self._popens)
		for _ in range(shortage):
			popen = Popen(self.worker)
			self._popens[popen.pid] = popen

		if shortage > 0:
			logger.info('%s worker(s) supplemented' % shortage)

	def incr_worker(self):
		logger.info('one worker increased')
		self.replicas += 1
		self.maintain()

	def decr_worker(self):
		logger.info('one worker decreased')
		self.replicas -= 1
		self.maintain()
