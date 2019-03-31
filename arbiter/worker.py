import sys
import functools
from logging import getLogger

from . import coroutine
from .coroutine import Coroutine
from .popen import Popen
from .fd_pool import FDPool

logger = getLogger(__name__)


class Worker(object):
	def __init__(self, entry_func):
		self.entry_func = entry_func
		self.wrapper = self._wrap(entry_func)
		self._fd_pool = FDPool.get_instance()

	def __call__(self):
		return self.wrapper()

	def _wrap(self, func):
		@functools.wraps(func)
		def wrapper():
			self._fd_pool.on_fork()
			func()
			sys.exit(0)

		return wrapper


class WorkerManager(object):
	def __init__(
		self, entry_func, replicas, graceful_timeout=30, heartbeat_timeout=30
	):
		self.entry_func = entry_func
		self.replicas = replicas
		self.graceful_timeout = graceful_timeout
		self.heartbeat_timeout = heartbeat_timeout

		self._popens = []
		self._running = False

		self.worker = Worker(entry_func)

	def run(self):
		self._running = True
		while self._running:
			Coroutine(target=self.maintain).start()
			coroutine.sleep(1)

	def stop(self):
		self._running = False
		self.purge(sterilize=True)

	def purge(self, sterilize=False):
		logger.info('[arbiter] kill all children')

		if sterilize:
			self.replicas = 0

		coros = []
		while self._popens:
			popen = self._popens.pop()
			coro = Coroutine(
				target=self._gracefully_terminate, args=(popen, self.graceful_timeout)
			)
			coro.start()
			coros.append(coro)

		for coro in coros:
			coro.join()

	def _gracefully_terminate(self, popen, timeout):
		popen.terminate()
		exit_code = popen.wait(timeout)
		if exit_code is None:
			popen.kill()

	def maintain(self):
		logger.debug('[arbiter] maintaining workers')
		self._reap()
		self._repopulate()
		self._depopulate()

	def _reap(self):
		for i, popen in enumerate(self._popens):
			if popen.poll() is not None:
				logger.info('[arbiter] worker %s reaped' % popen.pid)
				del self._popens[i]

	def _depopulate(self):
		coros = []
		while len(self._popens) > self.replicas:
			popen = self._popens.pop()
			coro = Coroutine(
				target=self._gracefully_terminate, args=(
				popen,
				self.graceful_timeout,
				)
			)
			coro.start()
			coros.append(coro)

		for coro in coros:
			coro.join()

	def _repopulate(self):
		shortage = self.replicas - len(self._popens)
		for _ in range(shortage):
			popen = Popen(self.worker)
			self._popens.append(popen)

		if shortage > 0:
			logger.info('[arbiter] %s worker(s) supplemented' % shortage)

	def incr_worker(self):
		logger.info('[arbiter] one worker increased')
		self.replicas += 1
		self.maintain()

	def decr_worker(self):
		logger.info('[arbiter] one worker decreased')
		self.replicas -= 1
		self.maintain()
