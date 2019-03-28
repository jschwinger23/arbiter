import os
import sys
import signal
import logging

from . import coroutine
from .coroutine import Coroutine
from .worker import WorkerManager

logging.basicConfig(
	level=getattr(logging, os.getenv('PY_LOG_LEVEL', 'INFO'), logging.INFO)
)
logger = logging.getLogger(__name__)


class Arbiter(object):
	SIGNAL_NAMES = 'HUP QUIT INT TERM CHLD TTIN TTOU'.split()

	def __init__(
		self, entry_func, replicas, bind_address=None, graceful_timeout=30
	):
		self.running = False

		self.worker_manager = WorkerManager(
			entry_func,
			replicas,
			graceful_timeout=graceful_timeout,
			bind_address=bind_address
		)

		self.graceful_timeout = graceful_timeout

	def run(self, **context):
		self.add_signal_handlers()

		# TODO: daemonize
		self._run()

	def _run(self):
		worker_manager = self.worker_manager
		Coroutine(target=worker_manager.run).start()

		self.running = True
		while self.running:
			coroutine.sleep(1)

		logger.info('arbiter exiting')
		worker_manager.stop()
		sys.exit(0)

	def add_signal_handlers(self):
		for signal_name in self.SIGNAL_NAMES:
			sig = getattr(signal, 'SIG' + signal_name)
			handler = getattr(self, 'handle_' + signal_name)
			coroutine.add_signal_handler(sig, handler)

	def handle_HUP(self, signum, frame):
		logger.info('handling signal HUP')
		self.worker_manager.purge()
		self.worker_manager.maintain()

	def handle_TERM(self, signum, frame):
		logger.info('handling signal %s' % signum)
		self.running = False

	handle_QUIT = handle_INT = handle_TERM

	def handle_CHLD(self, signum, frame):
		logger.info('handling signal CHLD')
		while True:
			try:
				pid, status = os.waitpid(-1, os.WNOHANG)
			except OSError:
				# No child processes
				break
			else:
				if pid == 0:
					break

			self.worker_manager.reap_exited(pid)

	def handle_TTIN(self, signum, frame):
		logger.info('handling signal TTIN')
		self.worker_manager.incr_worker()

	def handle_TTOU(self, signum, frame):
		logger.info('handling signal TTOU')
		self.worker_manager.decr_worker()
