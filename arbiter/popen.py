import os
import sys
import time
import errno
import signal
from logging import getLogger

from . import coroutine
from .ioloop import IOLoop

logger = getLogger(__name__)


class Popen(object):
	def __init__(self, worker):
		sys.stdout.flush()
		sys.stderr.flush()
		self.exit_code = None

		self.pid = os.fork()
		if self.pid == 0:
			exit_code = worker()
			sys.stdout.flush()
			sys.stderr.flush()
			os._exit(exit_code)

		self.ioloop = IOLoop.get_instance()

	def poll(self, flag=os.WNOHANG):
		if self.exit_code is None:
			while True:
				try:
					pid, sts = os.waitpid(self.pid, flag)
				except os.error as e:
					if e.errno == errno.EINTR:
						continue
					return None
				else:
					break

			if pid == self.pid:
				if os.WIFSIGNALED(sts):
					self.exit_code = -os.WTERMSIG(sts)
				else:
					assert os.WIFEXITED(sts)
					self.exit_code = os.WEXITSTATUS(sts)
		return self.exit_code

	def wait(self, timeout=None):
		if timeout is None:
			timeout = float('inf')

		deadline = time.time() + timeout
		while True:
			exit_code = self.poll()
			if exit_code is not None:
				break

			balance = deadline - time.time()
			if balance <= 0:
				break

			coroutine.sleep(min(1, balance))
		return exit_code

	def terminate(self):
		if self.exit_code is None:
			logger.info('soft kill child %s' % self.pid)
			try:
				os.kill(self.pid, signal.SIGTERM)
			except OSError as e:
				if self.wait(timeout=0.1) is None:
					raise

	def kill(self):
		if self.exit_code is None:
			logger.warn('hard kill child %s' % self.pid)
			try:
				os.kill(self.pid, signal.SIGKILL)
			except OSError as e:
				if self.wait(timeout=0.1) is None:
					raise
