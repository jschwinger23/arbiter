import os
import sys
import time
import errno
import signal
import gevent


class Popen(object):

	def __init__(self, worker):
		sys.stdout.flush()
		sys.stderr.flush()
		self.returncode = None

		self.pid = os.fork()
		if self.pid == 0:
			if 'random' in sys.modules:
				import random
				random.seed()

			exit_code = worker()
			sys.stdout.flush()
			sys.stderr.flush()
			os._exit(exit_code)

	def poll(self, flag=os.WNOHANG):
		if self.returncode is None:
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
					self.returncode = -os.WTERMSIG(sts)
				else:
					assert os.WIFEXITED(sts)
					self.returncode = os.WEXITSTATUS(sts)
		return self.returncode

	def wait(self, timeout=None):
		if timeout is None:
			return self.poll(0)

		deadline = time.time() + timeout
		delay = 0.0005
		while True:
			res = self.poll()
			if res is not None:
				break
			remaining = deadline - time.time()
			if remaining <= 0:
				break
			delay = min(delay * 2, remaining, 0.05)
			gevent.sleep(delay)
		return res

	def terminate(self):
		if self.returncode is None:
			try:
				os.kill(self.pid, signal.SIGTERM)
			except OSError as e:
				if self.wait(timeout=0.1) is None:
					raise
