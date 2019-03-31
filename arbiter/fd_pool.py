import linuxfd
from logging import getLogger
from pysigset import (
	sigprocmask, sigaddset, SIG_BLOCK, SIG_UNBLOCK, SIGSET, NULL
)

logger = getLogger(__name__)


class FDPool(object):
	_instance = None

	@classmethod
	def get_instance(cls):
		if cls._instance is None:
			cls._instance = cls()
		return cls._instance

	def __init__(self):
		self._close_on_fork_fds = set()

	def get_timerfd(self, delay, interval=0, close_on_fork=True):
		fd = linuxfd.timerfd(rtc=True, nonBlocking=True)
		if close_on_fork:
			self._close_on_fork_fds.add(fd)

		fd.settime(delay, 0)
		return fd

	def get_signalfd(self, sig, close_on_fork=True):
		sigset = SIGSET()
		sigaddset(sigset, sig)
		sigprocmask(SIG_BLOCK, sigset, NULL)

		fd = linuxfd.signalfd(signalset={sig}, nonBlocking=True)
		if close_on_fork:
			self._close_on_fork_fds.add(fd)

		return fd

	def get_eventfd(self, close_on_fork=True):
		fd = linuxfd.eventfd(initval=0, nonBlocking=True)
		if close_on_fork:
			self._close_on_fork_fds.add(fd)

		return fd

	def release(self, fd):
		# TODO: singledispatch
		if fd not in self._close_on_fork_fds:
			return

		self._close_on_fork_fds.remove(fd)
		fd.close()

		if isinstance(fd, linuxfd.signalfd):
			sigset = SIGSET()
			for sig in fd.signals():
				sigaddset(sigset, sig)
			logger.debug('unblock sigset %s' % sigset)
			sigprocmask(SIG_UNBLOCK, sigset, NULL)

	def on_fork(self):
		logger.debug('close fds on fork')
		while self._close_on_fork_fds:
			fd = next(iter(self._close_on_fork_fds))
			self.release(fd)
