import select
from collections import namedtuple, deque

from .fd_pool import FDPool


class Event(namedtuple('Event', ['fd', 'func'])):
	def callback(self, ioloop):
		raise NotImplementedError


class TimerEvent(Event):
	def callback(self, ioloop):
		self.fd.read()
		ioloop.del_reader(self.fd)
		self.func()


class SignalEvent(Event):
	def callback(self, ioloop):
		status = self.fd.read()
		self.func(status['signo'], None)


class IOLoop(object):
	_instance = None

	@classmethod
	def get_instance(cls):
		if not cls._instance:
			cls._instance = cls()
		return cls._instance

	def __init__(self):
		self.readers = set()
		self.writers = set()
		self.fds = {}
		self.ready = deque()
		self.fd_pool = FDPool.get_instance()

	def run_forever(self):
		self.running = True
		while self.running:
			while self.ready:
				callback = self.ready.pop()
				callback()

			for callback in self._select():
				callback(self)

	def _select(self):
		readables, writables, _ = select.select(self.readers, self.writers, [])

		for readable in readables:
			yield self.fds[readable]

		for writable in writables:
			yield self.fds[writable]

	def add_reader(self, fd, callback):
		self.fds[fd] = callback
		self.readers.add(fd)

	def del_reader(self, fd):
		del self.fds[fd]
		self.readers.remove(fd)
		self.fd_pool.release(fd)

	def call_soon(self, callback):
		self.ready.appendleft(callback)

	def call_later(self, delay, func):
		fd = self.fd_pool.get_timerfd(delay)
		self.add_reader(fd, TimerEvent(fd, func).callback)

	def add_signal_handler(self, sig, func):
		fd = self.fd_pool.get_signalfd(sig)
		self.add_reader(fd, SignalEvent(fd, func).callback)
