import linuxfd
from functools import wraps, partial
from greenlet import greenlet as Greenlet

from .ioloop import IOLoop

EVENT_LOOP_COROUTINE = None


def _get_event_loop_coroutine():
	global EVENT_LOOP_COROUTINE
	if not EVENT_LOOP_COROUTINE:
		event_loop = IOLoop.get_instance()
		EVENT_LOOP_COROUTINE = Coroutine(event_loop.run_forever)
	return EVENT_LOOP_COROUTINE


def sleep(sec):
	ioloop = IOLoop.get_instance()

	if sec == 0:
		ioloop.call_soon(_resume_current())
	else:
		ioloop.call_later(sec, _resume_current())

	_yield_current()


def add_signal_handler(sig, handler):
	ioloop = IOLoop.get_instance()
	ioloop.add_signal_handler(sig, handler)


def _resume_current():
	crtn = Coroutine.current()
	return crtn.resume


def _yield_current():
	_get_event_loop_coroutine().resume()


class Coroutine:
	@classmethod
	def current(cls):
		return cls(greenlet=Greenlet.getcurrent())

	def __init__(
		self,
		target=None,
		args=None,
		kwargs=None,
		greenlet=None,
	):
		args = args or ()
		kwargs = kwargs or {}

		if greenlet:
			target = greenlet.switch

		@wraps(target)
		def wrapper():
			target(*args, **kwargs)
			self._finish_fd.write()

		self.greenlet = Greenlet(wrapper)
		if target != IOLoop.get_instance().run_forever:
			self.greenlet.parent = _get_event_loop_coroutine().greenlet

		self._finish_fd = linuxfd.eventfd(initval=0, nonBlocking=True)

	def resume(self):
		return self.greenlet.switch()

	def start(self):
		ioloop = IOLoop.get_instance()
		ioloop.call_soon(self.greenlet.switch)
		sleep(0)

	def join(self):
		ioloop = IOLoop.get_instance()

		def callback(resume, ioloop):
			ioloop.del_reader(self._finish_fd)
			resume()

		ioloop.add_reader(self._finish_fd, partial(callback, _resume_current()))
		_yield_current()
