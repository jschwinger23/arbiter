import signal
import gevent
from daemon import DaemonContext

from .manager import WorkerManager


class Arbiter(object):
	SIGNAL_NAMES = 'HUP QUIT INT TERM TTIN TTOU'.split()

	def __init__(self, worker, num):
		self.manager = WorkerManager(worker=worker, worker_num=num)

	def run(self, **context):
		daemon_context = DaemonContext(**context)

		daemon_context.signal_map = {
			getattr(signal, signal_name): self.stash_signal
			for signal_name in self.SIGNAL_NAMES
		}

		with daemon_context:
			self._run()

	def _run(self):
		self.manager.start()
		self.add_timer(self.maintain_workers, interval=1)
		self.add_timer(self.handle_signals, interval=1)

	def add_timer(self, job, interval):
		def _timer():
			while True:
				job()
				gevent.sleep(interval)

		return gevent.spawn(_timer)

	def maintain_workers(self):
		self.manager.maintain()

	def handle_signals(self):
		pass

	def stash_signal(self, signum, frame):
		self.stash
