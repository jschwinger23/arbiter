from logging import getLogger

from .popen import Popen


logger = getLogger(__name__)


class WorkerManager(object):
	def __init__(self, worker, worker_num):
		self.worker = worker
		self.worker_num = worker_num
		self._popens = []

	def start(self):
		for _ in range(self.num):
			popen = Popen(self.worker)
			self._popens.append(popen)

	def maintain(self):
		self._murder_redundant()
		self._murder_blocked()
		self._reap_exited()
		self._repopulate()

	def _murder_redundant(self):
		redundant = len(self._popens) - self.worker_num
		for _ in range(redundant):
			popen = self._popens.pop()
			popen.terminate()
			logger.info('worker %s killed due to redundancy' % popen.pid)

	def _murder_blocked(self):
		pass

	def _reap_exited(self):
		for i, popen in enumerate(self._popens):
			if popen.poll():
				popen.wait()
				del self._popens[i]
				logger.info('worker %s reaped' % popen.pid)

	def _repopulate(self):
		shortage = self.worker_num - len(self._popens)
		for _ in range(shortage):
			popen = Popen(self.worker)
			self._popens.append(popen)

		if shortage > 0:
			logger.info('%s worker(s) supplemented' % shortage)
