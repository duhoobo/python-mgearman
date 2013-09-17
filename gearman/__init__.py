"""
Gearman API - Client, worker, and admin client interfaces
"""

__version__ = '2.0.2'

from gearman.admin import GearmanAdmin
from gearman.client import GearmanClient
from gearman.worker import GearmanWorker

from gearman.connectionmanager import DataEncoder
from gearman.job import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, \
        JOB_PENDING, JOB_CREATED, JOB_FAILED, JOB_COMPLETE

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

