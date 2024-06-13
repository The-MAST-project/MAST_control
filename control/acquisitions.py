from planning.tasks import Task
from common.utils import path_maker, Activities, RepeatTimer
from enum import IntFlag, auto
import logging


class AcquisitionActivities(IntFlag):
    Starting = auto()
    Ending = auto()
    Slewing = auto()
    Exposing = auto()


class Acquisition(Activities):
    """
    An Acquisition starts AFTER the @plan was checked and found runnable
    """

    def __init__(self, plan: Task):
        Activities.__init__(self)
        self.folder = path_maker
        self._logger = logging.getLogger('acquisition')
        self.start_activity(AcquisitionActivities.Starting)

        self.timer: RepeatTimer = RepeatTimer(interval=2, function=self.ontimer)
        self.timer.start()

    def ontimer(self):
        pass

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def status(self) -> dict:
        ret = {
            'activities': self.activities,
            'activities_verbal': self.activities.__repr__()
        }

        return ret
