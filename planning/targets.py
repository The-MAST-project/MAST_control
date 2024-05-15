from common.utils import init_log
from common.api import ApiUnit
import logging
from typing import List

logger = logging.getLogger('targets')
init_log(logger)


class Target:

    DEFAULT_PRIORITY = 0

    def __init__(self, name: str, ra: float, dec: float, priority: float = DEFAULT_PRIORITY):
        self.name: str = name
        self.ra: float = ra
        self.dec: float = dec
        self.priority:float = priority
        self.required_units: List[int] = []
        self.units: List[ApiUnit] = []
        self.number_of_visits: int = 1  # from config
        self.observing_duration: float  # [seconds] from config

    def snr(self) -> float:
        """
        Calculates the target's Signal-to-Noise-Ratio at the current time
        :return:
        """
        pass
