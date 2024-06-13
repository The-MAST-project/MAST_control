import pathlib
import tomlkit
import logging
from typing import List
from common.config import Config
from common.utils import init_log

logger = logging.getLogger('tasks')
init_log(logger)

class Task:
    pass

    def __init__(self, path: str):
        self.logger = logging.getLogger('plan')
        init_log(self.logger)

        try:
            with open(path, 'r') as f:
                self.toml = tomlkit.load(f)
        except Exception as e:
            self.logger.error(f"could not read TOML file '{path}'")
            return

        self.path = path
        self.in_progress: bool = False
        self._units = parse_units(self.toml['equipment']['units'])

    @property
    def units(self):
        return self._units

    def is_valid(self) -> bool:
        pass

    def merit(self) -> float:
        return 0.0


def parse_units(specifier: str | List[int] | List[str]) -> List[int]:
    units: List[int] = []
    if isinstance(specifier, list):
        for spec in specifier:
            if isinstance(spec, int):
                if spec in range(1, Config.NUMBER_OF_UNITS+1):
                    units.append(spec)
                else:
                    logger.error(f"bad unit specifier '{spec}'.  must be in 1..{Config.NUMBER_OF_UNITS}")

    return units


def sort_by_merit(plans: List[Task]) -> List[Task]:
    """
    Calculate each plan's merit according to: coordinates -> elevation, air-mass, priority
    :return: A sorted list with the most meritous plan as first
    """
