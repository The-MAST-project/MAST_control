import json
import logging
from common.config import Config
from common.utils import init_log
# from common.units import UnitLocator
from common.parsers import parse_units
from typing import List, Optional
from model import TaskModel, TargetModel, ConstraintsModel, SpectrographModel
import tasks
from ulid import ULID

logger = logging.getLogger('tasks')
init_log(logger)

from astropy.coordinates import Longitude, Latitude
from astropy import units as u

def sort_by_merit(plans: List[TaskModel]) -> List[TaskModel]:
    """
    Calculate each plan's merit according to: coordinates -> elevation, air-mass, priority
    :return: A sorted list with the most meritous plan as first
    """

class Target:

    def __init__(self, ra: float, dec: float, units: List[str]):
        self.ra: float = ra
        self.dec: float = dec
        self.units: List[str] = units

def target_from_models(target_model: TargetModel) -> Target:
    used_units_tuples: List[str] = []
    sites = Config().sites

    if not target_model:
        raise ValueError('No targets specified, at least one expected')

    try:
        ra: Longitude = Longitude(target_model.ra, unit=u.hourangle)
    except ValueError as e:
        raise ValueError(f"Invalid RA '{target_model.ra}' (error={e})")

    try:
        dec: Latitude = Latitude(target_model.dec, unit=u.deg)
    except ValueError as e:
        raise ValueError(f"Invalid DEC '{target_model.dec}' (error={e})")

    success, value = parse_units(target_model.units)
    if success:
        units = value
    else:
        raise ValueError(f"Invalid units specifier '{target_model.units}', error: {value}")

    return Target(ra.value, dec.value, units)

class Moon:
    def __init__(self, max_phase: float, min_distance: float):
        self.max_phase: float = max_phase
        self.min_distance: float = min_distance

class Airmass:
    def __init__(self, max_airmass: float):
        self.max: float = max_airmass

class Seeing:
    def __init__(self, max_seeing: float):
        self.max: float = max_seeing

class TimeWindow:
    def __init__(self, start: str | None = None, end: str | None = None):
        self.start: str = start
        self.end: str = end

class Constraints:
    def __init__(self, constraints_model: ConstraintsModel):
        self.moon = Moon(constraints_model.moon.max_phase, constraints_model.moon.min_distance) if hasattr(constraints_model, 'airmass') else None
        self.airmass = Airmass(constraints_model.airmass.max) if hasattr(constraints_model, 'airmass') else None
        self.seeing = Seeing(constraints_model.seeing.max) if hasattr(constraints_model, 'seeing') else None
        self.time_window = TimeWindow(constraints_model.when.start, constraints_model.when.end) if hasattr(constraints_model, 'when') else None

class Spectrograph:
    def __init__(self, spec_model: SpectrographModel):
        self.instrument: str = spec_model.instrument
        self.exposure: float = spec_model.exposure
        self.lamp = spec_model.lamp
        self.binning_x = spec_model.binning_x
        self.binning_y = spec_model.binning_y

class Task:

    def __init__(self, task_model: TaskModel, toml, file_name: Optional[str] = None):
        self.toml = toml
        self.file_name: str = file_name
        self.model = task_model
        self.ulid = task_model.settings.ulid if task_model.settings.ulid else None
        if not self.ulid:
            self.ulid = ULID()
        self.owner = task_model.settings.owner if task_model.settings.owner else None
        self.merit: int = task_model.settings.merit if task_model.settings.merit else 0
        self.state: str = task_model.settings.state if task_model.settings.state else 'new'
        self.ready_to_guide_timeout: int = task_model.settings.ready_to_guide_timeout if task_model.settings.ready_to_guide_timeout else 0

        self.target: Target = target_from_models(task_model.target)
        self.spec = Spectrograph(task_model.spec)
        self.constraints = Constraints(task_model.constraints)

    def save(self):
        """
        Uses a task's TOML field to save the task
        :return:
        """
        pass

    def complete(self):
        """
        Sets the task's state to 'complete' and moves the file to the 'completed' folder
        :return:
        """
        pass

    def run(self):
        """
        Runs the task
        :return:
        """
        # TODO:
        #  - if the spec is not operational, return, else tell the spec to prepare (lamp, filter, etc)
        #  - for each target
        #    - let all the target's units acquire and reach UnitActivities.Guiding within timeout
        #    - if the minimum required units are guiding, the target is viable, else tell the units to abort
        #  - if there are viable targets, tell the spec to expose
        #
        pass

    def abort(self):
        """
        Aborts the task
        :return:
        """

        for unit in self.target.units:
            pass  # unitApi(u, 'abort')

    def to_dict(self):
        # Convert the User object to a dictionary and recursively convert nested objects
        def convert(obj):
            if obj == self.model or obj == self.toml:
                return
            if isinstance(obj, list):
                return [convert(item) for item in obj]
            elif hasattr(obj, "__dict__"):
                return {key: convert(value) for key, value in obj.__dict__.items()}
            elif obj == self.ulid:
                return f"{self.ulid}"
            else:
                return obj

        return convert(self)


if __name__ == '__main__':
    file = 'dummy-task.toml'
    task = None
    try:
        with open(file, 'r') as fp:
            task = tasks.load(fp)
    except ValueError as e:
        raise ValueError(f"{e}")

    print(json.dumps(task.to_dict(), indent=2))
