import os.path
import time

from common.utils import init_log, path_maker, BASE_CONTROL_PATH, Component, time_stamp
from common.fswatcher import FsWatcher
from common.config import Config
from common.api import ApiUnit, ApiSpec, ApiResponse
from planning.plans import Plan, sort_by_merit
import logging
from typing import List
from watchdog.events import FileSystemEvent
from pathlib import Path
from threading import Lock, Thread
from fastapi import APIRouter
import random

logger = logging.getLogger('scheduler')
init_log(logger)


class Spec(Component):

    @property
    def was_shut_down(self) -> bool:
        return self._was_shut_down

    @property
    def status(self):
        if self.api:
            return self.api.client.get('status')

    @property
    def name(self) -> str:
        return 'spec'

    def __init__(self):
        Component.__init__(self)
        self.host = 'spec'
        logger.info(f"trying to make a Spec() connection with host='{self.host}' ...")
        self.api = ApiSpec()
        self._was_shut_down = False

    @property
    def detected(self) -> bool:
        stat = self.api.client.get('status') if self.api else None
        return stat.detected if stat else False

    @property
    def connected(self) -> bool:
        stat = self.api.client.get('status') if self.api else None
        return stat.connected if stat else False

    def abort(self):
        if self.api:
            self.api.client.get('abort')

    @property
    def operational(self) -> bool:
        stat = self.api.client.get('status') if self.api else None
        return stat.operational if stat else False

    @property
    def why_not_operational(self) -> List[str]:
        ret: List[str] = []
        if not self.detected:
            ret.append('not detected')
        elif not self.connected:
            ret.append('not connected')
        else:
            why = self.api.client.get('status')
            for reason in why:
                ret.append(reason)

        return ret

    def startup(self):
        if self.api:
            self._was_shut_down = False
            self.api.client.get('startup')

    def shutdown(self):
        if self.api:
            self._was_shut_down = True
            self.api.client.get('shutdown')


class Unit(Component):
    def __init__(self, unit_id: int):
        if not 1 <= unit_id <= Config.NUMBER_OF_UNITS:
            raise ValueError(f"bad {unit_id=}, unit id must be between 1 and {Config.NUMBER_OF_UNITS}")
        Component.__init__(self)
        self._name = f"mast{unit_id:02d}"
        self.host = self.name
        self._was_shut_down = False
        # ApiUnit calls the remote 'status'
        logger.info(f"trying to make a Unit({unit_id=}) connection with host='{self.host}' ...")
        self.api = ApiUnit(self.host)

    @property
    def detected(self) -> bool:
        st = self.api.client.get('status') if self.api else None
        return st.detected if st else False

    @property
    def connected(self) -> bool:
        stat = self.api.client.get('status') if self.api else None
        return stat.connected if stat else False

    @property
    def operational(self) -> bool:
        stat = self.api.client.get('status') if self.api else None
        return stat.operational if stat else False

    @property
    def why_not_operational(self) -> List[str]:
        ret: List[str] = []
        if not self.detected:
            ret.append('not detected')
        elif not self.connected:
            ret.append('not connected')
        elif not self.operational:
            why = self.api.client.get('why_not_operational') if self.api.client else []
            if why:
                for reason in why:
                    ret.append(reason)
        return ret

    @property
    def was_shut_down(self) -> bool:
        return self._was_shut_down

    def startup(self):
        self._was_shut_down = False
        if self.api.client:
            self.api.client.get('startup')

    def shutdown(self):
        self._was_shut_down = True
        if self.api.client:
            self.api.client.get('shutdown')

    @property
    def status(self) -> dict:
        return self.api.client.get('status') if self.api.client else None

    @property
    def name(self) -> str:
        return self._name

    def abort(self):
        if self.api.client:
            self.api.client.get('abort')


class Scheduler:
    """
    The Scheduler:
    - on startup:
      - loads all the targets from files <top>/targets/submitted
      - creates a list units: List[ApiUnit] objects, with Config.NUMBER_OF_UNITS elements (detected units)
      - creates an ApiSpec object
      - creates a list of targets, sorted by merit
      - creates a web service for
        - current list of targets, sorted by merit, editable
        - current list of acquisitions including the one in-progress and those pending
        - status of equipment (units and spec)

    - when needed, i.e. {
          - when a page is refreshed
          - when an acquisition can be started
          - when the acquisitions list is displayed
      }
      - re-sorts the targets according to merit

    - when an acquisition candidate is made:
      - gets the target with the highest merit and allocates units as needed
      - tries to allocate remaining units to other targets, according to their needs
      - when all the operational units are allocated starts the acquisition

    - if the in-progress acquisition succeeds it gets moved to '<top>/acquisitions/completed', if it fails it stays
       in '<top>/acquisitions/pending'
    """

    @property
    def name(self) -> str:
        return 'spec'

    def __init__(self):

        plans_folder = path_maker.make_plans_folder()
        self.pending_folder: str = os.path.join(plans_folder, 'pending')
        self.completed_folder: str = os.path.join(plans_folder, 'completed')

        self.plans: List[Plan] = []
        path = Path(self.pending_folder)
        self.plans_lock = Lock()
        for file in [entry.name for entry in path.iterdir() if entry.is_file()]:
            plan = Plan(file)
            if plan.is_valid():
                self.plans.append(plan)

        with self.plans_lock:
            self.plans.sort(key=lambda p: p.merit)

        self.plans_watcher = FsWatcher(folder=self.pending_folder, handlers={
            'modified': self.on_modified_plan,
            'deleted': self.on_deleted_plan,
            'moved': self.on_moved_plan,
        })

        self._terminated = False

        self.units: List[Unit | None] = [None] * Config.NUMBER_OF_UNITS
        self.spec: Spec | None = None

        def make_unit(slt: int):
            interval = 25 + random.randint(0, 10)
            unit_id = slt + 1
            self.units[slt] = Unit(unit_id=unit_id)
            while not self._terminated and not self.units[slt].api.client.detected:
                time.sleep(interval)
                self.units[slt] = Unit(unit_id=unit_id)
            logger.info(f"made a Unit connection with '{self.units[slt].host}'")

        def make_spec():
            self.spec = Spec()
            while not self._terminated and not self.spec.api.client.detected:
                time.sleep(30)
                self.spec = Spec()
            logger.info(f"made a Spec connection with '{self.spec.host}'")

        for slot in range(0, Config.NUMBER_OF_UNITS):
            Thread(target=make_unit, args=[slot]).start()

        Thread(target=make_spec).start()

    def start_scheduling(self):
        self._terminated = False
        self.plans_watcher.run()

    def stop_scheduling(self):
        self._terminated = True
        self.plans_watcher.stop()

    def on_modified_plan(self, event: FileSystemEvent):
        """
        A plan was modified.  Load, verify and sort it in.
        :return:
        """

    def on_deleted_plan(self, event: FileSystemEvent):
        """
        A plan was deleted.  Delete it from the list
        """
        with self.plans_lock:
            for plan in self.plans:
                if plan.path == event.src_path:
                    if plan.in_progress:
                        return
                    else:
                        self.plans.remove(plan)
                    return

    def on_moved_plan(self, event: FileSystemEvent):
        """
        A plan file was renamed
        """
        found = [plan for plan in self.plans if plan.path == event.src_path]
        if found:
            found[0].path = event.dest_path

    def startup(self):
        self.start_scheduling()

    def shutdown(self):
        self.stop_scheduling()

    def status(self) -> dict:
        not_detected = {
            'detected': False,
        }
        time_stamp(not_detected)
        return {
            'spec': self.spec.status if self.spec.api.client.detected else not_detected,
            'units': [{unit.name: unit.status if unit.api.client.detected else not_detected} for unit in self.units]
        }


scheduler: Scheduler = Scheduler()


def startup():
    global scheduler

    if scheduler is None:
        scheduler = Scheduler()


def shutdown():
    global scheduler

    scheduler.shutdown()


base_path = BASE_CONTROL_PATH
tag = 'Control'
router = APIRouter()

router.add_api_route(base_path + '/status', tags=[tag], endpoint=scheduler.status)
router.add_api_route(base_path + '/startup', tags=[tag], endpoint=scheduler.startup)
router.add_api_route(base_path + '/shutdown', tags=[tag], endpoint=scheduler.shutdown)


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.startup()
