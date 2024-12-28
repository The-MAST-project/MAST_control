import time
import socket
from common import targets
from common.utils import BASE_CONTROL_PATH, Component, time_stamp
from common.mast_logging import init_log
from common.fswatcher import FsWatcher
from common.config import Config
from common.api import ApiUnit, ApiSpec
from common.networking import NetworkedDevice
from dlipower.dlipower.dlipower import SwitchedPowerDevice
import logging
from typing import List, Optional, Literal, Union, Dict
from watchdog.events import FileSystemEvent
from threading import Lock, Thread
from fastapi import APIRouter
import random

logger = logging.getLogger('controller')
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


class ControlledUnit(Component, SwitchedPowerDevice, NetworkedDevice):
    def __init__(self, host: str):
        Component.__init__(self)
        NetworkedDevice.__init__(self, conf={'network': {'host': host}})
        unit_conf = Config().get_unit(host)
        SwitchedPowerDevice.__init__(self, unit_conf['power_switch'], outlet_name='Computer',
                                     upload_outlet_names=False)
        if not self.is_on():
            self.power_on()
            # TODO: wait till the unit computer boots
        self.powered = self.is_on()
        self._was_shut_down = False
        # ApiUnit calls the remote 'status'
        logger.info(f"trying to make an ApiUnit(host='{self.network.hostname}') connection ...")
        self.api = ApiUnit(self.network.hostname)

    @property
    def detected(self) -> bool:
        if not self.api.client:
            return False

        st = self.api.client.get('status') if self.api else None
        return st.detected if st else False

    @property
    def connected(self) -> bool:
        if not self.api.client:
            return False

        stat = self.api.client.get('status') if self.api else None
        return stat.connected if stat else False

    @property
    def operational(self) -> bool:
        if not self.api.client.detected:
            return False

        stat = self.api.client.get('status') if self.api else None
        return stat.operational if stat else False

    @property
    def why_not_operational(self) -> List[str]:
        if not self.api.client:
            return []

        ret: List[str] = []
        if not self.detected:
            ret.append('not detected')
        elif not self.connected:
            ret.append('not connected')
        elif not self.operational:
            if not self.api.client.detected:
                ret.append(f"api client not detected")
            else:
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
        if self.api.client.detected:
            self.api.client.get('startup')

    def shutdown(self):
        self._was_shut_down = True
        if self.api.client.detected:
            self.api.client.get('shutdown')

    @property
    def status(self) -> dict:
        if not self.api.client.detected:
            return {
                'powered': self.powered,
                'detected': False,
            }
        return self.api.client.get('status') if self.api.client else None

    def move_to_coordinates(self, ra: float, dec: float):
        if self.api.client.detected:
            self.api.client.get('move_to_coordinates', {'ra': ra, 'dec': dec})

    def expose(self, seconds):
        if self.api.client.detected:
            self.api.client.get('expose', {'seconds': seconds})

    @property
    def name(self) -> str:
        return self.network.hostname

    def abort(self):
        if self.api.client.detected:
            self.api.client.get('abort')


class Controller:
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
        return socket.gethostname()

    def __init__(self):

        self.tasks: dict = {}
        for key in targets.folders.keys():
            try:
                self.tasks[key] = targets.load_folder(key)
                logger.info(f"loaded {len(self.tasks[key])} targets from '{targets.folders[key]}'")
            except Exception as e:
                logger.error(f"failed to load {targets.folders[key]}: {e}")

        self.tasks_lock = Lock()
        # with self.tasks_lock:
        #     self.targets.sort(key=lambda p: p.merit)

        self.tasks_watcher = FsWatcher(folder=targets.folders['pending'], handlers={
            'modified': self.on_modified_task,
            'deleted': self.on_deleted_task,
            'moved': self.on_moved_task,
        })

        self._terminated = False

        self.units: List[ControlledUnit] = []
        self.spec: Spec | None = None

        def make_unit(name: str):
            interval = 25 + random.randint(0, 10)
            unit = ControlledUnit(host=name)
            self.units.append(unit)
            while not self._terminated and not (unit.api.client and unit.api.client.detected):
                time.sleep(interval)
                unit.api.client.get('status')
            logger.info(f"made a Unit connection with '{name}'")

        sites_conf = Config().get_sites()
        for site in sites_conf:
            if hasattr(site, 'local') and site.local == True:
                for unit_name in site['deployed']:
                    Thread(target=make_unit, args=[unit_name]).start()
                break

    def start_controlling(self):
        self._terminated = False
        self.tasks_watcher.run()

    def stop_controlling(self):
        self._terminated = True
        self.tasks_watcher.stop()

    def on_modified_task(self, event: FileSystemEvent):
        """
        A task was modified.  Load, verify and sort it in.
        :return:
        """

    def on_deleted_task(self, event: FileSystemEvent):
        """
        A task was deleted.  Delete it from the list
        """
        with self.tasks_lock:
            for task in self.tasks:
                if task.path == event.src_path:
                    if task.in_progress:
                        return
                    else:
                        self.tasks.remove(task)
                    return

    def on_moved_task(self, event: FileSystemEvent):
        """
        A task file was renamed
        """
        found = [task for task in self.tasks if task.path == event.src_path]
        if found:
            found[0].path = event.dest_path

    def startup(self):
        self.start_controlling()

    def shutdown(self):
        self.stop_controlling()

    def status(self) -> dict:
        not_detected = {
            'detected': False,
        }
        time_stamp(not_detected)
        spec_status = not_detected
        if self.spec and self.spec.api and self.spec.api.client and self.spec.api.client.detected:
            spec_status = {'detected': self.spec.api.client.detected}
        return {
            'spec': spec_status,
            'units': [{unit.name: unit.status if unit.api.client.detected else not_detected} for unit in self.units]
        }

    def unit_minimal_status(self, unit_name: str) -> dict | None:
        """
        Returns a minimal status for the unit, including only 'powered' and 'detected'
        Anyone wanting the whole status should ask the unit directly
        :param unit_name:
        :return:
        """
        for unit in self.units:
            if unit.name == unit_name:
                detected = unit.api.client.detected
                ret = {
                    'type': 'short',
                    'powered': unit.powered,
                    'detected': detected
                }
                return ret
        return None

    def power_switch_status(self, unit_name) -> dict | None:
        for unit in self.units:
            if unit.name == unit_name:
                return unit.switch.status()
        return None

    def set_outlet(self, unit_name, outlet: int | str, state: Literal['on', 'off', 'toggle']):
        if isinstance(outlet, str):
            outlet = int(outlet)
        logger.info(f"set_outlet: {unit_name=}, {outlet=}, {state=}")
        for unit in self.units:
            if unit.name == unit_name:
                if state == 'on':
                    unit.switch.on(outlet=outlet-1)
                elif state == 'off':
                    unit.switch.off(outlet=outlet-1)
                elif state == 'toggle':
                    unit.switch.toggle(outlet=outlet-1)
                return


    def get_tasks(self,
                  kind: Literal['pending', 'in-progress', 'completed', 'all'] = 'pending',
                  ulid: Optional[str] = None,
                  name: Optional[str] = None) -> Union[Dict, List]:
        """
        Get either a specific (by name or by ulid) pending task, or all of them

        :param kind: what kind of targets
        :param ulid: optional ULID
        :param name: optional name
        :return: one or more targets, of the specified kind
        """
        if kind == 'all':
            return {
                'maintainer': self.name,
                'pending': [t.to_dict() for t in self.tasks['pending']],
                'inprogress': [t.to_dict() for t in self.tasks['in-progress']],
                'completed': [t.to_dict() for t in self.tasks['completed']],
            }

        if ulid:
            return [t.to_dict() for t in self.tasks[kind] if t.ulid == ulid]
        elif name:
            return [t.to_dict() for t in self.tasks[kind] if t.name == name]

        return [t.to_dict() for t in self.tasks[kind]]

controller: Controller = Controller()


def startup():
    global controller

    if controller is None:
        controller = Controller()


def shutdown():
    global controller

    controller.shutdown()


def config_get_sites_conf() -> dict:
    return Config().get_sites()


def config_get_users() -> List[str]:
    return Config().get_users()


def config_get_user(user_name: str) -> dict:
    return Config().get_user(user_name)


def config_get_unit(unit_name: str):
    ret = Config().get_unit(unit_name)
    return ret


def config_set_unit(unit_name: str, unit_conf: dict):
    Config().set_unit(unit_name, unit_conf)

def config_get_thar_filters():
    return Config().get_specs()['wheels']['ThAr']['filters']

base_path = BASE_CONTROL_PATH
tag = 'Control'
router = APIRouter()

router.add_api_route(base_path + '/status', tags=[tag], endpoint=controller.status)
router.add_api_route(base_path + '/startup', tags=[tag], endpoint=controller.startup)
router.add_api_route(base_path + '/shutdown', tags=[tag], endpoint=controller.shutdown)

tag = 'Config'
router.add_api_route(base_path + '/config/sites_conf', tags=[tag], endpoint=config_get_sites_conf)
router.add_api_route(base_path + '/config/users', tags=[tag], endpoint=config_get_users)
router.add_api_route(base_path + '/config/user', tags=[tag], endpoint=config_get_user)
router.add_api_route(base_path + '/config/get_unit/{unit_name}', tags=[tag], endpoint=config_get_unit)
router.add_api_route(base_path + '/config/set_unit/{unit_name}', tags=[tag], endpoint=config_set_unit)
router.add_api_route(base_path + '/config/get_thar_filters', tags=[tag], endpoint=config_get_thar_filters)

router.add_api_route(base_path + '/unit/{unit_name}/minimal_status', tags=[tag], endpoint=controller.unit_minimal_status)
router.add_api_route(base_path + '/unit/{unit_name}/power_switch/status', tags=[tag], endpoint=controller.power_switch_status)
router.add_api_route(base_path + '/unit/{unit_name}/power_switch/outlet', tags=[tag], endpoint=controller.set_outlet)
# router.add_api_route(base_path + '/{unit}/expose', tags=[tag], endpoint=scheduler.units.{unit}.expose)
# router.add_api_route(base_path + '/{unit}/move_to_coordinates', tags=[tag], endpoint=scheduler.units.{unit}.move_to_coordinates)

router.add_api_route(base_path + '/targets', tags=[tag], endpoint=controller.get_tasks)


if __name__ == '__main__':
    controller.startup()
