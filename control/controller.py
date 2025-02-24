import os
import time
import socket

from fastapi.exceptions import ValidationException
from common.utils import BASE_CONTROL_PATH, Component, time_stamp, CanonicalResponse, CanonicalResponse_Ok
from common.utils import function_name
from common.mast_logging import init_log
from common.fswatcher import FsWatcher
from common.config import Config
from common.api import UnitApi, SpecApi
from common.networking import NetworkedDevice
from common.dlipowerswitch import SwitchedOutlet, OutletDomain
import logging
from typing import List, Optional, Literal, Dict
from watchdog.events import FileSystemEvent
from threading import Lock, Thread
from fastapi import APIRouter
import random
from common.tasks.models import AssignedTaskModel, TaskNotification
from common.paths import PathMaker
import asyncio
from pathlib import Path

logger = logging.getLogger('controller')
init_log(logger)


class TasksContainer:
    """
    Manages task files in a given folder.
    - if the folder does not exist, it is created
    - all the files named 'TSK_...toml' in the folder are loaded into the provided list of tasks
    - watchers are set up to handle:
      - file creation: the task is loaded and added to the list
      - file deletion: the folder is scanned to figure out which ULID was deleted.  the respective task gets deleted from the list
      - file modification: we load the task and update the respective element in the list (by ULID)
    """

    TASK_PATH_PATTERN = "*/TSK_*.toml"

    def __init__(self, folder_name: str):

        self.folder_name = folder_name
        self.path = Path(PathMaker.make_tasks_folder()) / self.folder_name
        try:
            self.path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"could not create '{self.path}' (error: {e})")
            raise

        self.lock = Lock()

        directory = Path(self.path)
        with self.lock:
            task_files = directory.glob('TSK_*.toml')
            self.tasks = []
            for task_file in task_files:
                try:
                    task = AssignedTaskModel.from_toml_file(task_file)
                except Exception as e:
                    logger.error(f"could not load task from {task_file}, error: {e}")
                    continue
                self.tasks.append(task)
            if len(self.tasks) > 0:
                logger.info(f"loaded {len(self.tasks)} tasks from '{self.folder_name}'")

        self.watcher: FsWatcher = FsWatcher(folder=str(self.path), handlers= {
            'created': self.on_created,
            'modified': self.on_modified,
            'deleted': self.on_deleted,
        })
        Thread(target=self.watcher.run, name=f"task-{self.folder_name}-thread").start()

    def on_created(self, event: FileSystemEvent):
        """
        A new file just materialized, add the task to the list
        :param event:
        :return:
        """
        path = Path(event.src_path)
        if not path.match(TasksContainer.TASK_PATH_PATTERN):
            return
        try:
            new_task = AssignedTaskModel.from_toml_file(str(path))
        except Exception as e:
            logger.error(f"could not load an AssignedTaskModel from '{str(path)}', error: {e}")
            return

        # check for duplicates
        with self.lock:
            for task in self.tasks:
                if task.task.ulid == new_task.task.ulid:
                    logger.error(f"duplicate task ulid ({task.task.ulid}) in {new_task.task.file} and {task.task.file}")
                    return
            # add it
            self.tasks.append(new_task)
            logger.info(f"task '{new_task.task.ulid}' created in '{str(path)}'")

    def on_modified(self, event: FileSystemEvent):
        """
        A task file was modified, update the list member with the same ulid with the new data
        :param event:
        :return:
        """
        path = Path(event.src_path)
        if not path.match(TasksContainer.TASK_PATH_PATTERN):
            return

        try:
            modified_task = AssignedTaskModel.from_toml_file(str(path))
        except Exception as e:
            logger.error(f"could not load updated task from '{str(path)}', error: {e}")
            return

        with self.lock:
            for task in self.tasks:
                if task.task.ulid == modified_task.task.ulid:
                    self.tasks.remove(task)
                    self.tasks.append(modified_task)
                    logger.info(f"task '{modified_task.task.ulid}' modified in '{str(path)}'")

    def on_deleted(self, event: FileSystemEvent):
        """
        A task file was deleted, scan the folder and find out which ULID was deleted
        :param event:
        :return:
        """
        path = Path(event.src_path)
        if not path.match(TasksContainer.TASK_PATH_PATTERN):
            return

        with self.lock:
            previous_ulids: List[str] = [t.task.ulid for t in self.tasks]
            current_task_files = Path(self.path).glob('TSK_*.toml')
            current_tasks: List[AssignedTaskModel] = []
            for file in current_task_files:
                try:
                    task = AssignedTaskModel.from_toml_file(file)
                    current_tasks.append(task)
                except ValidationException as e:
                    logger.error(f"could not load an AssignedTaskModel from '{file}' (error: {e})")
                    continue

            current_ulids = [t.task.ulid for t in current_tasks]
            deleted_ulids = [u for u in previous_ulids if u not in current_ulids]
            for deleted_ulid in deleted_ulids:  # more than one may have been deleted
                deleted_task = [t for t in self.tasks if t.task.ulid == deleted_ulid]
                if len(deleted_task) == 1:
                    self.tasks.remove(deleted_task[0])
                    logger.info(f"task '{deleted_ulid}' was deleted from '{self.path}'")

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
        self.api = SpecApi()
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


class ControlledUnit(Component, SwitchedOutlet, NetworkedDevice):
    def __init__(self, host: str):
        Component.__init__(self)
        NetworkedDevice.__init__(self, conf={'network': {'host': host}})
        SwitchedOutlet.__init__(self, OutletDomain.Unit, outlet_name='Computer')
        if not self.is_on():
            self.power_on()
            # TODO: wait till the unit computer boots
        self.powered = self.is_on()
        self._was_shut_down = False
        # ApiUnit calls the remote 'status'
        logger.info(f"trying to make an ApiUnit(host='{self.network.hostname}') connection ...")
        self.api = UnitApi(self.network.hostname)

    @property
    async def detected(self) -> bool:
        stat = await self.api.get('status') if self.api else None
        return stat.detected if stat else False

    @property
    async def connected(self) -> bool:
        stat = await self.api.get('status') if self.api else None
        return stat.connected if stat else False

    @property
    async def operational(self) -> bool:
        stat = await self.api.get('status') if self.api else None
        return stat.operational if stat else False

    @property
    async def why_not_operational(self) -> List[str]:
        ret: List[str] = []
        if not self.detected:
            ret.append('not detected')
        elif not self.connected:
            ret.append('not connected')
        elif not self.operational:
            if not self.detected:
                ret.append(f"api client not detected")
            else:
                why: List[str] | None  = await self.api.get('why_not_operational')
                if why:
                    for reason in why:
                        ret.append(reason)
        return ret

    @property
    def was_shut_down(self) -> bool:
        return self._was_shut_down

    async def startup(self):
        self._was_shut_down = False
        return await self.api.get('startup')

    async def shutdown(self):
        self._was_shut_down = True
        return await self.api.get('shutdown')

    @property
    async def status(self) -> dict:
        if not self.detected:
            return {
                'powered': self.powered,
                'detected': False,
            }
        return await self.api.get('status')

    async def move_to_coordinates(self, ra: float, dec: float):
        return await self.api.get('move_to_coordinates', {'ra': ra, 'dec': dec})

    async def expose(self, seconds):
        return await self.api.get('expose', {'seconds': seconds})

    @property
    def name(self) -> str:
        return self.network.hostname

    async def abort(self):
        return await self.api.get('abort')

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

        # assigned_tasks_top = os.path.join(PathMaker().make_tasks_folder(), 'assigned')
        # self.assigned_tasks = common.tasks.load_assigned()
        # logger.info(f"loaded {len(self.assigned_tasks)} assigned tasks from '{assigned_tasks_top}'")

        self.pending_tasks_container = TasksContainer(folder_name='pending')
        self.assigned_tasks_container = TasksContainer(folder_name='assigned')
        self.failed_tasks_container = TasksContainer(folder_name='failed')
        self.completed_task_container = TasksContainer(folder_name='completed')
        self.in_progress_task_container = TasksContainer(folder_name='in-progress')
        self.task_containers = [
            self.pending_tasks_container,
            self.assigned_tasks_container,
            self.failed_tasks_container,
            self.in_progress_task_container,
            ]

        self._terminated = False

        self.units: List[ControlledUnit] = []
        self.spec: Spec | None = None

        def make_unit(name: str):
            interval = 25 + random.randint(0, 10)
            unit = ControlledUnit(host=name)
            self.units.append(unit)
            while not self._terminated and not unit.api.detected:
                time.sleep(interval)
                unit.api.get('status')
            logger.info(f"made a Unit connection with '{name}'")

        sites_conf = Config().get_sites()
        for site in sites_conf:
            if hasattr(site, 'local') and site.local == True:
                for unit_name in site['deployed']:
                    Thread(target=make_unit, args=[unit_name]).start()
                break

    def start_controlling(self):
        self._terminated = False
        for container in self.task_containers:
            Thread(name=f"tasks-watcher-{container.folder_name}",target=container.watcher.run).start()

    def stop_controlling(self):
        self._terminated = True
        for container in self.task_containers:
            container.watcher.stop()

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
            'units': [{unit.name: unit.status if unit.api.detected else not_detected} for unit in self.units]
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
                detected = unit.api.detected
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
                return unit.power_switch.status()
        return None

    def set_outlet(self, unit_name, outlet: int | str, state: Literal['on', 'off', 'toggle']):
        if isinstance(outlet, str):
            outlet = int(outlet)
        logger.info(f"set_outlet: {unit_name=}, {outlet=}, {state=}")
        for unit in self.units:
            if unit.name == unit_name:
                if state == 'on':
                    unit.power_switch.on(outlet=outlet-1)
                elif state == 'off':
                    unit.power_switch.off(outlet=outlet-1)
                elif state == 'toggle':
                    unit.power_switch.toggle(outlet=outlet-1)
                return


    def get_assigned_tasks(self,
                  ulid: Optional[str] = None,
                  name: Optional[str] = None) -> List[Dict]:
        """
        Get either a specific (by name or by ulid) pending task, or all of them

        :param ulid: optional ULID
        :param name: optional name
        :return: one or more targets, of the specified kind
        """
        if ulid:
            return [t.model_dump() for t in self.assigned_tasks_container.tasks if t.task.ulid == ulid]
        elif name:
            return [t.model_dump() for t in self.assigned_tasks_container.tasks if t.task.name == name]
        else:
            return [t.model_dump() for t in self.assigned_tasks_container.tasks]

    async def execute_assigned_task(self, ulid: str) -> CanonicalResponse:
        matching_tasks = [t for t in self.assigned_tasks_container.tasks if t.task.ulid == ulid]
        if len(matching_tasks) == 0:
            return CanonicalResponse(errors=[f"no matching task for {ulid=}"])
        task = matching_tasks[0]
        task.task.run_folder = PathMaker.make_run_folder()
        os.makedirs(task.task.run_folder, exist_ok=True)
        os.link(task.task.file, os.path.join(task.task.run_folder, 'task'))
        asyncio.create_task(task.execute())
        return CanonicalResponse_Ok

    async def task_product_notification(self, product: TaskNotification):
        """
        Receives locations of products related to a running task:
        - from units: type: 'autofocus' or 'acquisition'
        - from spec: type: 'spec', folder containing the spec's acquisition
        :param product:
        :return:
        """
        op = function_name()

        matches = [t for t in self.assigned_tasks_container.tasks if t.task.ulid == product.ulid]
        if len(matches) == 0:
            logger.error(f"{op}: could not find task '{product.ulid}' in self.assigned_tasks_container.tasks")
            return
        task = matches[0]
        src = product.path
        dst = os.path.join(task.task.run_folder, product.unit, product.type)
        try:
            os.symlink(src, dst)
            logger.info(f"{op}: created symlink '{src}' -> '{dst}'")
        except Exception as e:
            logger.error(f"{op}: failed to symlink '{src}' -> '{dst}' (error: {e})")


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

router.add_api_route(base_path + '/get_assigned_tasks', tags=[tag], endpoint=controller.get_assigned_tasks)
router.add_api_route(base_path + '/execute_assigned_task', tags=[tag], endpoint=controller.execute_assigned_task)
router.add_api_route(base_path + '/task_product_notification', tags=[tag], endpoint=controller.task_product_notification)


if __name__ == '__main__':
    controller.startup()
