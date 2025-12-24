import logging
import shutil
from enum import StrEnum
from pathlib import Path
from threading import Lock, Thread

from pydantic import BaseModel
from watchdog.events import FileSystemEvent

from common.canonical import CanonicalResponse, CanonicalResponse_Ok
from common.const import Const
from common.fswatcher import FsWatcher
from common.mast_logging import init_log
from common.models.plans import Plan
from common.paths import PathMaker

logger = logging.getLogger("planning")
init_log(logger)

PLAN_PATH_PATTERN: str = f"*/{Const.PlanFileNamePattern}"


class PlansFolder:
    """
    Manages plan files in a given folder.
    - if the folder does not exist, it is created
    - all the files named 'PLAN_...toml' in the folder are loaded into the provided list of plans
    - watchers are set up to handle:
      - file creation: the plan is loaded and added to the list
      - file deletion: the folder is scanned to figure out which ULID was deleted.  the respective plan gets deleted from the list
      - file modification: we load the plan and update the respective element in the list (by ULID)
    """

    folder_name: str

    def __init__(self, folder_name: str):
        self.folder_name = folder_name
        self.path = Path(PathMaker.make_plans_folder()) / self.folder_name
        try:
            self.path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"could not create '{self.path}' (error: {e})")
            raise

        self.lock = Lock()

        self.refresh()

        self.watcher: FsWatcher = FsWatcher(
            folder=str(self.path),
            handlers={
                "created": self.on_created,
                "modified": self.on_modified,
                "deleted": self.on_deleted,
            },
        )
        self.thread = Thread(
            target=self.watcher.run, name=f"plans-{self.folder_name}-thread"
        )

    def refresh(self):
        """
        Reload all plans from the folder
        :return:
        """
        with self.lock:
            plan_files = Path(self.path).glob(Const.PlanFileNamePattern)
            self.plans = []
            for plan_file in plan_files:
                try:
                    plan = Plan.from_toml_file(str(plan_file))
                except Exception as e:
                    logger.error(f"could not load plan from {plan_file}, error: {e}")
                    continue
                self.plans.append(plan)
            logger.info(f"loaded {len(self.plans)} plans from '{self.folder_name}'")

    def start_watching(self):
        self.thread.start()

    def stop_watching(self):
        self.watcher.stop()
        self.thread.join()

    def on_created(self, _: FileSystemEvent):
        """
        A new file just materialized, refresh the list of plans
        """
        self.refresh()

    def on_modified(self, event: FileSystemEvent):
        """
        A plan file was modified, update the list member with the same ulid with the new data
        :param event:
        :return:
        """
        self.refresh()

    def on_deleted(self, event: FileSystemEvent):
        """
        A plan file was deleted, scan the folder and find out which ULID was deleted
        :param event:
        :return:
        """
        self.refresh()


class PlanState(StrEnum):
    pending = "pending"
    in_progress = "in-progress"
    failed = "failed"
    completed = "completed"
    expired = "expired"
    postponed = "postponed"


class PlansResponse(BaseModel):
    maintaining_controller: str
    in_progress: list[Plan]
    completed: list[Plan]
    pending: list[Plan]
    failed: list[Plan]
    expired: list[Plan]
    postponed: list[Plan]


class Planner:
    def __init__(self, controller):
        self.controller = controller

        def _make_folder(name: str) -> PlansFolder:
            return PlansFolder(folder_name=name)

        self.pending_folder = _make_folder("pending")
        self.expired_folder = _make_folder("expired")
        self.failed_folder = _make_folder("failed")
        self.completed_folder = _make_folder("completed")
        self.postponed_folder = _make_folder("postponed")
        self.in_progress_folder = _make_folder("in-progress")

        self.plan_folders = [
            self.pending_folder,
            self.expired_folder,
            self.failed_folder,
            self.in_progress_folder,
            self.completed_folder,
            self.postponed_folder,
        ]
        # ensure non-None typing for downstream code
        self.plan_in_progress = None

    def get_plans(
        self, ulid: str | None = None, state: PlanState | None = None
    ) -> CanonicalResponse:
        """
        Get either specific (by ulid or stage) plan(s), or all of them

        :param ulid: optional ULID
        :param stage: optional name
        :return: either the specified or all plans
        """
        folder_map = {
            PlanState.pending: self.pending_folder,
            PlanState.in_progress: self.in_progress_folder,
            PlanState.failed: self.failed_folder,
            PlanState.completed: self.completed_folder,
            PlanState.expired: self.expired_folder,
        }

        assert self.plan_folders is not None
        if ulid is not None:
            for plan in self.plan_folders:
                matching_plans = [p for p in plan.plans if p.ulid == ulid]
                if len(matching_plans) == 1:
                    return CanonicalResponse(value=matching_plans[0])
            return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])

        if state is not None:
            plan = folder_map.get(state)
            if plan is None:
                return CanonicalResponse(errors=[f"unknown PlanState '{state}'"])
            return CanonicalResponse(value=plan.plans)

        return CanonicalResponse(
            value=PlansResponse(
                maintaining_controller=self.controller.name,
                expired=self.expired_folder.plans,
                pending=self.pending_folder.plans,
                failed=self.failed_folder.plans,
                in_progress=self.in_progress_folder.plans,
                completed=self.completed_folder.plans,
                postponed=self.postponed_folder.plans,
            )
        )

    async def postpone_plan(self, ulid: str) -> CanonicalResponse:
        matching_plans = [p for p in self.pending_folder.plans if p.ulid == ulid]
        if len(matching_plans) == 0:
            return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])

        plan = matching_plans[0]
        self.pending_folder.plans.remove(plan)
        self.postponed_folder.plans.append(plan)

        assert plan.file is not None
        logger.debug(
            f"postponing plan {ulid=}, moving {str(Path(plan.file))} to {str(self.postponed_folder.path / Path(plan.file).name)}"
        )
        shutil.move(
            str(Path(plan.file)),
            str(self.postponed_folder.path / Path(plan.file).name),
        )
        return CanonicalResponse_Ok

    async def revive_plan(self, ulid: str) -> CanonicalResponse:
        for folder in self.plan_folders:
            matching_plans = [p for p in folder.plans if p.ulid == ulid]
            if len(matching_plans) > 0:
                plan = matching_plans[0]
                folder.plans.remove(plan)
                self.pending_folder.plans.append(plan)
                assert plan.file is not None
                logger.debug(
                    f"reviving plan {ulid=}, moving {str(Path(plan.file))} to {str(self.pending_folder.path / Path(plan.file).name)}"
                )
                try:
                    shutil.move(
                        str(Path(plan.file)),
                        str(self.pending_folder.path / Path(plan.file).name),
                    )
                except Exception as e:
                    logger.error(
                        f"could not move plan file for {ulid=} to pending folder, error: {e}"
                    )
                    return CanonicalResponse(
                        errors=[
                            f"could not move plan file for {ulid=} to pending folder"
                        ]
                    )
        return CanonicalResponse_Ok

    async def delete_plan(self, ulid: str) -> CanonicalResponse:
        for folder in self.plan_folders:
            matching_plans = [p for p in folder.plans if p.ulid == ulid]
            if len(matching_plans) > 0:
                plan = matching_plans[0]
                folder.plans.remove(plan)
                self.pending_folder.plans.append(plan)
                assert plan.file is not None
                logger.debug(f"deleting plan {ulid=}, removing {str(Path(plan.file))}")
                shutil.rmtree(str(Path(plan.file)), ignore_errors=True)
                return CanonicalResponse_Ok
        return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])

    def start_planning(self):
        """
        Starts the planning process
        :return:
        """
        for folder in self.plan_folders:
            folder.start_watching()

    def stop_planning(self):
        """
        Stops the planning process
        :return:
        """
        for folder in self.plan_folders:
            folder.stop_watching()

    def add_routes(self, router):
        tag = "Planning"

        tag = "Plans"
        plans_base = Const.BASE_CONTROL_PATH + "/plans"
        router.add_api_route(
            plans_base + "/get", tags=[tag], endpoint=self.get_plans, methods=["GET"]
        )
        router.add_api_route(
            plans_base + "/postpone",
            tags=[tag],
            endpoint=self.postpone_plan,
            methods=["POST"],
        )
        router.add_api_route(
            plans_base + "/revive",
            tags=[tag],
            endpoint=self.revive_plan,
            methods=["POST"],
        )
        router.add_api_route(
            plans_base + "/delete",
            tags=[tag],
            endpoint=self.delete_plan,
            methods=["DELETE"],
        )
