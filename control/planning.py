import logging
import shutil
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Callable

from pydantic import BaseModel

from common.canonical import CanonicalResponse
from common.const import Const
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
        self.folder_path = Path(PathMaker.make_plans_folder()) / self.folder_name
        try:
            self.folder_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"could not create '{self.folder_path}' (error: {e})")
            raise

        self.lock = Lock()

        self.refresh()

    def refresh(self):
        """
        Reload all plans from the folder
        :return:
        """
        with self.lock:
            paths = Path(self.folder_path).glob(Const.PlanFileNamePattern)
            self.plans = []
            for path in paths:
                try:
                    plan = Plan.from_toml_file(str(path))
                    plan.full_path = path
                except Exception as e:
                    logger.error(f"could not load plan from {path}, error: {e}")
                    continue
                self.plans.append(plan)
            logger.info(f"loaded {len(self.plans)} plans from '{self.folder_name}'")

    def start_watching(self):
        # self.thread.start()
        pass

    def stop_watching(self):
        # self.watcher.stop()
        # self.thread.join()
        pass


class PlanState(StrEnum):
    pending = "pending"
    in_progress = "in-progress"
    failed = "failed"
    completed = "completed"
    expired = "expired"
    postponed = "postponed"
    deleted = "deleted"
    canceled = "canceled"


class PlansResponse(BaseModel):
    maintaining_controller: str
    in_progress: list[Plan]
    completed: list[Plan]
    pending: list[Plan]
    failed: list[Plan]
    expired: list[Plan]
    postponed: list[Plan]
    deleted: list[Plan]
    canceled: list[Plan]


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
        self.deleted_folder = _make_folder("deleted")
        self.canceled_folder = _make_folder("canceled")

        self.plan_folders = [
            self.pending_folder,
            self.expired_folder,
            self.failed_folder,
            self.in_progress_folder,
            self.completed_folder,
            self.postponed_folder,
            self.deleted_folder,
            self.canceled_folder,
        ]
        # ensure non-None typing for downstream code
        self.plan_in_progress = None

        self.transitions: dict[PlanState, list[tuple[PlanState, Callable]]] = {
            PlanState.pending: [
                (PlanState.in_progress, self.do_execute_plan),
                (PlanState.postponed, self.do_postpone_plan),
                (PlanState.deleted, self.do_delete_plan),
            ],
            PlanState.in_progress: [
                (PlanState.canceled, self.do_cancel_plan),
            ],
            PlanState.postponed: [
                (PlanState.pending, self.do_revive_plan),
            ],
            PlanState.deleted: [
                (PlanState.pending, self.do_revive_plan),
            ],
            PlanState.expired: [
                (PlanState.pending, self.do_revive_plan),
            ],
            PlanState.failed: [
                (PlanState.pending, self.do_revive_plan),
            ],
            PlanState.completed: [
                (PlanState.pending, self.do_revive_plan),
            ],
            PlanState.canceled: [
                (PlanState.pending, self.do_revive_plan),
            ],
        }

        self.lock = Lock()

    def transition_to_postponed(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.postponed)

    def transition_to_pending(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.pending)

    def transition_to_deleted(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.deleted)

    def transition_to_in_progress(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.in_progress)

    def transition_to_canceled(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.canceled)

    def transition_to_completed(self, ulid: str) -> CanonicalResponse:
        return self.transition_plan(ulid, PlanState.completed)

    def transition_plan(self, ulid: str, target_state: PlanState) -> CanonicalResponse:
        result = self.locate_plan(ulid)
        if result is None:
            return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])

        current_state, plan = result
        transitions = self.transitions.get(current_state, [])
        for state, action in transitions:
            if state == target_state:
                try:
                    action(plan)
                except Exception as e:
                    logger.error(
                        f"error transitioning plan {ulid} to state {target_state}: {e}"
                    )
                    return CanonicalResponse(
                        errors=[
                            f"error transitioning plan {ulid} to state {target_state}"
                        ]
                    )
                finally:
                    self.refresh()
        else:
            logger.warning(
                f"invalid transition from {current_state} to {target_state} for plan {ulid}"
            )

        return CanonicalResponse(
            errors=[
                f"invalid transition from {current_state} to {target_state} for plan {ulid}"
            ]
        )

    def refresh(self):
        with self.lock:
            for folder in self.plan_folders:
                folder.refresh()

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
            PlanState.deleted: self.deleted_folder,
            PlanState.canceled: self.canceled_folder,
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
                deleted=self.deleted_folder.plans,
                canceled=self.canceled_folder.plans,
            )
        )

    def locate_plan(self, ulid: str) -> tuple[PlanState, Plan] | None:
        with self.lock:
            for folder in self.plan_folders:
                matching_plans = [p for p in folder.plans if p.ulid == ulid]
                if len(matching_plans) > 0:
                    return (PlanState(folder.folder_name), matching_plans[0])
        return None

    def do_cancel_plan(self, plan: Plan):
        assert plan is not None and plan.full_path is not None
        new_path = self.canceled_folder.folder_path / plan.full_path.name

        logger.debug(
            f"canceling plan {plan.ulid}, moving {str(plan.full_path)} to {str(new_path)}"
        )
        shutil.move(str(plan.full_path), str(new_path))

    def do_postpone_plan(self, plan: Plan):
        assert plan is not None and plan.full_path is not None
        new_path = self.postponed_folder.folder_path / plan.full_path.name

        logger.debug(
            f"postponing plan {plan.ulid}, moving {str(plan.full_path)} to {str(new_path)}"
        )
        shutil.move(str(plan.full_path), str(new_path))

    def do_revive_plan(self, plan: Plan):
        assert plan is not None and plan.full_path is not None
        new_path = self.pending_folder.folder_path / plan.full_path.name

        logger.debug(
            f"reviving plan {plan.ulid}, moving {str(plan.full_path)} to {str(new_path)}"
        )
        shutil.move(str(plan.full_path), str(new_path))

    def do_delete_plan(self, plan: Plan):
        assert plan is not None and plan.full_path is not None
        new_path = self.deleted_folder.folder_path / plan.full_path.name

        logger.debug(
            f"deleting plan {plan.ulid}, moving {str(plan.full_path)} to {str(new_path)}"
        )
        shutil.move(str(plan.full_path), str(new_path))

    def do_execute_plan(self, plan: Plan):
        assert plan is not None and plan.full_path is not None
        new_path = self.in_progress_folder.folder_path / plan.full_path.name

        logger.debug(
            f"executing plan {plan.ulid}, moving {str(plan.full_path)} to {str(new_path)}"
        )
        shutil.move(str(plan.full_path), str(new_path))

        # TBD: here we would trigger the actual execution of the plan, for now we just move the file and update the lists

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
            endpoint=self.transition_to_postponed,
            methods=["POST"],
        )
        router.add_api_route(
            plans_base + "/revive",
            tags=[tag],
            endpoint=self.transition_to_pending,
            methods=["POST"],
        )
        router.add_api_route(
            plans_base + "/delete",
            tags=[tag],
            endpoint=self.transition_to_deleted,
            methods=["DELETE"],
        )
        router.add_api_route(
            plans_base + "/execute",
            tags=[tag],
            endpoint=self.transition_to_in_progress,
            methods=["POST"],
        )
        router.add_api_route(
            plans_base + "/cancel",
            tags=[tag],
            endpoint=self.transition_to_canceled,
            methods=["POST"],
        )
