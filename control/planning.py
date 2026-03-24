import logging
import shutil
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Callable

import tomlkit
import ulid
from pydantic import BaseModel, Field, ValidationError

from common.canonical import CanonicalResponse, CanonicalResponse_Ok
from common.config import Config
from common.const import Const
from common.mast_logging import init_log
from common.models.events import EventModel
from common.models.constraints import ConstraintsModel, RepeatsModel
from common.models.plans import Plan
from common.models.spectrographs import SpectrographModel
from common.paths import PathMaker
from common.utils import function_name

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
                    raise ValidationError(
                        f"could not load plan from {path}, error: {e}"
                    ) from e
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
    submitted = "submitted"
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
    submitted: list[Plan] = []
    in_progress: list[Plan] = []
    completed: list[Plan] = []
    pending: list[Plan] = []
    failed: list[Plan] = []
    expired: list[Plan] = []
    postponed: list[Plan] = []
    deleted: list[Plan] = []
    canceled: list[Plan] = []


class NewPlanTemplate(BaseModel):
    ulid: str
    owner: str | None = None
    merit: int = 1
    timeout_to_guiding: float = 600
    autofocus: bool = False
    too: bool = False
    approved: bool = False
    production: bool = True
    quorum: int = 1
    requested_units: list[str] = []
    target: dict = Field(default_factory=lambda: {
        "ra_hours": None,
        "dec_degrees": None,
        "requested_exposure_duration": None,
        "requested_number_of_exposures": 1,
        "max_exposure_duration": None,
        "repeats": RepeatsModel().model_dump(),
    })
    spec_assignment: dict = Field(default_factory=lambda: SpectrographModel().model_dump())
    constraints: dict = Field(default_factory=lambda: ConstraintsModel().model_dump())
    filter_options: list[str] = []


class Planner:
    _instance = None
    _initialized = False

    def __new__(cls, controller):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, controller):
        self.controller = controller

        def _make_folder(name: str) -> PlansFolder:
            return PlansFolder(folder_name=name)

        self.submitted_folder = _make_folder("submitted")
        self.pending_folder = _make_folder("pending")
        self.expired_folder = _make_folder("expired")
        self.failed_folder = _make_folder("failed")
        self.completed_folder = _make_folder("completed")
        self.postponed_folder = _make_folder("postponed")
        self.in_progress_folder = _make_folder("in-progress")
        self.deleted_folder = _make_folder("deleted")
        self.canceled_folder = _make_folder("canceled")

        self.plan_folders = [
            self.submitted_folder,
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
            PlanState.submitted: [
                (PlanState.pending, self.do_revive_plan),
            ],
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
            PlanState.submitted: self.submitted_folder,
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
            for plan_folder in self.plan_folders:
                matching_plans = [p for p in plan_folder.plans if p.ulid == ulid]
                if len(matching_plans) == 1:
                    return CanonicalResponse(value=matching_plans[0])
            return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])

        if state is not None:
            plan_folder = folder_map.get(state)
            if plan_folder is None:
                return CanonicalResponse(errors=[f"unknown PlanState '{state}'"])
            return CanonicalResponse(value=plan_folder.plans)

        try:
            return CanonicalResponse(
                value=PlansResponse(
                    maintaining_controller=self.controller.name,
                    submitted=self.submitted_folder.plans,
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
        except Exception as e:
            return CanonicalResponse(
                errors=[f"{function_name()}: error getting plans: {e}"]
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

    def get_new_plan(self) -> CanonicalResponse:
        try:
            return CanonicalResponse(value=NewPlanTemplate(
                ulid=str(ulid.ULID()),
                filter_options=Config().get_thar_filters(),
            ))
        except Exception as e:
            return CanonicalResponse(errors=[f"{function_name()}: {e}"])

    def submit_plan(self, plan: Plan) -> CanonicalResponse:
        try:
            if plan.spec_assignment is None or plan.spec_assignment.instrument is None:
                return CanonicalResponse(
                    errors=["submit_plan: spec_assignment.instrument must be specified"]
                )
            if plan.ulid is None:
                return CanonicalResponse(errors=["submit_plan: plan must have a ulid"])

            file_path = self.submitted_folder.folder_path / f"PLAN_{plan.ulid}.toml"

            plan_dict = plan.model_dump(
                mode="json",
                exclude={"full_path", "spec_api", "commited_unit_apis"},
                exclude_none=True,
            )
            submitted_event = EventModel(what="submitted").model_dump(
                mode="json", exclude_none=True
            )
            plan_dict["events"] = [submitted_event]

            with open(file_path, "w") as f:
                f.write(tomlkit.dumps(plan_dict))

            self.submitted_folder.refresh()
            return CanonicalResponse_Ok
        except Exception as e:
            return CanonicalResponse(errors=[f"{function_name()}: {e}"])

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
            plans_base + "/new", tags=[tag], endpoint=self.get_new_plan, methods=["GET"]
        )
        router.add_api_route(
            plans_base + "/submit",
            tags=[tag],
            endpoint=self.submit_plan,
            methods=["POST"],
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


if __name__ == "__main__":
    response = Planner(controller=None).get_plans(state=PlanState.pending)
    print(response.model_dump_json(indent=2))
