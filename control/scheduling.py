import logging
from datetime import datetime
from typing import Self, cast

from common.config import Config
from common.mast_logging import init_log
from common.models.batches import Batch
from common.models.constraints import TimeWindow
from common.utils import function_name

from .controller import Controller
from .planning import Plan, Planner, PlanState

logger = logging.getLogger("scheduling")
init_log(logger)


class Scheduler:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, controller: Controller) -> None:
        if self._initialized:
            return
        self.controller = controller
        self.sites_conf = Config().get_sites()
        self._initialized = True

    def make_immediate_batch(self) -> list[Batch]:
        """
        Makes a single next-in-line batch of plans, starting either immediately or today.
        """

        preferred_site = self.controller.preferred_site
        if preferred_site is None:
            logger.error(
                f"{function_name()}: cannot determine preferred site for controller '{self.controller.hostname}'"
            )
            return []

        site = [s for s in self.sites_conf if s.name == preferred_site][0]
        if not site:
            logger.error(
                f"{function_name()}: preferred site '{preferred_site}' not found in configuration"
            )
            return []
        nearest_observing_window: TimeWindow | None = site.observing_window()
        if not nearest_observing_window:
            logger.error(
                f"{function_name()}: could not determine nearest observing window for site '{preferred_site}'"
            )
            return []

        now = datetime.now()
        assert nearest_observing_window.start is not None
        if now > nearest_observing_window.start:
            logger.info(
                f"{function_name()}: it's already past dusk at site '{preferred_site}', starting batch immediately"
            )
            start_time = now
        else:
            logger.info(
                f"{function_name()}: it's not yet dusk at site '{preferred_site}', starting batch at dusk ({nearest_observing_window.start})"
            )
            start_time = nearest_observing_window.start

        end_time = nearest_observing_window.end
        time_window = TimeWindow(start=start_time, end=end_time)

        ret: list[Batch] = []

        response = Planner(self.controller).get_plans(state=PlanState.pending)
        pending_plans = cast(list[Plan], response.value)

        # look only at approved plans, the rest are not ready for scheduling
        candidates = [plan for plan in pending_plans if plan.approved]

        candidates = self.filter_by_time_window(candidates, time_window)
        # filter_by_visibility_and_airmass_from_site is not yet implemented, so we skip it for now
        # candidates = self.filter_by_visibility_and_airmass_from_site(candidates, day=start_time

        return ret

    def make_predicted_batches(self) -> list[Batch]:
        return []

    def filter_by_time_window(self, evaluated_time_window: TimeWindow) -> Self:
        """
        Eliminate plans that cannot be executed within their specified time windows.
        :param plans: the list of plans to filter
        :param evaluated_time_window: the time window for which to filter the plans
        :return: the filtered list of plans
        """

        if evaluated_time_window.start is None or evaluated_time_window.end is None:
            logger.warning(
                f"{function_name()}: evaluated_time_window is missing start or end, skipping time window filtering"
            )
            return self

        ret = []
        for plan in self.plans:
            if not plan.constraints or not plan.constraints.time_window:
                ret.append(plan)
                continue

            plan_time_window = plan.constraints.time_window
            if not plan_time_window.start:
                ret.append(plan)
                continue

            # TODO: handle the case where start and end are datetimes, and the plan has a specified time of day for execution
            # check plan's time window against the given time window
            if (
                plan_time_window.start >= evaluated_time_window.start
                and plan_time_window.start < evaluated_time_window.end
            ):
                ret.append(plan)

        return self

    def filter_by_visibility_and_airmass_from_site(self) -> Self:
        """
        Eliminate plans that cannot be executed on the specified day due to visibility constraints.
        :param plans: the list of plans to filter
        :param evaluated_time_window: the time window for which to filter the plans
        :return: the filtered list of plans

        Notes:
        - This is a rough visibility estimation from the site's location.  It does not consider the
            actual horizon as seen from a unit (building, walls raised/lowered, etc)
        - The plans are known to have been checked for schedulability on the specified day
        """

        site = self.controller.preferred_site
        # TODO: per/target check for visibility and airmass constraints within the site's observing window
        ret: list[Plan] = []
        for plan in [p for p in self.plans if p.target is not None]:
            target = plan.target
            if target.ra_hours is None or target.dec_degrees is None:
                logger.warning(
                    f"{function_name()}: Plan {plan.ulid}: bad target ({plan.target}), skipping visibility check"
                )
                continue

            ret.append(plan)
        return self
