from pydantic import BaseModel, field_validator, model_validator
import logging
from common.config import Config
from common.utils import init_log
from common.parsers import parse_units
from typing import Literal, List, Optional, Union

from astropy.coordinates import Longitude, Latitude
from astropy import units as u

logger = logging.getLogger('tasks')
init_log(logger)
class CalibrationLampModel(BaseModel):
    on: bool
    filter: str

    @field_validator('filter')
    def validate_filter(cls, filter_name: str) -> str | None:
        thar_filters = Config().get_thar_filters()

        if filter_name not in thar_filters.values():
            raise ValueError \
                (f"Invalid filter '{filter_name}', currently mounted ThAr filters are: {[f"{k}:{v}" for k, v in thar_filters.items() if v]}")
        for k, v in thar_filters.items():
            if filter_name == v:
                return f"{k}:{v}"
        return None

class SpectrographModel(BaseModel):
    exposure: float
    lamp: CalibrationLampModel
    instrument: Literal['deep', 'deepspec', 'high', 'highspec']
    binning_x: Literal[1, 2, 4] = 1
    binning_y: Literal[1, 2, 4] = 1

    @field_validator('instrument')
    def validate_instrument(cls, instrument: str) -> str:
        return 'deepspec' if instrument in ['deep', 'deepspec'] else 'highspec'

class DeepSpecModel(SpectrographModel):
    binning: Literal[1, 2, 4] = 1

class HighSpecModel(SpectrographModel):
    gratings: Union[str, List[str]]

    @field_validator('gratings')
    def validate_gratings(cls, specs: Union[str, List[str]]):
        valid_gratings_dict = Config().get_gratings()
        valid_grating_names = list(valid_gratings_dict.keys())
        ret: List[str] = []

        if isinstance(specs, str):
            specs = [specs]
        for spec in specs:
            spec = spec.lower()
            if spec not in valid_grating_names:
                raise ValueError(f"Invalid grating specifier '{spec}', must be one of {valid_grating_names}.")
            else:
                ret.append(spec)
        return ret

class TargetModel(BaseModel):
    ra: Union[float, str]
    dec: Union[float, str]
    units: Union[str, List[str]]
    allocated_units: Union[str, List[str]]
    quorum: Optional[int] = 1
    exposure: Optional[float] = 5 * 60
    priority: Optional[Literal['lowest', 'low', 'normal', 'high', 'highest', 'too']] = 'normal'
    magnitude: Optional[float]
    magnitude_band: Optional[str]

    @model_validator(mode='after')
    def validate_target(cls, values):
        quorum = values.quorum
        units = values.units
        if len(units) < quorum:
            raise ValueError(f"Expected {quorum=}, got only {len(units)} ({units})")
        return values

    @field_validator('units', mode='before')
    def validate_input_units(cls, specifiers: Union[str, List[str]]) -> List[str]:
        success, value = parse_units(specifiers if isinstance(specifiers, list) else [specifiers])
        if success:
            return value
        else:
            raise ValueError(f"Invalid units specifier '{specifiers}', errors: {value}")

    @field_validator('ra', mode='before')
    def validate_ra(cls, value):
        ra = Longitude(value, unit=u.hourangle)
        return ra.value

    @field_validator('dec', mode='before')
    def validate_dec(cls, value):
        dec = Latitude(value, unit=u.deg)
        return dec.value

class SettingsModel(BaseModel):
    ulid: Union[str, None]
    owner: Union[str, None]
    merit: Union[int, None]
    state: Literal['new', 'in-progress', 'postponed', 'canceled', 'completed']
    ready_to_guide_timeout: int

    @field_validator('owner')
    def validate_owner(cls, user: str) -> str:
        valid_users = Config().get_users()
        if user not in valid_users:
            raise ValueError(f"Invalid user '{user}'")
        user = Config().get_user(user)
        if not 'canOwnTasks' in user['capabilities']:
            raise ValueError(f"User '{user['name']}' cannot own tasks")
        return user['name']

class MoonConstraintModel(BaseModel):
    max_phase: float
    min_distance: float

class AirmassConstraintModel(BaseModel):
    max: float

class SeeingConstraintModel(BaseModel):
    max: float

class ConstraintsModel(BaseModel):
    moon: Optional[MoonConstraintModel]
    airmass: Optional[AirmassConstraintModel]
    seeing: Optional[SeeingConstraintModel]

class TaskModel(BaseModel):
    settings: SettingsModel
    target: TargetModel
    spec: Union[DeepSpecModel, HighSpecModel]
    constraints: Optional[ConstraintsModel]

    # def __post_init__(self, file_name: Optional[str] = None):
    #     self.file_name = file_name
