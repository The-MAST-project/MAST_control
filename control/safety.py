import httpx
from typing import List


class SafetyMonitor:
    """
    Monitors various safety inputs and produces:
    - is_safe: [bool]  - Is it safe to observe right now
    - why_not_safe: [List[str]] - List of reasons as to why it is not safe right now
    """

    def __init__(self):
        pass

    # def is_safe(self) -> bool:
    #     with httpx.Client() as client:


    def why_not_safe(self) -> List[str]:
        pass

