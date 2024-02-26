"""
Data structure for Jobs
"""

from typing import Any, Optional

class Job:
    """
    Job class is a data structure that holds the information of a job to be executed.
    """
    def __init__(self, id_exec: int, category: str, id_user: int, path: str):
        self.id_exec = id_exec
        self.category = category
        self.id_user = id_user
        self.path = path

        self.project_type: Any = None
        self.broken = False
        self.text_content: str = str()
        self.gpt_content: str = str()
        self.java_file: list[str] = list()
        self.abstraction_score: Optional[float] = None
        self.banned_found: Optional[list[str]] = None
