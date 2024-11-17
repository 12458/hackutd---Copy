from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from uuid import uuid4

class Priority(str, Enum):
    Low = "Low"
    Medium = "Medium"
    High = "High"

class TaskBase(BaseModel):
    title: str
    dueDate: datetime
    priority: Priority
    description: str
    source: str
    completed: bool = False

class TaskCreate(TaskBase):
    pass

class Task(TaskBase):
    document_id: str = Field(default_factory=lambda: str(uuid4().hex))

    def to_dict(self):
        return {
            "document_id": self.document_id,
            "title": self.title,
            "dueDate": self.dueDate.isoformat(),
            "priority": self.priority,
            "description": self.description,
            "source": self.source
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            document_id=data.get("document_id"),
            title=data.get("title"),
            dueDate=datetime.fromisoformat(data.get("dueDate")),
            priority=data.get("priority"),
            description=data.get("description"),
            source=data.get("source")
        )

class NodeType(str, Enum):
    GET_DATA = "get_data"
    COMPARE = "compare"
    PUBLISH = "publish"
    END = "end"
    TRIGGER = "trigger"

class Node(BaseModel):
    id: str
    type: NodeType
    properties: Optional[Dict[str, Any]] = None
    next: Optional[List[str]] = None
    next_true: Optional[List[str]] = None
    next_false: Optional[List[str]] = None

class ActionFlow(BaseModel):
    description: str = ""
    enabled: bool = True
    id: str = Field(default_factory=lambda: str(uuid4().hex))
    interval: int = 3600
    last_run: str = str(datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat())
    name: str
    start_node: str
    nodes: list[Node]

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)