"""
Shared data models and enums for the Video Transcoding Service
"""
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any
from dataclasses import dataclass, asdict


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class AssetStatus(Enum):
    UPLOADED = "uploaded"
    INGESTED = "ingested"
    INSPECTED = "inspected"
    ENCODING = "encoding"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class InspectionData:
    """Technical metadata from inspection service as specified in document"""
    codec: str
    resolution: str
    width: int
    height: int
    bitrate: int
    duration: float
    frame_rate: float
    color_space: str
    audio_tracks: List[Dict]
    file_size: int


@dataclass
class AssetRecord:
    """Asset table structure as specified in document"""
    asset_id: str
    video_type: str  # movie, tv-episode, trailer, user-content
    source_url: str   # original location in asset_bucket
    status: AssetStatus
    temp_url: str = None  # working copy in temp_bucket (used during encoding)
    inspection_data: InspectionData = None
    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()


@dataclass
class EncodingTask:
    """Task definition as specified in document"""
    task_id: str
    dependencies: List[str]
    encoding_params: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    output_url: str = None
    retry_count: int = 0


@dataclass
class DAGDefinition:
    """DAG structure as specified in document"""
    dag_id: str
    asset_uuid: str
    tasks: List[EncodingTask]
    status: str = "pending"
