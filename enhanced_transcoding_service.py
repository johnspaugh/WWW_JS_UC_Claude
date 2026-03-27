import os
import json
import shutil
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Callable
import logging
from dataclasses import dataclass, asdict


# Configure logging as specified in the document
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transcoding_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


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
    source_url: str
    status: AssetStatus
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


class Task:
    def __init__(self, name: str, function: Callable, dependencies: List[str] = None):
        self.name = name
        self.function = function
        self.dependencies = dependencies or []
        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None

    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute the task and return success status"""
        try:
            self.status = TaskStatus.RUNNING
            logger.info(f"task.started - {self.name}")
            self.result = self.function(context)
            self.status = TaskStatus.SUCCESS
            logger.info(f"task.completed - {self.name}: {self.result}")
            return True
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
            logger.error(f"task.failed - {self.name}: {self.error}")
            return False


class DAG:
    """Directed Acyclic Graph for task execution"""
    def __init__(self, name: str):
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.execution_log = []

    def add_task(self, task: Task):
        """Add a task to the DAG"""
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name} already exists")
        self.tasks[task.name] = task

    def _get_ready_tasks(self) -> List[Task]:
        """Get tasks that are ready to execute (dependencies met)"""
        ready = []
        for task in self.tasks.values():
            if task.status != TaskStatus.PENDING:
                continue

            deps_met = all(
                self.tasks[dep].status == TaskStatus.SUCCESS
                for dep in task.dependencies
            )
            if deps_met:
                ready.append(task)
        return ready

    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute all tasks in topological order"""
        logger.info(f"dag.started - {self.name}")
        print(f"\n{'='*60}")
        print(f"Executing DAG: {self.name}")
        print(f"{'='*60}\n")

        while True:
            ready_tasks = self._get_ready_tasks()

            if not ready_tasks:
                pending = [t for t in self.tasks.values() if t.status == TaskStatus.PENDING]
                if pending:
                    raise RuntimeError(f"DAG has circular dependency or failed tasks blocking: {[t.name for t in pending]}")
                break

            for task in ready_tasks:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Executing: {task.name}")
                success = task.execute(context)

                log_entry = {
                    'task': task.name,
                    'status': task.status.value,
                    'timestamp': datetime.now().isoformat(),
                    'result': task.result,
                    'error': task.error
                }
                self.execution_log.append(log_entry)

                if success:
                    print(f"  ✓ Success: {task.result}")
                else:
                    print(f"  ✗ Failed: {task.error}")
                    return False

        logger.info(f"dag.completed - {self.name}")
        print(f"\n{'='*60}")
        print(f"DAG Execution Complete: All tasks successful")
        print(f"{'='*60}\n")
        return True


class RulesEngine:
    """Rules Engine as specified in document - evaluates conditions and generates DAGs"""
    def __init__(self):
        self.rules = []
        self.dags: Dict[str, DAG] = {}

    def add_rule(self, name: str, priority: int, conditions: List[Dict], 
                 tasks: List[Dict], dag_generator: Callable = None):
        """Add a rule as specified in document"""
        self.rules.append({
            'name': name,
            'priority': priority,
            'conditions': conditions,
            'tasks': tasks,
            'dag_generator': dag_generator
        })
        # Sort by priority (lower number = higher priority)
        self.rules.sort(key=lambda x: x['priority'])

    def add_dag(self, dag: DAG):
        """Add a DAG workflow"""
        self.dags[dag.name] = dag

    def evaluate_conditions(self, conditions: List[Dict], asset: AssetRecord) -> bool:
        """Evaluate if all conditions are met"""
        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            
            # Get actual value from asset
            if field == 'video_type':
                actual = asset.video_type
            elif field == 'resolution_height' and asset.inspection_data:
                actual = asset.inspection_data.height
            elif field == 'codec' and asset.inspection_data:
                actual = asset.inspection_data.codec
            else:
                continue  # Skip unknown fields
            
            # Apply operator
            if operator == 'equals' and actual != value:
                return False
            elif operator == 'not_equals' and actual == value:
                return False
            elif operator == 'greater_than' and actual <= value:
                return False
            elif operator == 'less_than' and actual >= value:
                return False
            elif operator == 'contains' and value not in actual:
                return False
        
        return True

    def generate_dag_for_asset(self, asset: AssetRecord) -> DAGDefinition:
        """Generate DAG based on rules evaluation"""
        for rule in self.rules:
            if self.evaluate_conditions(rule['conditions'], asset):
                logger.info(f"Rule matched: {rule['name']} for asset {asset.asset_id}")
                
                # Generate encoding tasks based on rule
                encoding_tasks = []
                for task_def in rule['tasks']:
                    task = EncodingTask(
                        task_id=f"{task_def['name']}_{asset.asset_id}",
                        dependencies=task_def.get('dependencies', []),
                        encoding_params=task_def['encoding_params']
                    )
                    encoding_tasks.append(task)
                
                dag_def = DAGDefinition(
                    dag_id=str(uuid.uuid4()),
                    asset_uuid=asset.asset_id,
                    tasks=encoding_tasks
                )
                
                logger.info(f"dag.created - {dag_def.dag_id} with {len(encoding_tasks)} tasks")
                return dag_def
        
        # No matching rule - create default
        logger.warning(f"No matching rule for asset {asset.asset_id}, using default")
        return self._create_default_dag(asset)

    def _create_default_dag(self, asset: AssetRecord) -> DAGDefinition:
        """Create default encoding DAG when no rules match"""
        default_task = EncodingTask(
            task_id=f"default_encode_{asset.asset_id}",
            dependencies=[],
            encoding_params={
                'codec': 'h264',
                'resolution': '1920x1080',
                'bitrate': 4000000,
                'preset': 'medium'
            }
        )
        
        return DAGDefinition(
            dag_id=str(uuid.uuid4()),
            asset_uuid=asset.asset_id,
            tasks=[default_task]
        )

    def execute_dag(self, dag_name: str, context: Dict[str, Any]) -> bool:
        """Execute a specific DAG"""
        if dag_name not in self.dags:
            raise ValueError(f"DAG {dag_name} not found")
        return self.dags[dag_name].execute(context)


class IngestService:
    """Ingest Service as specified in document"""
    
    def __init__(self, asset_bucket: str = './asset_bucket', asset_table_path: str = './asset_table.json'):
        self.asset_bucket = asset_bucket
        self.asset_table_path = asset_table_path
        os.makedirs(asset_bucket, exist_ok=True)

    def ingest_video(self, s3_url: str, video_type: str, metadata: Dict = None) -> AssetRecord:
        """Initiates video ingest workflow - implements POST /api/v1/ingest"""
        asset_id = str(uuid.uuid4())
        
        # For demo, assume s3_url is a local file path
        if not os.path.exists(s3_url):
            raise FileNotFoundError(f"Source video not found: {s3_url}")
        
        # Move to internal storage
        filename = os.path.basename(s3_url)
        internal_url = os.path.join(self.asset_bucket, filename)
        shutil.copy2(s3_url, internal_url)
        
        # Create asset record
        asset = AssetRecord(
            asset_id=asset_id,
            video_type=video_type,
            source_url=internal_url,
            status=AssetStatus.INGESTED
        )
        
        self._save_asset(asset)
        logger.info(f"asset.ingested - {asset_id}")
        
        return asset

    def _save_asset(self, asset: AssetRecord):
        """Save asset to asset table"""
        assets = self._load_assets()
        assets[asset.asset_id] = asdict(asset)
        
        with open(self.asset_table_path, 'w') as f:
            json.dump(assets, f, indent=2)

    def _load_assets(self) -> Dict:
        """Load assets from asset table"""
        if os.path.exists(self.asset_table_path):
            with open(self.asset_table_path, 'r') as f:
                return json.load(f)
        return {}

    def get_asset(self, asset_id: str) -> AssetRecord:
        """Get asset by ID - implements GET /api/v1/assets/{uuid}"""
        assets = self._load_assets()
        if asset_id not in assets:
            raise ValueError(f"Asset {asset_id} not found")
        
        asset_data = assets[asset_id]
        
        # Reconstruct InspectionData if present
        inspection_data = None
        if asset_data.get('inspection_data'):
            inspection_data = InspectionData(**asset_data['inspection_data'])
        
        return AssetRecord(
            asset_id=asset_data['asset_id'],
            video_type=asset_data['video_type'],
            source_url=asset_data['source_url'],
            status=AssetStatus(asset_data['status']),
            inspection_data=inspection_data,
            created_at=asset_data['created_at'],
            updated_at=asset_data['updated_at']
        )


class InspectionService:
    """Inspection Service as specified in document"""
    
    def __init__(self, ingest_service: IngestService):
        self.ingest_service = ingest_service

    def inspect_asset(self, asset_id: str) -> InspectionData:
        """Analyzes video using FFprobe/MediaInfo - implements inspection workflow"""
        asset = self.ingest_service.get_asset(asset_id)
        
        # Simulate inspection (in real implementation, use ffprobe)
        inspection_data = InspectionData(
            codec='h264',
            resolution='1920x1080',
            width=1920,
            height=1080,
            bitrate=5000000,
            duration=120.0,
            frame_rate=30.0,
            color_space='yuv420p',
            audio_tracks=[{
                'codec': 'aac',
                'channels': 2,
                'sample_rate': 48000,
                'bitrate': 128000
            }],
            file_size=os.path.getsize(asset.source_url) if os.path.exists(asset.source_url) else 0
        )
        
        # Update asset with inspection data
        asset.inspection_data = inspection_data
        asset.status = AssetStatus.INSPECTED
        asset.updated_at = datetime.now().isoformat()
        
        self.ingest_service._save_asset(asset)
        logger.info(f"asset.inspected - {asset_id}")
        
        return inspection_data


class EncodingService:
    """Encoding Service as specified in document"""
    
    def __init__(self, encoded_bucket: str = './encoded_bucket'):
        self.encoded_bucket = encoded_bucket
        os.makedirs(encoded_bucket, exist_ok=True)

    def execute_dag(self, dag_def: DAGDefinition, source_asset: AssetRecord) -> List[str]:
        """Execute DAG tasks in dependency order"""
        output_urls = []
        
        for task in dag_def.tasks:
            if task.status != TaskStatus.PENDING:
                continue
                
            # Simulate encoding
            output_filename = f"{source_asset.asset_id}_{task.task_id}.mp4"
            output_path = os.path.join(self.encoded_bucket, output_filename)
            
            logger.info(f"Encoding task: {task.task_id}")
            logger.info(f"Params: {task.encoding_params}")
            
            # Simulate FFmpeg encoding
            # In real implementation: subprocess.run(['ffmpeg', ...])
            
            task.status = TaskStatus.SUCCESS
            task.output_url = output_path
            output_urls.append(output_path)
        
        return output_urls


# ==================== SETUP RULES AS SPECIFIED IN DOCUMENT ====================

def setup_encoding_rules(rules_engine: RulesEngine):
    """Setup encoding rules as specified in document"""
    
    # Rule: Premium Movie ABR Ladder
    rules_engine.add_rule(
        name="premium_movie_abr",
        priority=1,
        conditions=[
            {'field': 'video_type', 'operator': 'equals', 'value': 'movie'},
            {'field': 'resolution_height', 'operator': 'greater_than', 'value': 1079}
        ],
        tasks=[
            {
                'name': 'h264_1080p',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1920,
                    'height': 1080,
                    'bitrate': 5000000,
                    'preset': 'medium'
                }
            },
            {
                'name': 'h264_720p',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1280,
                    'height': 720,
                    'bitrate': 3000000,
                    'preset': 'medium'
                }
            },
            {
                'name': 'h265_1080p',
                'dependencies': ['h264_1080p'],
                'encoding_params': {
                    'codec': 'h265',
                    'width': 1920,
                    'height': 1080,
                    'bitrate': 3500000,
                    'preset': 'medium'
                }
            }
        ]
    )
    
    # Rule: User Content Quick Encode
    rules_engine.add_rule(
        name="user_content_quick",
        priority=2,
        conditions=[
            {'field': 'video_type', 'operator': 'equals', 'value': 'user-content'}
        ],
        tasks=[
            {
                'name': 'h264_720p_fast',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1280,
                    'height': 720,
                    'bitrate': 2500000,
                    'preset': 'fast'
                }
            },
            {
                'name': 'h264_480p_fast',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 854,
                    'height': 480,
                    'bitrate': 1200000,
                    'preset': 'fast'
                }
            }
        ]
    )


# ==================== ORCHESTRATOR ====================

class Orchestrator:
    """Orchestrator as specified in document - coordinates all services"""
    
    def __init__(self):
        self.ingest_service = IngestService()
        self.inspection_service = InspectionService(self.ingest_service)
        self.encoding_service = EncodingService()
        self.rules_engine = RulesEngine()
        
        # Setup rules as per document
        setup_encoding_rules(self.rules_engine)

    def process_video(self, s3_url: str, video_type: str) -> Dict[str, Any]:
        """Complete end-to-end workflow as specified in document"""
        try:
            # Step 1: Ingest
            logger.info("=== STEP 1: INGEST ===")
            asset = self.ingest_service.ingest_video(s3_url, video_type)
            
            # Step 2: Inspection  
            logger.info("=== STEP 2: INSPECTION ===")
            inspection_data = self.inspection_service.inspect_asset(asset.asset_id)
            
            # Step 3: Rules Evaluation and DAG Generation
            logger.info("=== STEP 3: RULES EVALUATION ===")
            asset = self.ingest_service.get_asset(asset.asset_id)  # Reload with inspection data
            dag_def = self.rules_engine.generate_dag_for_asset(asset)
            
            # Step 4: Encoding Execution
            logger.info("=== STEP 4: ENCODING ===")
            output_urls = self.encoding_service.execute_dag(dag_def, asset)
            
            # Update final status
            asset.status = AssetStatus.COMPLETED
            asset.updated_at = datetime.now().isoformat()
            self.ingest_service._save_asset(asset)
            
            return {
                'asset_id': asset.asset_id,
                'status': 'completed',
                'inspection_data': asdict(inspection_data),
                'dag_id': dag_def.dag_id,
                'output_renditions': output_urls
            }
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }


# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    # Create test video file
    test_video_path = "./test_movie.mp4"
    with open(test_video_path, 'w') as f:
        f.write("fake movie content for testing")

    try:
        # Initialize orchestrator
        orchestrator = Orchestrator()
        
        print("\n🎬 " + "="*60)
        print("VIDEO ENCODING SERVICE - DOCUMENT COMPLIANT VERSION")
        print("="*60 + " 🎬\n")
        
        # Process a movie (triggers premium ABR ladder)
        print("Processing Movie...")
        result = orchestrator.process_video(test_video_path, 'movie')
        
        print("\n📊 FINAL RESULT:")
        print(json.dumps(result, indent=2))
        
        # Demo API-like access
        print("\n🔍 ASSET LOOKUP (GET /api/v1/assets/{uuid}):")
        if result.get('asset_id'):
            asset = orchestrator.ingest_service.get_asset(result['asset_id'])
            print(f"Asset ID: {asset.asset_id}")
            print(f"Video Type: {asset.video_type}")
            print(f"Status: {asset.status.value}")
            if asset.inspection_data:
                print(f"Resolution: {asset.inspection_data.resolution}")
                print(f"Duration: {asset.inspection_data.duration}s")
                print(f"Codec: {asset.inspection_data.codec}")
        
    except Exception as e:
        logger.error(f"System error: {e}")
        print(f"❌ System error: {e}")
    
    finally:
        # Cleanup
        if os.path.exists(test_video_path):
            os.remove(test_video_path)
        
        print("\n" + "="*60)
        print("Workflow execution complete - see transcoding_service.log for details")
        print("="*60)
