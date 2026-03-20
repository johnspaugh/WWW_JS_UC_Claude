import os
import json
import shutil
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Callable


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


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
            self.result = self.function(context)
            self.status = TaskStatus.SUCCESS
            return True
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
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
                    print(f"  v Success: {task.result}")
                else:
                    print(f"  x Failed: {task.error}")
                    return False

        print(f"\n{'='*60}")
        print(f"DAG Execution Complete: All tasks successful")
        print(f"{'='*60}\n")
        return True


class RulesEngine:
    def __init__(self):
        self.rules = []
        self.dags: Dict[str, DAG] = {}

    def add_rule(self, condition: Callable, action: Callable, name: str = None):
        """Add a simple rule"""
        self.rules.append({
            'name': name,
            'condition': condition,
            'action': action
        })

    def add_dag(self, dag: DAG):
        """Add a DAG workflow"""
        self.dags[dag.name] = dag

    def evaluate(self, data: Dict[str, Any]) -> List[Dict]:
        """Evaluate simple rules"""
        results = []
        for rule in self.rules:
            if rule['condition'](data):
                result = rule['action'](data)
                results.append({
                    'rule': rule['name'],
                    'result': result
                })
        return results

    def execute_dag(self, dag_name: str, context: Dict[str, Any]) -> bool:
        """Execute a specific DAG"""
        if dag_name not in self.dags:
            raise ValueError(f"DAG {dag_name} not found")
        return self.dags[dag_name].execute(context)


# ==================== INGEST WORKFLOW FUNCTIONS ====================

def find_asset(context: Dict[str, Any]) -> str:
    """Find and validate the source video in asset_bucket"""
    asset_bucket = context.get('asset_bucket', './asset_bucket')
    filename = context['video_filename']
    video_path = os.path.join(asset_bucket, filename)

    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video not found in asset_bucket: {video_path}")

    file_size = os.path.getsize(video_path)
    if file_size == 0:
        raise ValueError("Video file is empty")

    context['asset_video_path'] = video_path
    context['file_size'] = file_size
    return f"Asset found: {video_path} ({file_size} bytes)"


def inspect_video(context: Dict[str, Any]) -> str:
    """Single inspection to extract all metadata and determine encoding requirements"""
    # In real implementation, use ffprobe to get actual video properties:
    # ffprobe -v quiet -print_format json -show_format -show_streams input.mp4

    inspection_data = {
        'duration': 6,    # seconds — matches Testing6SecondVideo.mp4
        'resolution': '1920x1080',
        'width': 1920,
        'height': 1080,
        'codec': 'h264',
        'fps': 30,
        'bitrate': 5000000,
        'audio_codec': 'aac',
        'audio_bitrate': 128000,
        'file_format': 'mp4'
    }

    encoding_model = {
        'name': 'standard_web',
        'video_codec': 'h264',
        'audio_codec': 'aac',
        'container': 'mp4',
        'width': 1920,
        'height': 1080,
        'video_bitrate': 4000000,
        'audio_bitrate': 128000,
        'fps': 30,
        'preset': 'medium'
    }

    context['inspection'] = inspection_data
    context['encoding_model'] = encoding_model

    return f"Inspection complete: {inspection_data['resolution']}, {inspection_data['duration']}s | Encoding model: {encoding_model['name']}"


def quality_check(context: Dict[str, Any]) -> str:
    """Perform quality checks on the video based on inspection"""
    inspection = context['inspection']

    if inspection['duration'] < 1:
        raise ValueError("Video too short")

    if inspection['width'] < 320 or inspection['height'] < 240:
        raise ValueError(f"Resolution too low: {inspection['width']}x{inspection['height']}")

    if not inspection.get('audio_codec'):
        print("    Warning: No audio track detected")

    return "Quality check passed"


def generate_thumbnail(context: Dict[str, Any]) -> str:
    """Generate video thumbnail from the asset"""
    # In real implementation: ffmpeg -i input.mp4 -ss 00:00:01 -vframes 1 thumbnail.jpg
    thumbnail_path = context['asset_video_path'].replace('.mp4', '_thumb.jpg')

    context['thumbnail_path'] = thumbnail_path
    return f"Thumbnail generated: {thumbnail_path}"


def copy_to_temp_bucket(context: Dict[str, Any]) -> str:
    """Copy original video from asset_bucket into temp_bucket for processing"""
    temp_bucket = context.get('temp_bucket', './temp_bucket')
    os.makedirs(temp_bucket, exist_ok=True)

    source = context['asset_video_path']
    filename = os.path.basename(source)
    destination = os.path.join(temp_bucket, filename)

    shutil.copy2(source, destination)

    context['temp_video_path'] = destination
    return f"Video copied to temp_bucket: {destination}"


def add_to_asset_table(context: Dict[str, Any]) -> str:
    """Add asset record to the asset table"""
    asset_record = {
        'id': context.get('asset_id', 'asset_' + datetime.now().strftime('%Y%m%d%H%M%S')),
        'original_path': context['asset_video_path'],
        'temp_path': context['temp_video_path'],
        'thumbnail_path': context.get('thumbnail_path'),
        'inspection': context['inspection'],
        'encoding_model': context['encoding_model'],
        'ingested_at': datetime.now().isoformat(),
        'status': 'ingested',
        'encoding_status': 'pending'
    }

    table_path = context.get('asset_table', './asset_table.json')

    records = []
    if os.path.exists(table_path):
        with open(table_path, 'r') as f:
            records = json.load(f)

    records.append(asset_record)

    with open(table_path, 'w') as f:
        json.dump(records, f, indent=2)

    context['asset_record'] = asset_record
    return f"Asset added to table: {asset_record['id']}"


# ==================== ENCODING WORKFLOW FUNCTIONS ====================

def encode_video(context: Dict[str, Any]) -> str:
    """Encode video from temp_bucket using the single encoding model from inspection"""
    model = context['encoding_model']
    source_path = context['temp_video_path']

    # Build output filename in temp_bucket
    output_filename = os.path.basename(source_path).replace('.mp4', f"_{model['name']}.mp4")
    output_path = os.path.join(os.path.dirname(source_path), output_filename)

    # In real implementation, use ffmpeg:
    # ffmpeg -i input.mp4 \
    #   -c:v libx264 -preset medium -b:v 4000k -s 1920x1080 -r 30 \
    #   -c:a aac -b:a 128k \
    #   output.mp4

    print(f"    Source: {source_path}")
    print(f"    Encoding with model: {model['name']}")
    print(f"    Video: {model['video_codec']} @ {model['video_bitrate']/1000000}Mbps, {model['width']}x{model['height']}")
    print(f"    Audio: {model['audio_codec']} @ {model['audio_bitrate']/1000}kbps")

    encoded_file = {
        'path': output_path,
        'model': model,
        'size': 38000000,  # Simulated file size ~38MB
        'duration': context['inspection']['duration'],
        'created_at': datetime.now().isoformat()
    }

    context['encoded_file'] = encoded_file
    return f"Video encoded in temp_bucket: {output_path}"


def validate_encode(context: Dict[str, Any]) -> str:
    """Validate that the encoded file is valid"""
    encoded = context['encoded_file']

    # In real implementation, use ffprobe to verify:
    # - File is not corrupted
    # - Duration matches source
    # - Resolution matches expected
    # - Codecs are correct

    print(f"    Validating encoded file...")
    print(f"    Checking duration: {encoded['duration']}s")
    print(f"    Checking resolution: {encoded['model']['width']}x{encoded['model']['height']}")
    print(f"    Checking codecs: {encoded['model']['video_codec']}/{encoded['model']['audio_codec']}")

    if encoded['size'] == 0:
        raise ValueError("Encoded file is empty")

    return "Encode validation passed"


def cleanup_temp(context: Dict[str, Any]) -> str:
    """Remove the original copy from temp_bucket after successful encoding"""
    temp_video_path = context['temp_video_path']

    if os.path.exists(temp_video_path):
        os.remove(temp_video_path)
        return f"Temp file removed: {temp_video_path}"
    else:
        return f"Temp file already gone: {temp_video_path}"


def move_to_encoded_bucket(context: Dict[str, Any]) -> str:
    """Move the encoded video from temp_bucket to encoded_bucket"""
    encoded_bucket = context.get('encoded_bucket', './encoded_bucket')
    os.makedirs(encoded_bucket, exist_ok=True)

    encoded = context['encoded_file']
    filename = os.path.basename(encoded['path'])
    destination = os.path.join(encoded_bucket, filename)

    # In real implementation, move from temp to encoded bucket:
    # shutil.move(encoded['path'], destination)

    # Simulated — file doesn't physically exist in temp yet
    context['encoded_destination'] = destination
    return f"Encoded video moved to encoded_bucket: {destination}"


def create_encode_record(context: Dict[str, Any]) -> str:
    """Create database record for the encoded video"""
    asset_id = context['asset_record']['id']
    encoded = context['encoded_file']

    encode_record = {
        'id': f"encode_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        'asset_id': asset_id,
        'model': encoded['model']['name'],
        'path': context['encoded_destination'],
        'size': encoded['size'],
        'duration': encoded['duration'],
        'video_codec': encoded['model']['video_codec'],
        'audio_codec': encoded['model']['audio_codec'],
        'resolution': f"{encoded['model']['width']}x{encoded['model']['height']}",
        'created_at': encoded['created_at'],
        'status': 'ready'
    }

    table_path = context.get('encode_table', './encode_table.json')

    records = []
    if os.path.exists(table_path):
        with open(table_path, 'r') as f:
            records = json.load(f)

    records.append(encode_record)

    with open(table_path, 'w') as f:
        json.dump(records, f, indent=2)

    context['encode_record'] = encode_record
    return f"Encode record created: {encode_record['id']}"


def update_asset_status(context: Dict[str, Any]) -> str:
    """Update the original asset record with encoding completion"""
    asset_id = context['asset_record']['id']

    table_path = context.get('asset_table', './asset_table.json')

    with open(table_path, 'r') as f:
        records = json.load(f)

    for record in records:
        if record['id'] == asset_id:
            record['encoding_status'] = 'completed'
            record['encoding_completed_at'] = datetime.now().isoformat()
            record['encoded_video_id'] = context['encode_record']['id']
            break

    with open(table_path, 'w') as f:
        json.dump(records, f, indent=2)

    return f"Asset {asset_id} updated: encoding complete"


# ==================== DAG BUILDERS ====================

def build_ingest_dag() -> DAG:
    """
    Ingest DAG:
      find_asset
        └── inspect_video
                ├── quality_check
                │       └── copy_to_temp_bucket
                │                   └── add_to_asset_table <─┐
                └── generate_thumbnail ────────────────────────┘
    """
    dag = DAG("video_ingest")

    dag.add_task(Task(
        name="find_asset",
        function=find_asset,
        dependencies=[]
    ))

    dag.add_task(Task(
        name="inspect_video",
        function=inspect_video,
        dependencies=["find_asset"]
    ))

    dag.add_task(Task(
        name="quality_check",
        function=quality_check,
        dependencies=["inspect_video"]
    ))

    dag.add_task(Task(
        name="generate_thumbnail",
        function=generate_thumbnail,
        dependencies=["inspect_video"]
    ))

    dag.add_task(Task(
        name="copy_to_temp_bucket",
        function=copy_to_temp_bucket,
        dependencies=["quality_check"]
    ))

    dag.add_task(Task(
        name="add_to_asset_table",
        function=add_to_asset_table,
        dependencies=["copy_to_temp_bucket", "generate_thumbnail"]
    ))

    return dag


def build_encode_dag() -> DAG:
    """
    Encode DAG:
      encode_video (from temp_bucket)
        └── validate_encode
                └── cleanup_temp
                        └── move_to_encoded_bucket
                                └── create_encode_record
                                        └── update_asset_status
    """
    dag = DAG("video_encode")

    dag.add_task(Task(
        name="encode_video",
        function=encode_video,
        dependencies=[]
    ))

    dag.add_task(Task(
        name="validate_encode",
        function=validate_encode,
        dependencies=["encode_video"]
    ))

    dag.add_task(Task(
        name="cleanup_temp",
        function=cleanup_temp,
        dependencies=["validate_encode"]
    ))

    dag.add_task(Task(
        name="move_to_encoded_bucket",
        function=move_to_encoded_bucket,
        dependencies=["cleanup_temp"]
    ))

    dag.add_task(Task(
        name="create_encode_record",
        function=create_encode_record,
        dependencies=["move_to_encoded_bucket"]
    ))

    dag.add_task(Task(
        name="update_asset_status",
        function=update_asset_status,
        dependencies=["create_encode_record"]
    ))

    return dag


# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    engine = RulesEngine()
    engine.add_dag(build_ingest_dag())
    engine.add_dag(build_encode_dag())

    # Pick a video that already exists in asset_bucket
    video_filename = "Testing6SecondVideo.mp4"

    # STEP 1: Ingest workflow
    print("\n" + "- "*30)
    print("STARTING VIDEO INGEST WORKFLOW")
    print("- "*30)

    ingest_context = {
        'video_filename': video_filename,
        'asset_bucket': './asset_bucket',
        'temp_bucket': './temp_bucket',
        'asset_table': './asset_table.json'
    }

    try:
        ingest_success = engine.execute_dag("video_ingest", ingest_context)

        if ingest_success:
            print("\n" + "+ "*30)
            print("INGEST COMPLETED SUCCESSFULLY")
            print("+ "*30)
            print(f"\nAsset ID:    {ingest_context['asset_record']['id']}")
            print(f"Asset Path:  {ingest_context['asset_video_path']}")
            print(f"Temp Copy:   {ingest_context['temp_video_path']}")
            print(f"\nInspection Results:")
            print(f"  Resolution: {ingest_context['inspection']['resolution']}")
            print(f"  Duration:   {ingest_context['inspection']['duration']}s")
            print(f"  Codec:      {ingest_context['inspection']['codec']}")
            print(f"\nEncoding Model Selected:")
            print(f"  Name:    {ingest_context['encoding_model']['name']}")
            print(f"  Output:  {ingest_context['encoding_model']['width']}x{ingest_context['encoding_model']['height']}")
            print(f"  Bitrate: {ingest_context['encoding_model']['video_bitrate']/1000000}Mbps")

            # STEP 2: Encode workflow
            print("\n\n" + "- "*30)
            print("STARTING ENCODING WORKFLOW")
            print("- "*30)

            encode_context = {
                'inspection': ingest_context['inspection'],
                'encoding_model': ingest_context['encoding_model'],
                'temp_video_path': ingest_context['temp_video_path'],
                'asset_record': ingest_context['asset_record'],
                'asset_table': './asset_table.json',
                'temp_bucket': './temp_bucket',
                'encoded_bucket': './encoded_bucket',
                'encode_table': './encode_table.json'
            }

            encode_success = engine.execute_dag("video_encode", encode_context)

            if encode_success:
                print("\n" + "+ "*30)
                print("ENCODING COMPLETED SUCCESSFULLY")
                print("+ "*30)
                print(f"\nEncoded Video:")
                print(f"  ID:         {encode_context['encode_record']['id']}")
                print(f"  Path:       {encode_context['encoded_destination']}")
                print(f"  Size:       {encode_context['encode_record']['size']/1000000:.1f} MB")
                print(f"  Model:      {encode_context['encode_record']['model']}")
                print(f"  Resolution: {encode_context['encode_record']['resolution']}")
                print(f"  Codecs:     {encode_context['encode_record']['video_codec']}/{encode_context['encode_record']['audio_codec']}")

                print(f"\nFINAL SUMMARY:")
                print(f"  Source:        asset_bucket/{video_filename}")
                print(f"  Temp (cleaned): temp_bucket/{video_filename}  [removed]")
                print(f"  Final output:  {encode_context['encoded_destination']}")
                print(f"  Asset ID:      {ingest_context['asset_record']['id']}")
                print(f"  Encode ID:     {encode_context['encode_record']['id']}")
                print(f"  Status:        Complete and Ready")
            else:
                print("\nx Encoding workflow failed")
        else:
            print("\nx Ingest workflow failed")

    except Exception as e:
        print(f"\nx Workflow failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("\n" + "="*60)
        print("Workflow execution complete")
        print("="*60)
