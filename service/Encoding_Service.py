"""
Encoding Service for Video Transcoding Service
Executes transcoding tasks based on DAG workflows
"""
import os
import logging
from typing import List, Dict
from models import DAGDefinition, AssetRecord, TaskStatus

logger = logging.getLogger(__name__)


class EncodingService:
    """Encoding Service as specified in document"""
    
    def __init__(self, encoded_bucket: str = './encoded_bucket',
                 temp_bucket: str = './temp_bucket',
                 max_workers: int = 4, task_timeout_seconds: int = 3600,
                 max_retry_attempts: int = 3):
        self.encoded_bucket = encoded_bucket
        self.temp_bucket = temp_bucket
        self.max_workers = max_workers
        self.task_timeout_seconds = task_timeout_seconds
        self.max_retry_attempts = max_retry_attempts
        os.makedirs(encoded_bucket, exist_ok=True)
        os.makedirs(temp_bucket, exist_ok=True)

    def execute_dag(self, dag_def: DAGDefinition, source_asset: AssetRecord) -> List[str]:
        """
        Execute DAG tasks in dependency order
        
        In production, this would:
        1. Load DAG from workflow table
        2. Identify tasks with no dependencies (root tasks)
        3. Execute root tasks in parallel using worker pool
        4. On task completion, check for newly unblocked tasks
        5. Continue until all tasks complete or failure threshold exceeded
        
        Args:
            dag_def: DAG definition with tasks
            source_asset: Source asset record
            
        Returns:
            List of output URLs for encoded videos
        """
        logger.info(f"Starting DAG execution: {dag_def.dag_id} for asset {source_asset.asset_id}")
        output_urls = []
        
        # Simulate topological execution
        pending_tasks = dag_def.tasks.copy()
        completed_task_ids = set()
        
        while pending_tasks:
            # Find tasks with no pending dependencies
            ready_tasks = []
            for task in pending_tasks:
                deps_met = all(dep_id in completed_task_ids for dep_id in task.dependencies)
                if deps_met:
                    ready_tasks.append(task)
            
            if not ready_tasks:
                # Check for circular dependencies
                if pending_tasks:
                    failed_tasks = [t.task_id for t in pending_tasks]
                    raise RuntimeError(f"Circular dependency or failed tasks blocking: {failed_tasks}")
                break
            
            # Execute ready tasks (in production, this would be parallel)
            for task in ready_tasks:
                try:
                    output_url = self._execute_task(task, source_asset)
                    output_urls.append(output_url)
                    completed_task_ids.add(task.task_id)
                    pending_tasks.remove(task)
                    logger.info(f"Task completed: {task.task_id}")
                except Exception as e:
                    logger.error(f"Task failed: {task.task_id}: {e}")
                    task.status = TaskStatus.FAILED
                    task.retry_count += 1
                    
                    if task.retry_count <= self.max_retry_attempts:
                        logger.info(f"Retrying task {task.task_id} (attempt {task.retry_count})")
                        # Reset status for retry
                        task.status = TaskStatus.PENDING
                    else:
                        logger.error(f"Task {task.task_id} exceeded max retries")
                        pending_tasks.remove(task)
        
        # Clean up temp_bucket working copy now that encoding is done
        if source_asset.temp_url and os.path.exists(source_asset.temp_url):
            os.remove(source_asset.temp_url)
            logger.info(f"temp.cleaned - removed working copy: {source_asset.temp_url}")

        logger.info(f"DAG execution complete: {dag_def.dag_id} - {len(output_urls)} outputs")
        return output_urls

    def _execute_task(self, task, source_asset: AssetRecord) -> str:
        """
        Execute a single encoding task
        
        In production, this would:
        1. Worker claims task from queue
        2. Download source video from Storage Service
        3. Execute FFmpeg with encoding parameters
        4. Monitor progress and report status updates
        5. Upload encoded output to Storage Service
        6. Update task status and output URL in DAG
        7. Clean up temporary files
        
        Args:
            task: EncodingTask to execute
            source_asset: Source asset record
            
        Returns:
            Output URL for encoded video
        """
        task.status = TaskStatus.RUNNING
        logger.info(f"task.started - {task.task_id}")

        params = task.encoding_params

        # Build output filename
        codec = params.get('codec', 'h264')
        width = params.get('width', 1280)
        height = params.get('height', 720)
        preset = params.get('preset', 'medium')

        output_filename = f"{source_asset.asset_id[:8]}_{codec}_{width}x{height}_{preset}.mp4"
        output_path = os.path.join(self.encoded_bucket, output_filename)

        # Encode from temp_bucket working copy (original in asset_bucket is untouched)
        encode_source = source_asset.temp_url or source_asset.source_url

        # Log encoding parameters
        logger.info(f"Encoding parameters for {task.task_id}:")
        logger.info(f"  Source (temp): {encode_source}")
        logger.info(f"  Codec: {codec}")
        logger.info(f"  Resolution: {width}x{height}")
        logger.info(f"  Bitrate: {params.get('bitrate', 'auto')} bps")
        logger.info(f"  Preset: {preset}")
        logger.info(f"  Output: {output_path}")
        
        # In real implementation, would execute FFmpeg:
        self._simulate_ffmpeg_encoding(encode_source, output_path, params)
        
        task.status = TaskStatus.SUCCESS
        task.output_url = output_path
        
        logger.info(f"task.completed - {task.task_id}: {output_path}")
        return output_path

    def _simulate_ffmpeg_encoding(self, source_path: str, output_path: str, params: Dict):
        """
        Simulate FFmpeg encoding process
        
        In production, this would execute:
        ```python
        import subprocess
        
        cmd = [
            'ffmpeg', '-y', '-i', source_path,
            '-c:v', params['codec'],
            '-preset', params['preset'],
            '-b:v', str(params['bitrate']),
            '-s', f"{params['width']}x{params['height']}",
            '-c:a', 'aac', '-b:a', '128k',
            output_path
        ]
        
        # Execute with timeout and progress monitoring
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Monitor progress by parsing FFmpeg output
        while True:
            output = process.stderr.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                # Parse FFmpeg progress output
                # time=00:01:23.45 bitrate=2000.0kbits/s
                logger.debug(f"FFmpeg: {output.strip()}")
        
        return_code = process.poll()
        if return_code != 0:
            raise RuntimeError(f"FFmpeg failed with return code {return_code}")
        ```
        """
        
        # Simulate encoding by creating a placeholder file
        logger.info(f"Simulating encoding: {source_path} -> {output_path}")
        
        # Calculate simulated file size based on parameters
        duration = 120  # Assume 2 minute video
        bitrate = params.get('bitrate', 2500000)
        simulated_size = int((bitrate / 8) * duration * 0.9)  # 90% efficiency
        
        # Create output file with simulated content
        with open(output_path, 'w') as f:
            f.write(f"Encoded video: {params['codec']} {params['width']}x{params['height']}")
        
        logger.info(f"Simulated encoding complete - would be ~{simulated_size/1000000:.1f}MB")

    def get_encoding_progress(self, dag_id: str) -> Dict:
        """
        Get encoding progress for a DAG
        
        Args:
            dag_id: DAG identifier
            
        Returns:
            Progress information
        """
        # In production, would query workflow table
        # This is a simulation
        return {
            'dag_id': dag_id,
            'status': 'running',
            'progress_percent': 65,
            'completed_tasks': 2,
            'total_tasks': 4,
            'estimated_completion': '2024-01-15T14:30:00Z'
        }

    def cancel_encoding(self, dag_id: str) -> bool:
        """
        Cancel an encoding DAG
        
        Args:
            dag_id: DAG identifier
            
        Returns:
            True if successfully cancelled
        """
        logger.info(f"Cancelling encoding DAG: {dag_id}")
        
        # In production, would:
        # 1. Mark DAG as cancelled in workflow table
        # 2. Send stop signals to running workers
        # 3. Clean up temporary files
        # 4. Update task statuses
        
        return True

    def get_worker_stats(self) -> Dict:
        """
        Get worker utilization statistics
        
        Returns:
            Worker statistics
        """
        return {
            'max_workers': self.max_workers,
            'active_workers': 2,  # Simulated
            'queued_tasks': 5,    # Simulated
            'completed_today': 47,
            'failed_today': 2,
            'average_encode_time_minutes': 12.5,
            'worker_utilization_percent': 50
        }
