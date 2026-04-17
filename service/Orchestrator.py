"""
Orchestrator for Video Transcoding Service
Coordinates all services and manages end-to-end workflow
"""
import json
import logging
import os
from typing import Dict, Any
from datetime import datetime
from dataclasses import asdict

from models import AssetStatus
from DAG import DAG, Task
from Ingest_Service import IngestService
from Inspection_Service import InspectionService
from Encoding_Service import EncodingService
from Rules_Engine import RulesEngine, setup_encoding_rules

logger = logging.getLogger(__name__)


class Orchestrator:
    """Orchestrator as specified in document - coordinates all services"""
    
    def __init__(self, asset_bucket: str = './asset_bucket',
                 encoded_bucket: str = './encoded_bucket',
                 temp_bucket: str = './temp_bucket',
                 asset_table_path: str = './asset_table.json',
                 workflow_table_path: str = './workflow_table.json'):

        # Initialize services
        self.ingest_service = IngestService(asset_bucket, asset_table_path, temp_bucket)
        self.inspection_service = InspectionService(self.ingest_service)
        self.encoding_service = EncodingService(encoded_bucket, temp_bucket)
        self.rules_engine = RulesEngine()
        self.workflow_table_path = workflow_table_path

        # Setup encoding rules as specified in document
        setup_encoding_rules(self.rules_engine)

        logger.info("Orchestrator initialized with all services")

    def process_video(self, video_filename: str, video_type: str, metadata: Dict = None) -> Dict[str, Any]:
        """
        Complete end-to-end workflow executed via DAG.py pipeline.

        Step 1 — Ingest:
            video_filename must already exist in asset_bucket.
            IngestService copies it to temp_bucket and creates the asset record.

        Step 2 — Ingest Rules Evaluation (DAG pipeline begins):
            Rules Engine is triggered; current ingest rule schedules
            Inspection followed by Post-Inspect Rules Evaluation.

        Step 3 — Inspection (DAG task):
            InspectionService extracts metadata and stores results in asset table.

        Step 4 — Post-Inspect Rules Evaluation + DAG Generation (DAG task):
            Rules Engine evaluates video type and technical properties,
            generates an encoding DAGDefinition, and stores it in workflow_table.

        Step 5 — Encoding Execution (DAG task):
            EncodingService executes the encoding DAGDefinition from temp_bucket
            to encoded_bucket, then removes the temp working copy.

        Args:
            video_filename: Filename of the video already in asset_bucket
            video_type: Content classification (movie, tv-episode, trailer, user-content)
            metadata: Optional additional metadata

        Returns:
            Workflow result with asset ID, status, DAG info, and output renditions
        """
        asset = None
        try:
            # ── Step 1: Ingest ──────────────────────────────────────────────
            logger.info("=== STEP 1: INGEST ===")
            asset = self.ingest_service.ingest_video(video_filename, video_type, metadata)

            # Shared context passed through all DAG tasks
            pipeline_ctx: Dict[str, Any] = {
                'asset_id': asset.asset_id,
                'video_filename': video_filename,
            }

            # ── Steps 2-5: Build pipeline DAG using DAG.py ─────────────────
            # Each task is a callable that reads/writes pipeline_ctx so results
            # flow from one task to the next without re-querying unnecessarily.

            def task_inspect(ctx: Dict) -> str:
                """Step 3: Inspection"""
                logger.info("=== STEP 3: INSPECTION ===")
                inspection = self.inspection_service.inspect_asset(ctx['asset_id'])
                ctx['inspection_data'] = inspection
                return f"Inspected: {inspection.resolution}, {inspection.duration}s, codec={inspection.codec}"

            def task_evaluate_rules(ctx: Dict) -> str:
                """Step 4: Post-Inspect Rules Evaluation + DAG Generation"""
                logger.info("=== STEP 4: POST-INSPECT RULES EVALUATION ===")
                asset_with_inspection = self.ingest_service.get_asset(ctx['asset_id'])
                dag_def = self.rules_engine.generate_dag_for_asset(asset_with_inspection)
                ctx['dag_def'] = dag_def
                ctx['asset_reloaded'] = asset_with_inspection
                # Persist encoding DAG to workflow table
                self._save_workflow(dag_def, ctx['asset_id'])
                return f"DAG generated: {dag_def.dag_id} ({len(dag_def.tasks)} encoding tasks) — stored in workflow_table"

            def task_encode(ctx: Dict) -> str:
                """Step 5: Encoding Execution"""
                logger.info("=== STEP 5: ENCODING EXECUTION ===")
                self.ingest_service.update_asset_status(ctx['asset_id'], AssetStatus.ENCODING)
                output_urls = self.encoding_service.execute_dag(ctx['dag_def'], ctx['asset_reloaded'])
                ctx['output_urls'] = output_urls
                self.ingest_service.update_asset_status(ctx['asset_id'], AssetStatus.COMPLETED)
                return f"Encoding complete: {len(output_urls)} rendition(s) in encoded_bucket"

            # ── Step 2: Ingest Rules Evaluation ─────────────────────────────
            # The ingest rule for this service is: always run Inspection then
            # Post-Inspect Rules Evaluation before encoding.  This is expressed
            # as a linear dependency chain in the pipeline DAG.
            logger.info("=== STEP 2: INGEST RULES EVALUATION (building pipeline DAG) ===")

            pipeline = DAG(f"pipeline_{asset.asset_id[:8]}")
            pipeline.add_task(Task(
                name='inspect_video',
                function=task_inspect
            ))
            pipeline.add_task(Task(
                name='evaluate_post_inspect_rules',
                function=task_evaluate_rules,
                dependencies=['inspect_video']
            ))
            pipeline.add_task(Task(
                name='execute_encoding',
                function=task_encode,
                dependencies=['evaluate_post_inspect_rules']
            ))

            # Execute the full pipeline through DAG.py
            success = pipeline.execute(pipeline_ctx)

            if not success:
                raise RuntimeError("Pipeline DAG execution failed — see logs for task-level details")

            dag_def = pipeline_ctx['dag_def']
            return {
                'status': 'completed',
                'asset_id': asset.asset_id,
                'video_type': video_type,
                'inspection_data': asdict(pipeline_ctx['inspection_data']),
                'dag_id': dag_def.dag_id,
                'task_count': len(dag_def.tasks),
                'output_renditions': pipeline_ctx.get('output_urls', []),
                'processing_time_seconds': None,  # Would calculate in production
                'created_at': asset.created_at
            }

        except Exception as e:
            logger.error(f"Workflow failed for {video_filename}: {e}")
            if asset:
                try:
                    self.ingest_service.update_asset_status(asset.asset_id, AssetStatus.FAILED)
                except Exception:
                    pass
            return {
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'failed_at': datetime.now().isoformat()
            }

    # ── Workflow table helpers ───────────────────────────────────────────────

    def _save_workflow(self, dag_def, asset_id: str):
        """Persist an encoding DAGDefinition to workflow_table.json"""
        workflows = self._load_workflows()
        workflows[dag_def.dag_id] = {
            'dag_id': dag_def.dag_id,
            'asset_uuid': asset_id,
            'status': dag_def.status,
            'task_count': len(dag_def.tasks),
            'tasks': [
                {
                    'task_id': t.task_id,
                    'dependencies': t.dependencies,
                    'encoding_params': t.encoding_params,
                    'status': t.status.value
                }
                for t in dag_def.tasks
            ],
            'created_at': datetime.now().isoformat()
        }
        with open(self.workflow_table_path, 'w') as f:
            json.dump(workflows, f, indent=2)
        logger.info(f"workflow.saved - {dag_def.dag_id} for asset {asset_id}")

    def _load_workflows(self) -> Dict:
        """Load workflow table from disk"""
        if os.path.exists(self.workflow_table_path):
            with open(self.workflow_table_path, 'r') as f:
                return json.load(f)
        return {}

    def get_asset_status(self, asset_id: str) -> Dict[str, Any]:
        """
        Get asset status and metadata - implements GET /api/v1/assets/{uuid}
        
        Args:
            asset_id: UUID of the asset
            
        Returns:
            Asset information with status and metadata
        """
        try:
            asset = self.ingest_service.get_asset(asset_id)
            
            response = {
                'uuid': asset.asset_id,
                'video_type': asset.video_type,
                'status': asset.status.value,
                'created_at': asset.created_at,
                'updated_at': asset.updated_at
            }
            
            # Add inspection data if available
            if asset.inspection_data:
                response['inspection_data'] = asdict(asset.inspection_data)
                response['progress'] = self._calculate_progress(asset.status)
            
            # Add renditions if completed
            if asset.status == AssetStatus.COMPLETED:
                # In production, would query for actual renditions
                response['renditions'] = [
                    {
                        'format': 'h264_1080p',
                        'resolution': '1920x1080',
                        'bitrate': 5000000,
                        'url': f'./encoded_bucket/{asset_id[:8]}_h264_1920x1080_medium.mp4'
                    }
                ]
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to get asset status for {asset_id}: {e}")
            return {
                'error': str(e),
                'error_type': type(e).__name__
            }

    def list_assets(self, status_filter: str = None, video_type_filter: str = None,
                   page: int = 1, limit: int = 10) -> Dict[str, Any]:
        """
        List assets with filtering and pagination - implements GET /api/v1/assets
        
        Args:
            status_filter: Filter by status
            video_type_filter: Filter by video type
            page: Page number
            limit: Results per page
            
        Returns:
            Assets list with pagination info
        """
        try:
            return self.ingest_service.list_assets(status_filter, video_type_filter, page, limit)
        except Exception as e:
            logger.error(f"Failed to list assets: {e}")
            return {
                'error': str(e),
                'error_type': type(e).__name__
            }

    def get_system_status(self) -> Dict[str, Any]:
        """
        Get overall system status and metrics
        
        Returns:
            System status information
        """
        try:
            # Get asset statistics
            assets = self.ingest_service.list_assets(limit=1000)  # Get all assets
            total_assets = assets['pagination']['total']
            
            status_counts = {}
            video_type_counts = {}
            
            if assets['assets']:  # Check if assets exist
                for asset_data in assets['assets']:
                    status = asset_data['status']
                    video_type = asset_data['video_type']
                    
                    status_counts[status] = status_counts.get(status, 0) + 1
                    video_type_counts[video_type] = video_type_counts.get(video_type, 0) + 1
            
            # Get worker statistics
            worker_stats = self.encoding_service.get_worker_stats()
            
            # Get rules information
            rules_info = self.rules_engine.list_rules()
            
            return {
                'system_status': 'operational',
                'timestamp': datetime.now().isoformat(),
                'assets': {
                    'total_count': total_assets,
                    'status_breakdown': status_counts,
                    'video_type_breakdown': video_type_counts
                },
                'encoding': worker_stats,
                'rules': {
                    'rule_count': len(rules_info),
                    'rules': rules_info
                },
                'services': {
                    'ingest_service': 'operational',
                    'inspection_service': 'operational',
                    'encoding_service': 'operational',
                    'rules_engine': 'operational'
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'system_status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def _calculate_progress(self, status: AssetStatus) -> int:
        """Calculate progress percentage based on status"""
        progress_map = {
            AssetStatus.UPLOADED: 10,
            AssetStatus.INGESTED: 20,
            AssetStatus.INSPECTED: 40,
            AssetStatus.ENCODING: 70,
            AssetStatus.COMPLETED: 100,
            AssetStatus.FAILED: 0
        }
        return progress_map.get(status, 0)

    # API-style methods for external integration

    def api_ingest_video(self, request_data: Dict) -> Dict:
        """
        API endpoint implementation: POST /api/v1/ingest
        
        Args:
            request_data: Dictionary with s3_url, video_type, metadata
            
        Returns:
            API response
        """
        try:
            video_filename = request_data.get('video_filename')
            video_type = request_data.get('video_type')
            metadata = request_data.get('metadata', {})

            if not video_filename or not video_type:
                return {
                    'error': 'Missing required fields: video_filename, video_type',
                    'status_code': 400
                }

            result = self.process_video(video_filename, video_type, metadata)
            
            if result['status'] == 'completed':
                return {
                    'uuid': result['asset_id'],
                    'status': 'ingested',
                    'source_url': result.get('source_url'),
                    'status_code': 201
                }
            else:
                return {
                    'error': result.get('error', 'Processing failed'),
                    'status_code': 500
                }
                
        except Exception as e:
            logger.error(f"API ingest failed: {e}")
            return {
                'error': str(e),
                'status_code': 500
            }

    def api_get_asset(self, asset_id: str) -> Dict:
        """
        API endpoint implementation: GET /api/v1/assets/{uuid}
        
        Args:
            asset_id: Asset UUID
            
        Returns:
            API response
        """
        try:
            result = self.get_asset_status(asset_id)
            
            if 'error' in result:
                return {
                    'error': result['error'],
                    'status_code': 404 if 'not found' in result['error'].lower() else 500
                }
            else:
                return {
                    **result,
                    'status_code': 200
                }
                
        except Exception as e:
            logger.error(f"API get asset failed: {e}")
            return {
                'error': str(e),
                'status_code': 500
            }

    def api_list_assets(self, query_params: Dict) -> Dict:
        """
        API endpoint implementation: GET /api/v1/assets
        
        Args:
            query_params: Dictionary with status, video_type, page, limit
            
        Returns:
            API response
        """
        try:
            status_filter = query_params.get('status')
            video_type_filter = query_params.get('video_type')
            page = int(query_params.get('page', 1))
            limit = int(query_params.get('limit', 10))
            
            result = self.list_assets(status_filter, video_type_filter, page, limit)
            
            return {
                **result,
                'status_code': 200
            }
            
        except Exception as e:
            logger.error(f"API list assets failed: {e}")
            return {
                'error': str(e),
                'status_code': 500
            }
