"""
Rules Engine for Video Transcoding Service
Evaluates video type and inspection results to generate DAGs
"""
import uuid
import logging
from typing import Dict, List, Callable
from models import AssetRecord, EncodingTask, DAGDefinition

logger = logging.getLogger(__name__)


class RulesEngine:
    """Rules Engine as specified in document - evaluates conditions and generates DAGs"""
    
    def __init__(self):
        self.rules = []

    def add_rule(self, name: str, priority: int, conditions: List[Dict], 
                 tasks: List[Dict], dag_generator: Callable = None):
        """
        Add a rule as specified in document
        
        Args:
            name: Unique rule identifier
            priority: Integer (lower number = higher priority)
            conditions: Array of conditions that must ALL be met
            tasks: Array of transcoding task definitions
            dag_generator: Optional custom DAG generation function
        """
        self.rules.append({
            'name': name,
            'priority': priority,
            'conditions': conditions,
            'tasks': tasks,
            'dag_generator': dag_generator
        })
        # Sort by priority (lower number = higher priority)
        self.rules.sort(key=lambda x: x['priority'])
        logger.info(f"Added rule: {name} (priority {priority})")

    def evaluate_conditions(self, conditions: List[Dict], asset: AssetRecord) -> bool:
        """
        Evaluate if all conditions are met
        
        Supports operators: equals, not_equals, greater_than, less_than, contains
        
        Args:
            conditions: List of condition dictionaries
            asset: Asset record with inspection data
            
        Returns:
            True if all conditions match
        """
        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            
            # Get actual value from asset
            actual = self._get_field_value(field, asset)
            if actual is None:
                logger.debug(f"Field {field} not available for condition evaluation")
                return False
            
            # Apply operator
            if not self._apply_operator(operator, actual, value):
                logger.debug(f"Condition failed: {field} {operator} {value} (actual: {actual})")
                return False
        
        logger.debug(f"All conditions met for asset {asset.asset_id}")
        return True

    def _get_field_value(self, field: str, asset: AssetRecord):
        """Extract field value from asset record"""
        if field == 'video_type':
            return asset.video_type
        elif field == 'resolution_height' and asset.inspection_data:
            return asset.inspection_data.height
        elif field == 'resolution_width' and asset.inspection_data:
            return asset.inspection_data.width
        elif field == 'codec' and asset.inspection_data:
            return asset.inspection_data.codec
        elif field == 'duration' and asset.inspection_data:
            return asset.inspection_data.duration
        elif field == 'bitrate' and asset.inspection_data:
            return asset.inspection_data.bitrate
        elif field == 'frame_rate' and asset.inspection_data:
            return asset.inspection_data.frame_rate
        elif field == 'file_size' and asset.inspection_data:
            return asset.inspection_data.file_size
        elif field == 'audio_channels' and asset.inspection_data and asset.inspection_data.audio_tracks:
            return max(track['channels'] for track in asset.inspection_data.audio_tracks)
        else:
            return None

    def _apply_operator(self, operator: str, actual, value) -> bool:
        """Apply comparison operator"""
        if operator == 'equals':
            return actual == value
        elif operator == 'not_equals':
            return actual != value
        elif operator == 'greater_than':
            return actual > value
        elif operator == 'less_than':
            return actual < value
        elif operator == 'greater_equal':
            return actual >= value
        elif operator == 'less_equal':
            return actual <= value
        elif operator == 'contains':
            return value in str(actual)
        elif operator == 'not_in':
            return actual not in value  # value should be a list
        else:
            logger.warning(f"Unknown operator: {operator}")
            return False

    def generate_dag_for_asset(self, asset: AssetRecord) -> DAGDefinition:
        """
        Generate DAG based on rules evaluation
        
        Args:
            asset: Asset record with inspection data
            
        Returns:
            DAGDefinition with encoding tasks
        """
        # Find first matching rule (rules are sorted by priority)
        for rule in self.rules:
            if self.evaluate_conditions(rule['conditions'], asset):
                logger.info(f"Rule matched: {rule['name']} for asset {asset.asset_id}")
                
                # Use custom DAG generator if provided
                if rule.get('dag_generator'):
                    return rule['dag_generator'](asset, rule)
                
                # Generate encoding tasks based on rule
                encoding_tasks = []
                task_name_to_id = {}  # Map task names to generated IDs
                
                # First pass: create tasks and build name mapping
                for task_def in rule['tasks']:
                    task_id = f"{task_def['name']}_{asset.asset_id[:8]}"
                    task_name_to_id[task_def['name']] = task_id
                    
                    task = EncodingTask(
                        task_id=task_id,
                        dependencies=[],  # Will set in second pass
                        encoding_params=task_def['encoding_params']
                    )
                    encoding_tasks.append(task)
                
                # Second pass: resolve dependencies to actual task IDs
                for i, task_def in enumerate(rule['tasks']):
                    resolved_deps = []
                    for dep_name in task_def.get('dependencies', []):
                        if dep_name in task_name_to_id:
                            resolved_deps.append(task_name_to_id[dep_name])
                        else:
                            logger.warning(f"Dependency {dep_name} not found for task {task_def['name']}")
                    encoding_tasks[i].dependencies = resolved_deps
                
                dag_def = DAGDefinition(
                    dag_id=str(uuid.uuid4()),
                    asset_uuid=asset.asset_id,
                    tasks=encoding_tasks
                )
                
                logger.info(f"dag.created - {dag_def.dag_id} with {len(encoding_tasks)} tasks for rule {rule['name']}")
                return dag_def
        
        # No matching rule - create default
        logger.warning(f"No matching rule for asset {asset.asset_id}, using default DAG")
        return self._create_default_dag(asset)

    def _create_default_dag(self, asset: AssetRecord) -> DAGDefinition:
        """Create default encoding DAG when no rules match"""
        default_task = EncodingTask(
            task_id=f"default_encode_{asset.asset_id[:8]}",
            dependencies=[],
            encoding_params={
                'codec': 'h264',
                'width': 1280,
                'height': 720,
                'bitrate': 2500000,
                'preset': 'medium',
                'container': 'mp4'
            }
        )
        
        dag_def = DAGDefinition(
            dag_id=str(uuid.uuid4()),
            asset_uuid=asset.asset_id,
            tasks=[default_task],
            status="pending"
        )
        
        logger.info(f"dag.created - {dag_def.dag_id} (default) for asset {asset.asset_id}")
        return dag_def

    def list_rules(self) -> List[Dict]:
        """List all configured rules - implements GET /api/v1/rules"""
        return [
            {
                'name': rule['name'],
                'priority': rule['priority'],
                'conditions': rule['conditions'],
                'task_count': len(rule['tasks'])
            }
            for rule in self.rules
        ]

    def get_rule(self, rule_name: str) -> Dict:
        """Get specific rule by name"""
        for rule in self.rules:
            if rule['name'] == rule_name:
                return rule
        raise ValueError(f"Rule {rule_name} not found")

    def remove_rule(self, rule_name: str):
        """Remove a rule by name - implements DELETE /api/v1/rules/{rule_id}"""
        original_count = len(self.rules)
        self.rules = [rule for rule in self.rules if rule['name'] != rule_name]
        
        if len(self.rules) == original_count:
            raise ValueError(f"Rule {rule_name} not found")
        
        logger.info(f"Rule removed: {rule_name}")

    def update_rule(self, rule_name: str, priority: int = None, conditions: List[Dict] = None, 
                   tasks: List[Dict] = None):
        """Update an existing rule - implements PUT /api/v1/rules/{rule_id}"""
        for rule in self.rules:
            if rule['name'] == rule_name:
                if priority is not None:
                    rule['priority'] = priority
                if conditions is not None:
                    rule['conditions'] = conditions
                if tasks is not None:
                    rule['tasks'] = tasks
                
                # Re-sort by priority
                self.rules.sort(key=lambda x: x['priority'])
                logger.info(f"Rule updated: {rule_name}")
                return
        
        raise ValueError(f"Rule {rule_name} not found")


def setup_encoding_rules(rules_engine: RulesEngine):
    """Setup encoding rules as specified in document"""
    
    # Rule: Premium Movie ABR Ladder
    # Conditions: video_type equals movie AND resolution >= 1080p
    rules_engine.add_rule(
        name="premium_movie_abr",
        priority=1,
        conditions=[
            {'field': 'video_type', 'operator': 'equals', 'value': 'movie'},
            {'field': 'resolution_height', 'operator': 'greater_equal', 'value': 1080}
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
                    'preset': 'medium',
                    'container': 'mp4'
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
                    'preset': 'medium',
                    'container': 'mp4'
                }
            },
            {
                'name': 'h264_480p',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 854,
                    'height': 480,
                    'bitrate': 1500000,
                    'preset': 'medium',
                    'container': 'mp4'
                }
            },
            {
                'name': 'h265_1080p',
                'dependencies': ['h264_1080p'],  # Will be converted to full task ID
                'encoding_params': {
                    'codec': 'h265',
                    'width': 1920,
                    'height': 1080,
                    'bitrate': 3500000,
                    'preset': 'medium',
                    'container': 'mp4'
                }
            }
        ]
    )
    
    # Rule: User Content Quick Encode
    # Conditions: video_type equals user-content
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
                    'preset': 'fast',
                    'container': 'mp4'
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
                    'preset': 'fast',
                    'container': 'mp4'
                }
            }
        ]
    )
    
    # Rule: Legacy Codec Upgrade
    # Conditions: codec NOT IN (h264, h265, vp9, av1)
    rules_engine.add_rule(
        name="legacy_codec_upgrade",
        priority=3,
        conditions=[
            {'field': 'codec', 'operator': 'not_in', 'value': ['h264', 'h265', 'vp9', 'av1']}
        ],
        tasks=[
            {
                'name': 'modernize_codec',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': -1,  # Maintain source resolution
                    'height': -1,
                    'bitrate': -1,  # Maintain source bitrate
                    'preset': 'medium',
                    'container': 'mp4'
                }
            }
        ]
    )
    
    # Rule: TV Episode Standard Encoding
    # Conditions: video_type equals tv-episode
    rules_engine.add_rule(
        name="tv_episode_standard",
        priority=4,
        conditions=[
            {'field': 'video_type', 'operator': 'equals', 'value': 'tv-episode'}
        ],
        tasks=[
            {
                'name': 'h264_1080p_tv',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1920,
                    'height': 1080,
                    'bitrate': 4000000,
                    'preset': 'medium',
                    'container': 'mp4'
                }
            },
            {
                'name': 'h264_720p_tv',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1280,
                    'height': 720,
                    'bitrate': 2500000,
                    'preset': 'medium',
                    'container': 'mp4'
                }
            }
        ]
    )
    
    # Rule: Trailer High Quality
    # Conditions: video_type equals trailer
    rules_engine.add_rule(
        name="trailer_high_quality",
        priority=5,
        conditions=[
            {'field': 'video_type', 'operator': 'equals', 'value': 'trailer'}
        ],
        tasks=[
            {
                'name': 'h264_1080p_hq',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1920,
                    'height': 1080,
                    'bitrate': 8000000,  # Higher bitrate for trailers
                    'preset': 'slow',     # Better quality preset
                    'container': 'mp4'
                }
            },
            {
                'name': 'h264_720p_hq',
                'dependencies': [],
                'encoding_params': {
                    'codec': 'h264',
                    'width': 1280,
                    'height': 720,
                    'bitrate': 5000000,
                    'preset': 'slow',
                    'container': 'mp4'
                }
            }
        ]
    )
    
    logger.info("Encoding rules setup complete - 5 rules configured")
