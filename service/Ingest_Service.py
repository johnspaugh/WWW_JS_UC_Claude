"""
Ingest Service for Video Transcoding Service
Manages video intake, assigns UUIDs, and registers assets in the system
"""
import os
import json
import shutil
import uuid
import logging
from datetime import datetime
from typing import Dict
from dataclasses import asdict
from models import AssetRecord, AssetStatus, InspectionData

logger = logging.getLogger(__name__)


class IngestService:
    """Ingest Service as specified in document"""

    def __init__(self, asset_bucket: str = './asset_bucket', asset_table_path: str = './asset_table.json',
                 temp_bucket: str = './temp_bucket'):
        self.asset_bucket = asset_bucket
        self.asset_table_path = asset_table_path
        self.temp_bucket = temp_bucket
        os.makedirs(asset_bucket, exist_ok=True)
        os.makedirs(temp_bucket, exist_ok=True)

    def ingest_video(self, video_filename: str, video_type: str, metadata: Dict = None) -> AssetRecord:
        """
        Initiates video ingest workflow - implements POST /api/v1/ingest

        Expects the video to already exist in asset_bucket. Copies it into
        temp_bucket as a working copy for inspection and encoding.

        Args:
            video_filename: Filename of the video already in asset_bucket
            video_type: Content classification (movie, tv-episode, trailer, user-content)
            metadata: Optional additional metadata

        Returns:
            AssetRecord with generated UUID, asset_bucket URL, and temp_bucket URL
        """
        asset_id = str(uuid.uuid4())

        # Validate source exists in asset_bucket
        source_url = os.path.join(self.asset_bucket, video_filename)
        if not os.path.exists(source_url):
            raise FileNotFoundError(f"Video not found in asset_bucket: {source_url}")

        # Copy to temp_bucket as the working copy for encoding
        temp_url = os.path.join(self.temp_bucket, video_filename)
        shutil.copy2(source_url, temp_url)

        # Create asset record — source_url stays in asset_bucket (original untouched)
        asset = AssetRecord(
            asset_id=asset_id,
            video_type=video_type,
            source_url=source_url,
            temp_url=temp_url,
            status=AssetStatus.INGESTED
        )

        self._save_asset(asset)
        logger.info(f"asset.ingested - {asset_id} from asset_bucket/{video_filename} as {video_type}")
        logger.info(f"Working copy created: {temp_url}")

        return asset

    def get_asset(self, asset_id: str) -> AssetRecord:
        """
        Get asset by ID - implements GET /api/v1/assets/{uuid}
        
        Args:
            asset_id: UUID of the asset
            
        Returns:
            AssetRecord with current status and metadata
        """
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
            temp_url=asset_data.get('temp_url'),
            status=AssetStatus(asset_data['status']),
            inspection_data=inspection_data,
            created_at=asset_data['created_at'],
            updated_at=asset_data['updated_at']
        )

    def update_asset_status(self, asset_id: str, status: AssetStatus, inspection_data: InspectionData = None):
        """
        Update asset status and optionally add inspection data
        
        Args:
            asset_id: UUID of the asset
            status: New status
            inspection_data: Optional inspection results
        """
        asset = self.get_asset(asset_id)
        asset.status = status
        asset.updated_at = datetime.now().isoformat()
        
        if inspection_data:
            asset.inspection_data = inspection_data
            
        self._save_asset(asset)
        logger.info(f"Asset {asset_id} status updated to {status.value}")

    def list_assets(self, status_filter: str = None, video_type_filter: str = None, 
                   page: int = 1, limit: int = 10) -> Dict:
        """
        List assets with filtering and pagination - implements GET /api/v1/assets
        
        Args:
            status_filter: Filter by status
            video_type_filter: Filter by video type
            page: Page number (1-based)
            limit: Results per page
            
        Returns:
            Dict with assets array and pagination info
        """
        assets = self._load_assets()
        filtered_assets = []
        
        for asset_data in assets.values():
            # Apply filters
            if status_filter and asset_data['status'] != status_filter:
                continue
            if video_type_filter and asset_data['video_type'] != video_type_filter:
                continue
                
            filtered_assets.append(asset_data)
        
        # Sort by created_at descending
        filtered_assets.sort(key=lambda x: x['created_at'], reverse=True)
        
        # Apply pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        page_assets = filtered_assets[start_idx:end_idx]
        
        return {
            'assets': page_assets,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': len(filtered_assets),
                'has_next': end_idx < len(filtered_assets),
                'has_prev': page > 1
            }
        }

    def _save_asset(self, asset: AssetRecord):
        """Save asset to asset table (simulates database)"""
        assets = self._load_assets()
        
        # Convert to dict with proper serialization
        asset_dict = {
            'asset_id': asset.asset_id,
            'video_type': asset.video_type,
            'source_url': asset.source_url,
            'temp_url': asset.temp_url,
            'status': asset.status.value,  # Convert enum to string
            'inspection_data': asdict(asset.inspection_data) if asset.inspection_data else None,
            'created_at': asset.created_at,
            'updated_at': asset.updated_at
        }
        
        assets[asset.asset_id] = asset_dict
        
        with open(self.asset_table_path, 'w') as f:
            json.dump(assets, f, indent=2)

    def _load_assets(self) -> Dict:
        """Load assets from asset table (simulates database query)"""
        if os.path.exists(self.asset_table_path):
            with open(self.asset_table_path, 'r') as f:
                return json.load(f)
        return {}
