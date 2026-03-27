"""
Inspection Service for Video Transcoding Service
Analyzes video technical properties and metadata using FFprobe/MediaInfo
"""
import os
import logging
from datetime import datetime
from models import AssetRecord, AssetStatus, InspectionData
from Ingest_Service import IngestService

logger = logging.getLogger(__name__)


class InspectionService:
    """Inspection Service as specified in document"""
    
    def __init__(self, ingest_service: IngestService):
        self.ingest_service = ingest_service

    def inspect_asset(self, asset_id: str) -> InspectionData:
        """
        Analyzes video using FFprobe/MediaInfo - implements inspection workflow
        
        In production, this would:
        1. Download video from storage
        2. Run FFprobe to extract technical metadata
        3. Store results in asset table
        4. Update asset status to inspected
        
        Args:
            asset_id: UUID of the asset to inspect
            
        Returns:
            InspectionData with technical metadata
        """
        asset = self.ingest_service.get_asset(asset_id)
        
        if asset.status != AssetStatus.INGESTED:
            raise ValueError(f"Asset {asset_id} must be ingested before inspection")
        
        logger.info(f"Starting inspection for asset {asset_id}")
        
        # In real implementation, would run:
        # ffprobe -v quiet -print_format json -show_format -show_streams {asset.source_url}
        inspection_data = self._extract_metadata(asset.source_url)
        
        # Update asset with inspection data
        self.ingest_service.update_asset_status(asset_id, AssetStatus.INSPECTED, inspection_data)
        
        logger.info(f"asset.inspected - {asset_id}: {inspection_data.resolution}, {inspection_data.duration}s")
        
        return inspection_data

    def _extract_metadata(self, video_path: str) -> InspectionData:
        """
        Extract video metadata using FFprobe (simulated)
        
        In production implementation:
        ```python
        import subprocess
        import json
        
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', '-show_streams', video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        probe_data = json.loads(result.stdout)
        
        # Extract video stream
        video_stream = next(s for s in probe_data['streams'] if s['codec_type'] == 'video')
        audio_streams = [s for s in probe_data['streams'] if s['codec_type'] == 'audio']
        
        return InspectionData(
            codec=video_stream['codec_name'],
            resolution=f"{video_stream['width']}x{video_stream['height']}",
            width=int(video_stream['width']),
            height=int(video_stream['height']),
            bitrate=int(probe_data['format'].get('bit_rate', 0)),
            duration=float(probe_data['format']['duration']),
            frame_rate=eval(video_stream['r_frame_rate']),  # Convert "30/1" to 30.0
            color_space=video_stream.get('pix_fmt', 'unknown'),
            audio_tracks=[{
                'codec': stream['codec_name'],
                'channels': stream.get('channels', 0),
                'sample_rate': int(stream.get('sample_rate', 0)),
                'bitrate': int(stream.get('bit_rate', 0))
            } for stream in audio_streams],
            file_size=int(probe_data['format']['size'])
        )
        ```
        """
        
        # Simulated metadata extraction based on file analysis
        file_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
        
        # Simulate different video types based on filename or size
        filename = os.path.basename(video_path).lower()
        
        if 'movie' in filename or file_size > 1000000:  # Large files = movies
            inspection_data = InspectionData(
                codec='h264',
                resolution='1920x1080',
                width=1920,
                height=1080,
                bitrate=5000000,
                duration=7200.0,  # 2 hour movie
                frame_rate=24.0,
                color_space='yuv420p',
                audio_tracks=[{
                    'codec': 'aac',
                    'channels': 6,  # 5.1 surround
                    'sample_rate': 48000,
                    'bitrate': 384000
                }, {
                    'codec': 'ac3',  # Secondary audio track
                    'channels': 2,
                    'sample_rate': 48000,
                    'bitrate': 192000
                }],
                file_size=file_size
            )
        elif 'trailer' in filename:
            inspection_data = InspectionData(
                codec='h264',
                resolution='1920x1080',
                width=1920,
                height=1080,
                bitrate=8000000,  # Higher bitrate for trailer
                duration=150.0,  # 2.5 minute trailer
                frame_rate=24.0,
                color_space='yuv420p',
                audio_tracks=[{
                    'codec': 'aac',
                    'channels': 2,
                    'sample_rate': 48000,
                    'bitrate': 192000
                }],
                file_size=file_size
            )
        elif 'user' in filename or 'content' in filename:
            inspection_data = InspectionData(
                codec='h264',
                resolution='1280x720',
                width=1280,
                height=720,
                bitrate=2500000,
                duration=300.0,  # 5 minute user video
                frame_rate=30.0,
                color_space='yuv420p',
                audio_tracks=[{
                    'codec': 'aac',
                    'channels': 2,
                    'sample_rate': 44100,
                    'bitrate': 128000
                }],
                file_size=file_size
            )
        else:  # Default TV episode
            inspection_data = InspectionData(
                codec='h264',
                resolution='1920x1080',
                width=1920,
                height=1080,
                bitrate=4000000,
                duration=2700.0,  # 45 minute episode
                frame_rate=23.976,  # Broadcast framerate
                color_space='yuv420p',
                audio_tracks=[{
                    'codec': 'aac',
                    'channels': 2,
                    'sample_rate': 48000,
                    'bitrate': 128000
                }],
                file_size=file_size
            )
        
        logger.debug(f"Extracted metadata: {inspection_data.codec} {inspection_data.resolution} @ {inspection_data.bitrate/1000000:.1f}Mbps")
        
        return inspection_data

    def get_inspection_summary(self, asset_id: str) -> dict:
        """
        Get a summary of inspection results for an asset
        
        Args:
            asset_id: UUID of the asset
            
        Returns:
            Dict with inspection summary
        """
        asset = self.ingest_service.get_asset(asset_id)
        
        if not asset.inspection_data:
            return {'status': 'not_inspected'}
        
        data = asset.inspection_data
        return {
            'status': 'inspected',
            'technical_summary': {
                'codec': data.codec,
                'resolution': data.resolution,
                'duration_minutes': round(data.duration / 60, 1),
                'bitrate_mbps': round(data.bitrate / 1000000, 1),
                'frame_rate': data.frame_rate,
                'file_size_mb': round(data.file_size / 1000000, 1)
            },
            'audio_summary': {
                'track_count': len(data.audio_tracks),
                'primary_codec': data.audio_tracks[0]['codec'] if data.audio_tracks else None,
                'channels': data.audio_tracks[0]['channels'] if data.audio_tracks else None
            },
            'quality_indicators': {
                'is_hd': data.height >= 720,
                'is_full_hd': data.height >= 1080,
                'is_4k': data.height >= 2160,
                'has_surround_audio': any(track['channels'] > 2 for track in data.audio_tracks),
                'modern_codec': data.codec in ['h264', 'h265', 'vp9', 'av1']
            }
        }
