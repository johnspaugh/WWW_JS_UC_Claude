"""
Video Transcoding Service - Main Application
Entry point for the modular video transcoding service
"""
import os
import json
import logging
from datetime import datetime
from Orchestrator import Orchestrator

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


def get_videos_from_asset_bucket(asset_bucket: str = './asset_bucket'):
    """List video files already present in asset_bucket"""
    if not os.path.exists(asset_bucket):
        logger.warning(f"asset_bucket directory not found: {asset_bucket}")
        return []

    videos = [f for f in os.listdir(asset_bucket) if f.lower().endswith('.mp4')]
    logger.info(f"Found {len(videos)} video(s) in asset_bucket: {videos}")
    return videos


def demo_api_endpoints(orchestrator: Orchestrator, asset_bucket: str = './asset_bucket'):
    """Demonstrate API endpoint functionality using a video already in asset_bucket"""
    print("\nAPI ENDPOINTS DEMONSTRATION")
    print("="*60)

    # Use the first available video in asset_bucket
    available = get_videos_from_asset_bucket(asset_bucket)
    if not available:
        print("No videos in asset_bucket — skipping API demo.")
        return

    demo_filename = available[0]

    # POST /api/v1/ingest
    print(f"\n1. POST /api/v1/ingest  (filename: {demo_filename})")
    ingest_request = {
        'video_filename': demo_filename,
        'video_type': 'movie',
        'metadata': {'title': 'Demo Movie', 'genre': 'action'}
    }
    ingest_response = orchestrator.api_ingest_video(ingest_request)
    print(f"Response: {json.dumps(ingest_response, indent=2)}")

    if 'uuid' in ingest_response:
        asset_id = ingest_response['uuid']

        # GET /api/v1/assets/{uuid}
        print(f"\n2. GET /api/v1/assets/{asset_id}")
        asset_response = orchestrator.api_get_asset(asset_id)
        print(f"Response: {json.dumps(asset_response, indent=2)}")

        # GET /api/v1/assets (list)
        print("\n3. GET /api/v1/assets")
        list_response = orchestrator.api_list_assets({'page': 1, 'limit': 5})
        print(f"Response: {json.dumps(list_response, indent=2)}")


def main():
    """Main application entry point"""
    print("\n" + "="*72)
    print(" VIDEO TRANSCODING SERVICE - MODULAR ARCHITECTURE ")
    print("="*72 + "\n")

    asset_bucket = './asset_bucket'
    temp_bucket = './temp_bucket'
    encoded_bucket = './encoded_bucket'

    try:
        # Initialize orchestrator
        logger.info("Initializing Video Transcoding Service")
        orchestrator = Orchestrator(
            asset_bucket=asset_bucket,
            encoded_bucket=encoded_bucket,
            temp_bucket=temp_bucket
        )

        # Display system status
        system_status = orchestrator.get_system_status()
        print("SYSTEM STATUS:")
        print(f"  Status: {system_status['system_status']}")

        if 'services' in system_status:
            print(f"  Services: {list(system_status['services'].keys())}")
            print(f"  Rules Configured: {system_status['rules']['rule_count']}")
        else:
            print(f"  Error: {system_status.get('error', 'Unknown error')}")
        print()

        # Find videos already in asset_bucket
        available_videos = get_videos_from_asset_bucket(asset_bucket)

        if not available_videos:
            print(f"No .mp4 files found in {asset_bucket}/")
            print("Place video files in asset_bucket/ and re-run.")
            return 1

        print(f"VIDEOS FOUND IN asset_bucket: {available_videos}")
        print("-" * 60)

        results = []

        # Infer video type from filename — default to 'movie'
        def infer_video_type(filename: str) -> str:
            name = filename.lower()
            if 'trailer' in name:
                return 'trailer'
            if 'episode' in name or 'ep' in name:
                return 'tv-episode'
            if 'user' in name or 'content' in name or 'ugc' in name:
                return 'user-content'
            return 'movie'

        for filename in available_videos:
            video_type = infer_video_type(filename)
            print(f"\nProcessing {video_type.upper()}: {filename}")
            print(f"  asset_bucket/{filename} -> temp_bucket/{filename} -> [encode] -> encoded_bucket/")
            print("-" * 40)

            result = orchestrator.process_video(filename, video_type)
            results.append(result)

            if result['status'] == 'completed':
                print(f"  Success")
                print(f"  Asset ID: {result['asset_id']}")
                print(f"  Tasks: {result['task_count']}")
                print(f"  Outputs: {len(result['output_renditions'])}")

                inspection = result['inspection_data']
                print(f"  Resolution: {inspection['resolution']}")
                print(f"  Duration: {inspection['duration']}s")
                print(f"  Codec: {inspection['codec']}")
            else:
                print(f"  Failed: {result.get('error', 'Unknown error')}")

        # Summary
        print(f"\nPROCESSING SUMMARY:")
        print("="*60)
        successful = sum(1 for r in results if r['status'] == 'completed')
        failed = len(results) - successful
        print(f"Total Videos Processed: {len(results)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")

        if successful > 0:
            print(f"\nOUTPUT LOCATIONS (encoded_bucket):")
            for result in results:
                if result['status'] == 'completed':
                    print(f"  Asset {result['asset_id'][:8]}:")
                    for url in result['output_renditions']:
                        print(f"    - {url}")

        # Display final system status
        final_status = orchestrator.get_system_status()
        print(f"\nFINAL SYSTEM STATUS:")
        print(f"  Total Assets: {final_status['assets']['total_count']}")
        print(f"  Status Breakdown: {final_status['assets']['status_breakdown']}")
        print(f"  Video Types: {final_status['assets']['video_type_breakdown']}")

    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\nApplication failed: {e}")
        return 1

    finally:
        print(f"\nDetailed logs available in: transcoding_service.log")
        print("="*72)
        print("Application complete")
        print("="*72)

    return 0


if __name__ == "__main__":
    exit(main())
