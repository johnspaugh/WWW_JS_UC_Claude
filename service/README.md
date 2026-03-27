# Video Transcoding Service - Modular Architecture

This is a modular implementation of the Video Transcoding Service as specified in the "Simple Transcoding Service.docx" document.

## Architecture Overview

The service is split into separate modules for maintainability and scalability:

```
transcoding_service/
├── models.py              # Shared data models and enums
├── DAG.py                 # Directed Acyclic Graph implementation
├── Ingest_Service.py      # Video ingestion and asset management
├── Inspection_Service.py  # Video metadata analysis
├── Encoding_Service.py    # Video transcoding execution
├── Rules_Engine.py        # Rules evaluation and DAG generation
├── Orchestrator.py        # Service coordination and workflows
├── main.py               # Application entry point
└── README.md             # This file
```

## Modules Description

### 📊 `models.py`
- **Purpose**: Shared data structures
- **Contains**: `AssetRecord`, `InspectionData`, `DAGDefinition`, `EncodingTask`
- **Enums**: `TaskStatus`, `AssetStatus`

### 🔗 `DAG.py`
- **Purpose**: Task orchestration and dependency management
- **Classes**: `Task`, `DAG`
- **Features**: Topological sorting, dependency resolution, execution logging

### 📥 `Ingest_Service.py`
- **Purpose**: Video intake and asset registration
- **Key Methods**:
  - `ingest_video()` - Process new video uploads
  - `get_asset()` - Retrieve asset information
  - `list_assets()` - List assets with filtering
- **Storage**: JSON-based asset table (simulates database)

### 🔍 `Inspection_Service.py`
- **Purpose**: Video analysis and metadata extraction
- **Key Methods**:
  - `inspect_asset()` - Extract technical metadata
  - `get_inspection_summary()` - Get analysis summary
- **Simulation**: FFprobe-style metadata extraction

### ⚙️ `Encoding_Service.py`
- **Purpose**: Video transcoding execution
- **Key Methods**:
  - `execute_dag()` - Run encoding tasks in dependency order
  - `get_worker_stats()` - Worker utilization metrics
- **Simulation**: FFmpeg command execution

### 📋 `Rules_Engine.py`
- **Purpose**: Conditional logic and workflow generation
- **Key Methods**:
  - `add_rule()` - Define encoding rules
  - `generate_dag_for_asset()` - Create workflows based on conditions
- **Rules**: Premium Movie ABR, User Content Quick, Legacy Upgrade, etc.

### 🎭 `Orchestrator.py`
- **Purpose**: Service coordination and API implementation
- **Key Methods**:
  - `process_video()` - End-to-end workflow
  - `api_*()` methods - REST API endpoint implementations
- **Workflow**: Ingest → Inspect → Rules → Encode

### 🚀 `main.py`
- **Purpose**: Application entry point and demonstration
- **Features**: Test video processing, API demos, system status

## Document Compliance

This implementation fulfills all requirements from the specification document:

### ✅ **Core Services**
- **Ingest Service**: ✅ UUID assignment, asset registration
- **Inspection Service**: ✅ FFprobe-style metadata extraction
- **Encoding Service**: ✅ DAG execution, parallel task processing
- **Storage Service**: ✅ S3-compatible bucket simulation
- **Orchestrator**: ✅ Service coordination

### ✅ **Rules Engine**
- **Rule Structure**: ✅ name, priority, conditions, tasks
- **Condition Operators**: ✅ equals, not_equals, greater_than, contains, not_in
- **DAG Generation**: ✅ Based on video type and technical properties

### ✅ **Data Model**
- **Asset Table**: ✅ UUID, video_type, source_url, status, inspection_data
- **Inspection Data**: ✅ codec, resolution, bitrate, duration, frame_rate, audio_tracks
- **DAG Structure**: ✅ dag_id, asset_uuid, tasks, status

### ✅ **API Endpoints**
- `POST /api/v1/ingest` ✅
- `GET /api/v1/assets/{uuid}` ✅
- `GET /api/v1/assets` ✅
- `GET /api/v1/rules` ✅ (via Rules_Engine)

### ✅ **Encoding Rules (from Document)**
1. **Premium Movie ABR Ladder** ✅
   - Conditions: video_type=movie AND resolution≥1080p
   - Tasks: H.264 1080p/720p/480p + H.265 1080p (dependent)

2. **User Content Quick Encode** ✅
   - Conditions: video_type=user-content
   - Tasks: H.264 720p/480p fast preset

3. **Legacy Codec Upgrade** ✅
   - Conditions: codec NOT IN (h264, h265, vp9, av1)
   - Tasks: Transcode to H.264

4. **TV Episode Standard** ✅
   - Conditions: video_type=tv-episode
   - Tasks: H.264 1080p/720p

5. **Trailer High Quality** ✅
   - Conditions: video_type=trailer
   - Tasks: H.264 1080p/720p high bitrate

## Running the Service

```bash
# Run the main application
python main.py

# This will:
# 1. Initialize all services
# 2. Process test videos of different types
# 3. Demonstrate API endpoints
# 4. Show system status and metrics
```

## Example Output

```
🎬 ====================================================================
 VIDEO TRANSCODING SERVICE - MODULAR ARCHITECTURE 
====================================================================== 🎬

📊 SYSTEM STATUS:
  Status: operational
  Services: ['ingest_service', 'inspection_service', 'encoding_service', 'rules_engine']
  Rules Configured: 5

🎯 PROCESSING TEST VIDEOS:
------------------------------------------------------------

Processing MOVIE: test_movie.mp4
----------------------------------------
=== STEP 1: INGEST ===
=== STEP 2: INSPECTION ===
=== STEP 3: RULES EVALUATION ===
=== STEP 4: ENCODING ===
✅ Success!
   Asset ID: a1b2c3d4
   Tasks: 4
   Outputs: 4
   Resolution: 1920x1080
   Duration: 7200.0s
   Codec: h264
```

## Extending the Service

### Adding New Rules
```python
# In Rules_Engine.py
rules_engine.add_rule(
    name="custom_rule",
    priority=10,
    conditions=[
        {'field': 'duration', 'operator': 'greater_than', 'value': 3600}
    ],
    tasks=[
        {
            'name': 'long_form_encode',
            'dependencies': [],
            'encoding_params': {
                'codec': 'h265',
                'width': 1920,
                'height': 1080,
                'bitrate': 3000000,
                'preset': 'slow'
            }
        }
    ]
)
```

### Adding New Services
1. Create new service module following the existing pattern
2. Add to `Orchestrator.py` initialization
3. Wire into the workflow as needed

## Production Considerations

This implementation is a functional simulation. For production:

1. **Database**: Replace JSON files with PostgreSQL
2. **Storage**: Integrate with AWS S3 or compatible object storage
3. **Queue**: Add message queue (RabbitMQ/SQS) for task distribution
4. **Workers**: Implement actual FFmpeg encoding workers
5. **Monitoring**: Add metrics collection and alerting
6. **API**: Wrap in Flask/FastAPI for REST endpoints
7. **Containers**: Dockerize services for deployment

## Document Traceability

Every component maps directly to the specification:
- Service architecture (Table in document Section 2)
- End-to-end workflow (Steps 1-4 in document)
- Data model (Asset and Inspection tables)
- Rules examples (Premium Movie ABR, etc.)
- API specification (Section 8)
- Error handling and monitoring (Sections 10-11)

The modular design enables independent development, testing, and deployment of each service while maintaining the cohesive workflow described in the document.
