# Galaxy Digital MongoDB Sync

A Python tool to synchronize data from the Galaxy Digital API to a MongoDB database, with support for incremental updates, data aggregation, and a REST API for integration.

## Features

- Full and incremental data synchronization from Galaxy Digital API
- MongoDB storage with proper indexing
- Data type validation and normalization
- Activity report generation and aggregation
- Shift status tracking with check-in/check-out analysis
- REST API for integration with other programs
- Support for scheduled syncs
- Comprehensive error handling and retry logic

## Prerequisites

- Python 3.8+
- MongoDB instance (local or cloud)
- Galaxy Digital API credentials

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd galaxy_digital_mongo_sync
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file from the example:
```bash
cp .example.env .env
```

5. Edit `.env` with your credentials:
```
GALAXY_API_KEY=your_api_key_here
GALAXY_EMAIL=your_email@example.com
GALAXY_PASSWORD=your_password_here
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=galaxy_digital
```

## Usage

### Command Line Interface

#### One-time sync:
```bash
python galaxy_api_sync.py
```

#### Scheduled sync (runs every hour by default):
```bash
# Set environment variable to run continuously
export SYNC_INTERVAL_MINUTES=60
python galaxy_api_sync.py
```

#### Generate reports only:
```bash
export GENERATE_REPORTS=true
python galaxy_api_sync.py
```

#### Generate specific report:
```bash
export GENERATE_SPECIFIC_REPORT=shift_status
python galaxy_api_sync.py
```

Available report types:
- `user` - User activity summary
- `needs` or `opportunity` - Opportunity activity
- `agency` - Agency activity
- `time` - Time-based activity
- `shift_status` - Shift status with check-in/out tracking
- `checkin_analysis` - Check-in/check-out analysis for pending hours

### REST API

The FastAPI provides a REST interface for integrating with other programs.

#### Starting the API Server

```bash
# Run the API server
python api.py

# Or with uvicorn directly
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

#### API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

#### Key Endpoints

##### Health Check
```bash
GET /health
```

##### Sync Operations
```bash
# Trigger full sync of all resources
POST /sync/all

# Sync specific resource
POST /sync/resource
{
  "resource_name": "hours",
  "params": {},
  "since_field": "since_updated"
}
```

##### Report Generation
```bash
# Generate specific report
POST /reports/generate
{
  "report_type": "shift_status",
  "sync_first": true
}

# Generate all reports
POST /reports/generate-all?sync_first=true
```

##### Query Data
```bash
# Query any collection
POST /query
{
  "collection": "shift_status",
  "filter": {"need_id": 12345},
  "projection": {"title": 1, "users": 1},
  "limit": 10,
  "skip": 0
}

# Get shifts by check-in status
GET /shifts/checkin-status?status=checked_in_only&limit=50

# Get users pending checkout
GET /users/pending-checkout?limit=100
```

##### Statistics
```bash
# Get summary statistics
GET /stats/summary

# Get last sync times
GET /metadata/last-sync
```

#### Example API Usage (Python)

```python
import requests

# Base URL
base_url = "http://localhost:8000"

# Check health
response = requests.get(f"{base_url}/health")
print(response.json())

# Trigger full sync
response = requests.post(f"{base_url}/sync/all")
print(response.json())

# Generate shift status report
response = requests.post(
    f"{base_url}/reports/generate",
    json={"report_type": "shift_status", "sync_first": True}
)
print(response.json())

# Query users who checked in but not out
response = requests.get(
    f"{base_url}/shifts/checkin-status",
    params={"status": "checked_in_only", "limit": 50}
)
data = response.json()
print(f"Found {data['count']} users pending checkout")
```

#### Example API Usage (JavaScript/Node.js)

```javascript
const axios = require('axios');

const baseUrl = 'http://localhost:8000';

// Check health
async function checkHealth() {
    const response = await axios.get(`${baseUrl}/health`);
    console.log(response.data);
}

// Get users pending checkout
async function getPendingCheckouts() {
    const response = await axios.get(`${baseUrl}/users/pending-checkout`);
    console.log(`Found ${response.data.count} users pending checkout`);
    return response.data.data;
}

// Generate report
async function generateReport(reportType) {
    const response = await axios.post(`${baseUrl}/reports/generate`, {
        report_type: reportType,
        sync_first: true
    });
    console.log(response.data);
}
```

#### Example API Usage (cURL)

```bash
# Check health
curl http://localhost:8000/health

# Trigger sync
curl -X POST http://localhost:8000/sync/all

# Generate shift status report
curl -X POST http://localhost:8000/reports/generate \
  -H "Content-Type: application/json" \
  -d '{"report_type": "shift_status", "sync_first": true}'

# Get pending checkouts
curl "http://localhost:8000/users/pending-checkout?limit=50"

# Query collection
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "collection": "shift_status",
    "filter": {"users.checkout_status": "checked_in_only"},
    "limit": 20
  }'
```

## Configuration

Edit `config.json` to customize:
- API endpoints
- MongoDB connection settings
- Sync intervals
- Resources to sync

## Environment Variables

- `GALAXY_API_KEY` - Your Galaxy Digital API key (required)
- `GALAXY_EMAIL` - Your Galaxy Digital email (required)
- `GALAXY_PASSWORD` - Your Galaxy Digital password (required)
- `MONGODB_URI` - MongoDB connection string
- `MONGODB_DATABASE` - Database name (default: galaxy_digital)
- `SYNC_ONCE` - Set to "true" for one-time sync
- `SYNC_INTERVAL_MINUTES` - Minutes between syncs (default: 60)
- `GENERATE_REPORTS` - Set to "true" to only generate reports
- `GENERATE_SPECIFIC_REPORT` - Generate a specific report type
- `VALIDATE_DATA_TYPES` - Set to "true" to validate and repair data types

## Data Collections

The sync creates the following MongoDB collections:

### Primary Collections
- `agencies` - Organization data
- `users` - Volunteer user data
- `needs` - Volunteer opportunities
- `hours` - Logged volunteer hours
- `responses` - Shift sign-ups
- `qualifications` - User qualifications
- `teams` - Team data

### Aggregated Collections
- `user_activity_summary` - Per-user activity metrics
- `opportunity_activity` - Per-opportunity metrics
- `agency_activity` - Per-agency metrics
- `monthly_activity` - Time-based activity patterns
- `shift_status` - Real-time shift and attendance tracking
- `checkin_checkout_analysis` - Analysis of pending hours with check-in/out patterns

## Query Tool

The included query tool allows you to explore the synced data:

```bash
# Show sync status
python query-tool.py --status

# List available collections
python query-tool.py --list

# Query a specific collection
python query-tool.py --collection users --query '{"user_status": "active"}' --limit 5

# Run predefined analysis
python query-tool.py --analyze needs
```

## Production Deployment

### Using Docker

Create a `Dockerfile`:
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# For API server
CMD ["python", "api.py"]

# For sync service (uncomment to use)
# CMD ["python", "galaxy_api_sync.py"]
```

Build and run:
```bash
docker build -t galaxy-sync-api .
docker run -p 8000:8000 --env-file .env galaxy-sync-api
```

### Using systemd

Create `/etc/systemd/system/galaxy-sync-api.service`:
```ini
[Unit]
Description=Galaxy Digital Sync API
After=network.target mongodb.service

[Service]
Type=simple
User=yourusername
WorkingDirectory=/path/to/galaxy_digital_mongo_sync
Environment="PATH=/path/to/venv/bin"
ExecStart=/path/to/venv/bin/python api.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable galaxy-sync-api
sudo systemctl start galaxy-sync-api
```

## Troubleshooting

1. **Authentication errors**: Verify your credentials in `.env`
2. **MongoDB connection issues**: Check your MongoDB URI and ensure the database is running
3. **Rate limiting**: The tool handles rate limits automatically with exponential backoff
4. **Missing data**: Check the Galaxy Digital API permissions for your account
5. **API server issues**: Check logs with `journalctl -u galaxy-sync-api -f` if using systemd

## License

MIT License 