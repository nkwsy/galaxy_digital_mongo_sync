# Galaxy Digital API Sync Tool for MongoDB

This tool syncs data from the Galaxy Digital API to a local MongoDB database, allowing you to:

- Maintain a local copy of your Galaxy Digital data
- Perform historical analysis and reporting
- Build custom applications on top of the data
- Reduce API calls to the Galaxy Digital service

## Features

- **Incremental Sync**: Only fetches data that has changed since the last sync
- **Scheduled Operation**: Runs automatically at configurable intervals
- **Robust Error Handling**: Logs errors and continues operation
- **Flexible Configuration**: Easily configure which resources to sync
- **Query Tool**: Includes a utility script for querying and analyzing the synced data

## Requirements

- Python 3.7+
- MongoDB (local or remote)
- Galaxy Digital API access token

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-username/galaxy-api-sync.git
   cd galaxy-api-sync
   ```

2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Copy the example environment file and update it with your credentials:
   ```
   cp .env.example .env
   ```
   
   Edit the `.env` file and add your Galaxy Digital API token.

4. Update the configuration file if needed:
   ```
   cp config.json.example config.json
   ```

## Configuration

### Environment Variables

The following environment variables can be set in the `.env` file:

- `GALAXY_API_TOKEN` - Your Galaxy Digital API token (required)
- `MONGODB_URI` - MongoDB connection URI (default: `mongodb://localhost:27017/`)
- `MONGODB_DATABASE` - MongoDB database name (default: `galaxy_digital`)
- `SYNC_INTERVAL_MINUTES` - Interval between syncs in minutes (default: `60`)
- `SYNC_ONCE` - Set to `true` to run once and exit (default: `false`)

### Configuration File

The `config.json` file contains settings for the sync process:

```json
{
  "api_base_url": "https://api.galaxydigital.com/api",
  "mongodb_uri": "mongodb://localhost:27017/",
  "mongodb_database": "galaxy_digital",
  "resources": [
    {
      "name": "agencies",
      "since_field": "since_updated",
      "params": {
        "per_page": 100,
        "show_inactive": "Yes"
      }
    },
    ...
  ]
}
```

- `api_base_url` - The base URL for the Galaxy Digital API
- `mongodb_uri` - MongoDB connection URI (overrides environment variable)
- `mongodb_database` - MongoDB database name (overrides environment variable)
- `resources` - List of resources to sync:
  - `name` - Resource name/endpoint
  - `since_field` - Field to use for incremental sync
  - `params` - Additional parameters to pass to the API

## Usage

### Running the Sync Tool

To start the sync tool with the default hourly schedule:

```
python galaxy_api_sync.py
```

To run a one-time sync and exit:

```
SYNC_ONCE=true python galaxy_api_sync.py
```

### Using the Query Tool

The included query tool allows you to check the synced data:

```
# Show sync status
python query_data.py --status

# List available collections
python query_data.py --list

# Query a specific collection
python query_data.py --collection users --query '{"user_status": "active"}' --limit 5

# Run predefined analysis
python query_data.py --analyze needs
```

## MongoDB Collections

The sync tool creates the following collections in MongoDB:

- `agencies` - Organizations
- `users` - User profiles
- `needs` - Volunteer opportunities
- `events` - Events
- `hours` - Volunteer hours
- `responses` - Volunteer responses to needs
- `sync_metadata` - Tracks sync status and timing

Each document includes:
- All fields from the Galaxy Digital API
- `_synced_at` - When the document was last synced
- `_sync_source` - Source of the sync (always "galaxy_api")

## Aggregation Reports

The sync tool includes powerful MongoDB aggregation pipelines that generate detailed activity reports. These reports provide valuable insights into volunteer activity, participation patterns, and impact metrics.

### Available Reports

The tool generates the following aggregated collections:

- `user_activity_summary` - Comprehensive metrics for each volunteer:
  - Total hours logged
  - Number of shifts attended
  - List of opportunities participated in
  - First and last activity dates
  - Average hours per shift
  - Days since last activity

- `opportunity_activity` - Metrics for each volunteer opportunity:
  - Total hours logged
  - Number of unique volunteers
  - Average hours per volunteer
  - First and last activity dates
  - Monthly breakdown of hours

- `agency_activity` - Metrics for each agency:
  - Total volunteer hours
  - Number of unique volunteers
  - Number of opportunities
  - Most active opportunities
  - First and last activity dates

- `monthly_activity` - Time-based activity patterns:
  - Monthly totals of hours, volunteers, and opportunities
  - Average hours per volunteer by month
  - Trends over time

### Running Aggregations

You can generate these reports in several ways:

1. **During scheduled sync**: Reports are automatically generated after each sync cycle
   ```
   # Set in .env file
   INCLUDE_REPORTS=true
   ```

2. **Generate reports only** (without syncing data):
   ```
   GENERATE_REPORTS=true python galaxy-api-sync.py
   ```

3. **Generate a specific report**:
   ```
   GENERATE_SPECIFIC_REPORT=user python galaxy-api-sync.py
   ```
   
   Available options: `user`, `needs`, `opportunity`, `agency`, `time`

### Using Aggregated Data

The aggregated collections are optimized for analytics and reporting. You can query them directly from MongoDB or use the query tool:

```bash
# View top volunteers by hours
python query_data.py --collection user_activity_summary --sort '{"total_hours": -1}' --limit 10

# View most popular opportunities
python query_data.py --collection opportunity_activity --sort '{"volunteer_count": -1}' --limit 5

# View monthly trends
python query_data.py --collection monthly_activity --sort '{"_id": 1}'
```

### Custom Aggregations

You can extend the tool with your own custom aggregations by adding new methods to the `GalaxyAPISync` class. Follow the pattern of the existing aggregation methods.

## Troubleshooting

### Logs

The sync tool logs to both the console and a file named `galaxy_sync.log`:

```
tail -f galaxy_sync.log
```

### Common Issues

**API Token Errors**
- Check that your API token is correctly set in the `.env` file
- Verify the token has not expired

**MongoDB Connection Issues**
- Ensure MongoDB is running
- Check the connection string in your configuration

**Sync Not Completing**
- Check the logs for errors
- For large datasets, increase the sync interval

## Advanced Usage

### Customizing Sync Behavior

You can customize the sync behavior for specific resources by modifying the `config.json` file. For example, if you want to sync only active users:

```json
{
  "name": "users",
  "since_field": "since_updated",
  "params": {
    "per_page": 100,
    "user_status": "active"
  }
}
```

### MongoDB Schema Design

The tool preserves the original structure from the Galaxy Digital API in MongoDB, with a few additions:

- `_synced_at`: Timestamp when the record was last synced
- `_sync_source`: Always "galaxy_api" to identify the source of the data
- Original `id` field is maintained and indexed for quick lookups

### Running in Production

For production use, consider:

1. **Set up as a service**: Use systemd or similar to run the tool as a background service
   ```
   # /etc/systemd/system/galaxy-sync.service
   [Unit]
   Description=Galaxy Digital API Sync
   After=network.target mongodb.service
   
   [Service]
   User=yourusername
   WorkingDirectory=/path/to/galaxy-sync
   ExecStart=/usr/bin/python3 /path/to/galaxy-sync/galaxy_api_sync.py
   Restart=on-failure
   
   [Install]
   WantedBy=multi-user.target
   ```

2. **Docker container**: For containerized deployments
   ```
   # Dockerfile
   FROM python:3.9-slim
   
   WORKDIR /app
   
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   
   COPY . .
   
   CMD ["python", "galaxy_api_sync.py"]
   ```

3. **Monitoring**: Add alerts for sync failures

### Data Analysis Examples

The query tool supports various analyses. Here are some examples:

#### Volunteer Hours Summary
```bash
python query_data.py --analyze hours
```

This provides monthly volunteer hour totals.

#### User Activity Analysis
```bash
python query_data.py --collection users --query '{"created_at": {"$gt": "2023-01-01"}}' --limit 10
```

This shows users created since January 2023.

#### Agency Need Analytics
```bash
python query_data.py --analyze needs
```

This breaks down needs by status and shows the top agencies by need count.

### Extending the Tool

You can extend the sync tool in several ways:

1. **Add new resources**: Update the `config.json` file to include additional API endpoints.

2. **Custom transformations**: Modify the `_update_document` method to transform data before storing.

3. **Data normalization**: Create additional collections that normalize the data for specific reporting needs.

4. **Export capabilities**: Add functions to export synchronized data to CSV or other formats.

5. **Web interface**: Build a simple web dashboard on top of the MongoDB data.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
