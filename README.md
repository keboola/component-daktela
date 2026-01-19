# Daktela Extractor

Keboola component for extracting data from Daktela CRM/Contact Center API v6.

## Functionality

This component extracts data from Daktela API v6 endpoints and loads them into Keboola Storage tables. It supports:

- ✅ **Multiple endpoint extraction** - Configure multiple endpoints with individual field selection
- ✅ **Incremental loading** - Load only new/updated data based on timestamps
- ✅ **Date range filtering** - Filter data by date ranges with flexible date formats
- ✅ **Parallel extraction** - Concurrent requests and endpoint processing for performance
- ✅ **Field discovery** - Sync action to discover available fields for each endpoint
- ✅ **Automatic schema management** - Tracks and persists table schemas across runs
- ✅ **Memory-efficient streaming** - Processes large datasets without loading everything into memory

### Supported Endpoints

- `accounts`, `activities`, `activitiesCall`, `activitiesChat`, `activitiesEmail`
- `campaignsRecords`, `contacts`, `crmRecords`, `groups`, `pauses`
- `queues`, `statuses`, `templates`, `tickets`, `users`

## Configuration

This component uses **configuration rows** for flexible endpoint management. Each row represents one endpoint to extract.

### Global Configuration (Root Level)

#### 1. Connection

```json
{
  "connection": {
    "url": "https://yourcompany.daktela.com",
    "username": "your_username",
    "#password": "your_password",
    "verify_ssl": true
  }
}
```

- **url**: Daktela instance URL
- **username**: API username
- **#password**: API password (automatically encrypted by Keboola)
- **verify_ssl**: SSL certificate verification (default: `true`)

Use the **"Test Connection"** button to verify your credentials before saving.

#### 2. Destination

```json
{
  "destination": {
    "incremental": false
  }
}
```

- **incremental**: Enable incremental loading (appends data instead of replacing)
  - `false` (default): Replace existing tables with new data
  - `true`: Append new data to existing tables

#### 3. Advanced Settings

```json
{
  "advanced": {
    "batch_size": 1000,
    "max_concurrent_requests": 10,
    "max_concurrent_endpoints": 3
  }
}
```

- **batch_size**: Number of records per API page and CSV flush (100-1000, default: 1000)
  - Note: Daktela API max is 1000, higher values are automatically capped
- **max_concurrent_requests**: Max concurrent API requests across all endpoints (1-50, default: 10)
- **max_concurrent_endpoints**: Max endpoints to extract simultaneously (1-20, default: 3)
  - Lower values reduce memory usage but take longer

#### 4. Debug Mode

```json
{
  "debug": true
}
```

- **debug**: Enable detailed debug logging (default: `false`)

### Row Configuration (Per Endpoint)

Each configuration row defines one endpoint to extract:

```json
{
  "endpoint": "contacts",
  "date_from": "7 days ago",
  "date_to": "today",
  "fields": ["name", "email", "phone"]
}
```

- **endpoint**: Name of the API endpoint to extract (required)
- **date_from**: Start date for extraction (required)
  - Formats: `today`, `yesterday`, `5 hours ago`, `3 days ago`, `4 months ago`, `2 years ago`
- **date_to**: End date for extraction (required, same formats as `date_from`)
- **fields**: (Optional) Array of field names to extract
  - If empty or omitted, all available fields will be extracted

#### Field Discovery

Use the **"Discover Available Fields"** sync action to see all available fields for your configured endpoint:

1. Select an endpoint in the row configuration
2. Click "Discover Available Fields" button
3. The component will return a JSON object with all available fields:
   ```json
   {
     "contacts": ["name", "title", "email", "phone", "created", "modified"]
   }
   ```
4. Copy the field names you need into the `fields` array

## Complete Configuration Example

### Root Configuration (parameters)

```json
{
  "connection": {
    "url": "https://democz.daktela.com",
    "username": "agent_420407",
    "#password": "your_password",
    "verify_ssl": true
  },
  "destination": {
    "incremental": false
  },
  "advanced": {
    "batch_size": 1000,
    "max_concurrent_requests": 10,
    "max_concurrent_endpoints": 3
  },
  "debug": false
}
```

### Row Configurations (one per endpoint)

**Row 1 - Contacts:**
```json
{
  "endpoint": "contacts",
  "date_from": "7 days ago",
  "date_to": "today",
  "fields": ["name", "title", "email", "phone"]
}
```

**Row 2 - Tickets:**
```json
{
  "endpoint": "tickets",
  "date_from": "7 days ago",
  "date_to": "today",
  "fields": ["title", "status", "priority", "created"]
}
```

**Row 3 - Users:**
```json
{
  "endpoint": "users",
  "date_from": "30 days ago",
  "date_to": "today"
}
```

## Output Tables

The component creates one output table per endpoint:

- Table name: `{endpoint}.csv` (e.g., `contacts.csv`, `tickets.csv`)
- Primary key: `name` (or `id_call` for `activitiesCall` endpoint)
- Incremental mode: Supported via primary key

## Development

### Local Setup

```bash
# Clone repository
git clone https://github.com/keboola/component-daktela
cd component-daktela

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies with uv
pip install uv
uv sync

# Run locally
python src/component.py
```

### Testing

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=src
```

### Docker

```bash
# Build image
docker-compose build

# Run in dev mode
docker-compose run --rm dev

# Run tests
docker-compose run --rm test
```

## Architecture

### Component Structure

```
src/
├── component.py          # Main component orchestration
├── configuration.py      # Pydantic configuration models
├── daktela_client.py     # Async API client with rate limiting
├── extractor.py          # Extraction logic with streaming
└── __main__.py          # Entry point
```

### Key Features

1. **Async Architecture**
   - Uses `asyncio` for concurrent API requests
   - Rate limiting with `aiolimiter`
   - Connection pooling with `httpx`

2. **Memory-Efficient Streaming**
   - Processes records in batches
   - Streams directly to CSV without loading full datasets
   - Configurable batch sizes

3. **Schema State Management**
   - Tracks table schemas across runs
   - Persists schemas in state file
   - Handles schema evolution gracefully

4. **Robust Error Handling**
   - Retries with exponential backoff
   - Detailed error logging
   - User-friendly error messages

## Integration

For details about deployment and integration with Keboola Connection, refer to the [Keboola Developer Documentation](https://developers.keboola.com/extend/component/deployment/).

## License

MIT License - see [LICENSE.md](LICENSE.md)

## Support

For issues and questions, please use the [GitHub Issues](https://github.com/keboola/component-daktela/issues).
