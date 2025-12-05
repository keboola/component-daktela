# Daktela Extractor

Keboola component for extracting data from Daktela CRM/Contact Center API v6.

## Functionality

This component extracts data from Daktela API v6 endpoints and loads them into Keboola Storage tables. It supports incremental loading, date range filtering, parallel extraction, and automatic data transformation.

### Supported Endpoints

- accounts, activities, activitiesCall, activitiesChat, activitiesEmail, campaignsRecords, contacts, crmRecords, groups, pauses, queues, statuses, templates, tickets, users

## Configuration

The component requires configuration in three sections:

### Connection
- **url**: Daktela instance URL (e.g., `https://yourcompany.daktela.com`)
- **username**: API username
- **#password**: API password (encrypted)
- **verify_ssl**: SSL verification (default: `true`)

### Data Selection
- **date_from**: Start date (formats: `today`, `7 days ago`, etc.)
- **date_to**: End date (formats: `today`, `yesterday`, etc.)
- **endpoints**: Array of endpoint names to extract

### Destination
- **incremental**: Enable incremental loading (default: `false`)
- **batch_size**: Records per batch (default: `1000`)
- **max_concurrent_requests**: Concurrent API requests (default: `10`)

### Debug
- **debug**: Enable debug logging (default: `false`)


## Development

### Local Setup

```bash
git clone https://github.com/keboola/component-daktela
cd component-daktela
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/component.py
```

### Docker

```bash
docker-compose build
docker-compose run --rm dev
docker-compose run --rm test
```

### Code Quality

For detailed documentation on specific features:

- **[Manifest Configuration](docs/MANIFEST_CONFIGURATION.md)**: Learn how to customize manifest generation from table definitions

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
