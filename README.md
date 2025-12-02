Daktela Extractor
=================

Keboola component for extracting data from Daktela CRM/Contact Center API v6.

**Table of Contents:**

[TOC]

Description
===========

The Daktela Extractor component integrates with the Daktela CRM/Contact Center API v6 to extract specified data and produce CSV output files compatible with Keboola storage. The component supports:

- **Token-based authentication** with configurable SSL verification
- **State management** for true incremental processing
- **Date range filtering** with multiple format support
- **Parallel extraction** for independent tables with concurrency limiting
- **Sequential extraction** for dependent tables
- **Incremental loading** with automatic state tracking
- **Batched processing** for memory-efficient extraction of large datasets
- **Automatic retry logic** with linear backoff
- **Data transformation** and flattening
- **Support for list columns** and nested data structures
- **Automatic manifest generation** from table definitions
- **Component-level error handling** with proper cleanup

Prerequisites
=============

You need:
- Valid Daktela account credentials (username and password)
- Access to a Daktela instance (e.g., mycompany.daktela.com)
- Configured table definitions in `image_parameters`

Features
========

| **Feature**             | **Description**                               |
|-------------------------|-----------------------------------------------|
| Token Authentication    | Secure token-based authentication             |
| State Management        | Automatic state tracking for incremental runs |
| Date Range Filter       | Specify date range for data retrieval         |
| Incremental Loading     | Append new data with state-based filtering    |
| Parallel Extraction     | Extract independent tables concurrently       |
| Concurrency Limiting    | Configurable limit on concurrent API requests |
| Batched Processing      | Memory-efficient processing for large datasets|
| Dependent Tables        | Support for parent-child table relationships  |
| Data Transformation     | Flatten nested JSON and normalize columns     |
| Retry Logic             | Automatic retry with linear backoff           |
| List Handling           | Support for list columns and list of dicts    |
| Automatic Manifests     | Generate manifests from table definitions     |
| SSL Configuration       | Configurable SSL certificate verification     |
| Error Handling          | Component-level error handling with cleanup   |

Configuration
=============

Authentication
--------------

- **Username** (required): Daktela account username
- **Password** (required): Daktela account password (encrypted)
- **Server** (required): Server identifier (e.g., `democz`)
  - The component will construct the URL as `https://{server}.daktela.com`

Date Range
----------

- **From** (required): Start date for extraction
  - Formats: `today` or `0` (current datetime - 30 min), negative integer (today - N days), or `YYYY-MM-DD`
  - Example: `-7` (7 days ago)

- **To** (required): End date for extraction
  - Same formats as `from`
  - Must be at least one day after `from`
  - Example: `today`

Tables
------

- **Tables** (required): Comma-separated list of tables to extract
  - Example: `contacts,activities,tickets`

- **Incremental** (optional): Enable incremental loading (default: `false`)
  - `true`: Append new data to existing data and save state for next run
  - `false`: Replace existing data
  - When enabled, the component saves the current timestamp after successful extraction
  - On subsequent runs, this timestamp is used as the `from_date` to fetch only new data

Performance & Security
----------------------

- **Verify SSL** (optional): Enable SSL certificate verification (default: `true`)
  - `true`: Verify SSL certificates (recommended for production)
  - `false`: Disable SSL verification (for testing with self-signed certificates only)
  - **WARNING**: Disabling SSL verification is insecure and should only be used for testing

- **Max Concurrent Requests** (optional): Maximum concurrent API requests (default: `10`)
  - Range: 1-50
  - Lower this value if experiencing API rate limiting
  - Higher values may improve performance but can overwhelm the API

- **Batch Size** (optional): Records to process in each batch (default: `1000`)
  - Range: 100-100,000
  - Higher values use more memory but may be faster
  - Lower values use less memory and are better for very large tables
  - Recommended: 1000-10000 for most use cases

Debug
-----

- **Debug** (optional): Enable verbose debug logging (default: `false`)

Image Parameters
================

The component uses `image_parameters` in `config.json` to define table configurations. Each table configuration includes:

**Data Extraction:**
- **fields**: List of field names to extract
- **primary_keys**: Primary key fields for building compound IDs
- **secondary_keys**: Secondary key fields for compound IDs
- **keys**: Key fields to prefix with server name
- **no_prefix_columns**: Columns that should not be prefixed with server name
- **list_columns**: Columns containing lists to explode into rows
- **list_of_dicts_columns**: Columns containing lists of dicts to flatten
- **filters**: Additional API filters
- **requirements**: Tables that must be extracted first
- **parent_table**: For dependent tables, the parent table name
- **parent_id_field**: For dependent tables, the ID field in parent table

**Manifest Generation (Optional):**
- **manifest_destination**: Override destination table name (defaults to `server_tablename`)
- **manifest_primary_key**: Override primary key columns (defaults to `["id"]`)
- **manifest_metadata**: Add metadata to manifest file (e.g., KBC descriptions)

Project-Specific Configuration
-------------------------------

The component supports project-specific overrides. Configuration structure:

```json
{
  "image_parameters": {
    "default": {
      "table_name": { /* table config */ }
    },
    "PROJECT_ID": {
      "table_name": { /* project-specific overrides */ }
    }
  }
}
```

See `component_config/image_parameters_example.json` for a complete example.

Output
======

The component produces CSV files with the following structure:

- **Filename**: `{server}_{table_name}.csv`
- **Location**: `data/out/tables/`
- **Format**: UTF-8 encoded, comma-separated, no header
- **Manifest**: JSON manifest file with columns, primary_key, and incremental flag

Output Columns
--------------

Every output row includes:

1. **server**: Server name (e.g., `mycompany`)
2. **id**: Compound primary key (concatenation of primary and secondary key values)
3. **data columns**: All configured fields

Key columns (primary_keys, secondary_keys, keys) are automatically prefixed with the server name unless listed in `no_prefix_columns`.

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-daktela component-daktela
cd component-daktela
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Documentation
=============

For detailed documentation on specific features:

- **[Manifest Configuration](docs/MANIFEST_CONFIGURATION.md)**: Learn how to customize manifest generation from table definitions

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
