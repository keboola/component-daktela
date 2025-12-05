This component extracts data from the Daktela CRM/Contact Center API v6 and loads it into Keboola Storage tables.

### Supported Endpoints

The component can extract data from the following Daktela API endpoints:

- **accounts** - Account information
- **activities** - General activity records
- **activitiesCall** - Phone call activities
- **activitiesChat** - Chat conversation activities
- **activitiesEmail** - Email activities
- **campaignsRecords** - Campaign records
- **contacts** - Customer and contact information
- **crmRecords** - CRM records
- **groups** - User groups
- **pauses** - Pause records
- **queues** - Queue information
- **statuses** - Status definitions
- **templates** - Template configurations
- **tickets** - Support tickets and cases
- **users** - User accounts

### Key Features

- **Incremental loading** with automatic state management for efficient data synchronization
- **Date range filtering** to extract data for specific time periods
- **Parallel extraction** of multiple endpoints with configurable concurrency limits