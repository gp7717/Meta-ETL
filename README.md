# MetaETL Azure Function

This project is an Azure Function for running a robust, hourly ETL pipeline that fetches, logs, and stores Meta (Facebook) Ads data into a PostgreSQL database. It is designed for reliability, with full error handling, rate limit backoff, and hourly data snapshots.

## Features
- Fetches and upserts data for:
  - Campaigns
  - Ad Account Activity History
  - Regionwise Ad Insights
  - Ad Creatives (with full raw data)
  - Adsets (with full raw data)
  - Ads (with nested targeting/insights and full raw data)
- Handles API rate limits with exponential backoff
- Logs all fetches and errors to `metrics.log`
- Stores all API responses in the database for traceability
- Each ETL step runs independently; one failure does not block others
- Designed for hourly execution (uses IST for all timestamps)

## Setup

### 1. Requirements
- Python 3.8+
- PostgreSQL database
- Azure Functions Core Tools (for local testing/deployment)

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Variables
Set these in a `.env` file or in Azure Function App Settings:
- `DATABASE_URL` (or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
- `FACEBOOK_ACCESS_TOKEN` (Meta API token)
- `FACEBOOK_AD_ACCOUNT_ID` (Meta Ad Account ID)
- `META_API_RATE_LIMIT` (optional, default: 0.5 seconds)

### 4. Local Testing
You can run the ETL locally:
```bash
python MetaETL.py
```
Or test the Azure Function locally:
```bash
func start
```
Then trigger via HTTP (GET/POST).

### 5. Deploy to Azure
- Deploy the `MetaETL-function` folder as a Python Azure Function App.
- Set all environment variables in Azure's Application Settings.
- The function will run on HTTP trigger (can be scheduled with Azure Logic Apps or Timer trigger if desired).

## File Structure
- `MetaETL.py` - Main ETL logic
- `function_app.py` - Azure Function entry point
- `function.json` - Azure Function HTTP trigger config
- `requirements.txt` - Python dependencies
- `metrics.log` - Log file (created at runtime)

## Troubleshooting
- **Database errors:** Ensure your DB schema matches the latest code. Run any needed ALTER TABLE migrations.
- **API rate limits:** The ETL will retry with backoff, but if you hit persistent limits, consider increasing the interval or reducing frequency.
- **Environment variables:** Missing or incorrect values will cause connection/auth errors.



Ran tool
Read file: MetaETL.py
Read file: MetaETL.py
Read file: MetaETL.py
Read file: MetaETL.py
Read file: requirements.txt
Read file: host.json
Read file: httpmetaetl/function.json
Read file: httpmetaetl/function_app.py
Read file: local.settings.json
Here is a comprehensive README for your project, including all tables used in the ETL, setup, local testing, Azure deployment, and configuration details.

---

# MetaETL Azure Function

This project is an Azure Function for running a robust, hourly ETL pipeline that fetches, logs, and stores Meta (Facebook) Ads data into a PostgreSQL database. It is designed for reliability, with full error handling, rate limit backoff, and hourly data snapshots.

---

## Features

- Fetches and upserts data for:
  - Campaigns
  - Ad Account Activity History
  - Regionwise Ad Insights
  - Ad Creatives (with full raw data)
  - Adsets (with full raw data)
  - Ads (with nested targeting/insights and full raw data)
- Handles API rate limits with exponential backoff
- Logs all fetches and errors to `metrics.log`
- Stores all API responses in the database for traceability
- Each ETL step runs independently; one failure does not block others
- Designed for hourly execution (uses IST for all timestamps)

---

## Database Tables Used

The ETL creates and uses the following tables in your PostgreSQL database:

### 1. `dim_campaign`
| Column                  | Type             | Description                        |
|-------------------------|------------------|------------------------------------|
| campaign_id             | VARCHAR(50)      | Primary key                        |
| account_id              | VARCHAR(50)      | Ad account ID                      |
| campaign_name           | VARCHAR(255)     | Campaign name                      |
| objective               | VARCHAR(50)      | Campaign objective                 |
| status                  | VARCHAR(20)      | Campaign status                    |
| buying_type             | VARCHAR(20)      | Buying type                        |
| special_ad_categories   | VARCHAR(100)[]   | Special ad categories              |
| start_time              | TIMESTAMP        | Campaign start time                |
| end_time                | TIMESTAMP        | Campaign end time                  |
| budget_remaining        | NUMERIC          | Remaining budget                   |
| daily_budget            | NUMERIC          | Daily budget                       |
| lifetime_budget         | NUMERIC          | Lifetime budget                    |
| created_at              | TIMESTAMP        | Record creation time               |
| updated_at              | TIMESTAMP        | Record update time                 |
| hour                    | VARCHAR(32)      | Hourly snapshot                    |
| fetched_at              | TIMESTAMP        | Fetch timestamp                    |

### 2. `ad_account_activity_history`
| Column         | Type         | Description                        |
|----------------|--------------|------------------------------------|
| id             | SERIAL       | Primary key                        |
| object_id      | VARCHAR(50)  | Object ID                          |
| object_name    | TEXT         | Object name                        |
| object_type    | VARCHAR(50)  | Object type                        |
| event_type     | VARCHAR(100) | Event type                         |
| changed_fields | TEXT         | Changed fields                     |
| extra_data     | TEXT         | Extra data                         |
| actor_id       | VARCHAR(50)  | Actor ID                           |
| actor_name     | TEXT         | Actor name                         |
| event_time     | TIMESTAMP    | Event time                         |
| hour           | VARCHAR(32)  | Hourly snapshot                    |
| fetched_at     | TIMESTAMP    | Fetch timestamp                    |

### 3. `ad_insights_regionwise`
| Column      | Type         | Description                        |
|-------------|--------------|------------------------------------|
| id          | SERIAL       | Primary key                        |
| ad_id       | VARCHAR(50)  | Ad ID                              |
| impressions | VARCHAR(50)  | Impressions                        |
| clicks      | VARCHAR(50)  | Clicks                             |
| ctr         | VARCHAR(50)  | Click-through rate                 |
| date_start  | VARCHAR(20)  | Start date                         |
| date_stop   | VARCHAR(20)  | End date                           |
| region      | VARCHAR(100) | Region                             |
| hour        | VARCHAR(32)  | Hourly snapshot                    |
| fetched_at  | TIMESTAMP    | Fetch timestamp                    |

### 4. `ad_creatives`
| Column              | Type         | Description                        |
|---------------------|--------------|------------------------------------|
| id                  | VARCHAR(50)  | Creative ID (part of PK)           |
| name                | TEXT         | Creative name                      |
| body                | TEXT         | Creative body                      |
| title               | TEXT         | Creative title                     |
| object_story_spec   | JSONB        | Object story spec                  |
| call_to_action_type | VARCHAR(50)  | Call to action type                |
| hour                | VARCHAR(32)  | Hourly snapshot (part of PK)       |
| fetched_at          | TIMESTAMP    | Fetch timestamp                    |
| raw_data            | JSONB        | Full raw data                      |

### 5. `adsets`
| Column                        | Type         | Description                        |
|-------------------------------|--------------|------------------------------------|
| id                            | VARCHAR(50)  | Adset ID (part of PK)              |
| name                          | TEXT         | Adset name                         |
| campaign_id                   | VARCHAR(50)  | Campaign ID                        |
| account_id                    | VARCHAR(50)  | Account ID                         |
| status                        | VARCHAR(32)  | Status                             |
| optimization_goal             | VARCHAR(64)  | Optimization goal                  |
| billing_event                 | VARCHAR(64)  | Billing event                      |
| bid_strategy                  | VARCHAR(64)  | Bid strategy                       |
| bid_amount                    | NUMERIC      | Bid amount                         |
| daily_budget                  | VARCHAR(64)  | Daily budget                       |
| lifetime_budget               | VARCHAR(64)  | Lifetime budget                    |
| budget_remaining              | VARCHAR(64)  | Budget remaining                   |
| start_time                    | TIMESTAMP    | Start time                         |
| end_time                      | TIMESTAMP    | End time                           |
| created_time                  | TIMESTAMP    | Created time                       |
| updated_time                  | TIMESTAMP    | Updated time                       |
| effective_status              | VARCHAR(32)  | Effective status                   |
| destination_type              | VARCHAR(64)  | Destination type                   |
| learning_stage_info           | JSONB        | Learning stage info                |
| attribution_spec              | JSONB        | Attribution spec                   |
| promoted_object               | JSONB        | Promoted object                    |
| targeting                     | JSONB        | Targeting                          |
| pacing_type                   | JSONB        | Pacing type                        |
| adlabels                      | JSONB        | Ad labels                          |
| bid_adjustments               | JSONB        | Bid adjustments                    |
| bid_constraints               | JSONB        | Bid constraints                    |
| adset_schedule                | JSONB        | Adset schedule                     |
| issues_info                   | JSONB        | Issues info                        |
| creative_sequence             | JSONB        | Creative sequence                  |
| daily_spend_cap               | VARCHAR(64)  | Daily spend cap                    |
| lifetime_spend_cap            | VARCHAR(64)  | Lifetime spend cap                 |
| daily_min_spend_target        | VARCHAR(64)  | Daily min spend target             |
| lifetime_min_spend_target     | VARCHAR(64)  | Lifetime min spend target          |
| is_dynamic_creative           | BOOLEAN      | Is dynamic creative                |
| rf_prediction_id              | VARCHAR(64)  | RF prediction ID                   |
| time_based_ad_rotation_id_blocks | JSONB     | Time-based ad rotation ID blocks   |
| time_based_ad_rotation_intervals | JSONB     | Time-based ad rotation intervals   |
| frequency_control_specs       | JSONB        | Frequency control specs            |
| hour                          | VARCHAR(32)  | Hourly snapshot (part of PK)       |
| fetched_at                    | TIMESTAMP    | Fetch timestamp                    |
| raw_data                      | JSONB        | Full raw data                      |

### 6. `ads`
| Column      | Type         | Description                        |
|-------------|--------------|------------------------------------|
| id          | VARCHAR(50)  | Ad ID (part of PK)                 |
| name        | TEXT         | Ad name                            |
| adset_id    | VARCHAR(50)  | Adset ID                           |
| targeting   | JSONB        | Targeting                          |
| insights    | JSONB        | Insights                           |
| hour        | VARCHAR(32)  | Hourly snapshot (part of PK)       |
| fetched_at  | TIMESTAMP    | Fetch timestamp                    |
| raw_data    | JSONB        | Full raw data                      |

---

## Setup

### 1. Requirements

- Python 3.8+
- PostgreSQL database
- Azure Functions Core Tools (for local testing/deployment)

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment Variables

Set these in a `.env` file, `local.settings.json`, or in Azure Function App Settings:

- `DATABASE_URL` (or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
- `FACEBOOK_ACCESS_TOKEN` (Meta API token)
- `FACEBOOK_AD_ACCOUNT_ID` (Meta Ad Account ID)
- `META_API_RATE_LIMIT` (optional, default: 0.5 seconds)

Example `.env`:
```
DATABASE_URL=postgresql://user:password@host:5432/dbname
FACEBOOK_ACCESS_TOKEN=your_facebook_token
FACEBOOK_AD_ACCOUNT_ID=your_ad_account_id
META_API_RATE_LIMIT=0.5
```

---

## Local Testing

### Run ETL Directly

```bash
python MetaETL.py
```

### Test as Azure Function

1. Start the Azure Functions host locally:
   ```bash
   func start
   ```
2. Trigger the function via HTTP (GET/POST) to the local endpoint.

---

## Azure Deployment

- Deploy the `MetaETL-function` folder as a Python Azure Function App.
- Set all environment variables in Azure's Application Settings.
- The function will run on HTTP trigger (can be scheduled with Azure Logic Apps or Timer trigger if desired).

---

## Azure Function Configuration

- **Entry Point:** `httpmetaetl/function_app.py` (`main_function`)
- **Trigger:** HTTP (GET/POST)
- **Bindings:** See `httpmetaetl/function.json`

Example `function.json`:
```json
{
  "scriptFile": "function_app.py",
  "entryPoint": "main_function",
  "bindings": [
    {
      "authLevel": "function",
      "type": "httpTrigger",
      "direction": "in",
      "name": "req",
      "methods": ["get", "post"]
    },
    {
      "type": "http",
      "direction": "out",
      "name": "$return"
    }
  ]
}
```

---

## File Structure

- `MetaETL.py` - Main ETL logic
- `httpmetaetl/function_app.py` - Azure Function entry point
- `httpmetaetl/function.json` - Azure Function HTTP trigger config
- `requirements.txt` - Python dependencies
- `metrics.log` - Log file (created at runtime)
- `host.json`, `local.settings.json` - Azure Functions configuration

---

## Troubleshooting

- **Database errors:** Ensure your DB schema matches the latest code. Run any needed ALTER TABLE migrations.
- **API rate limits:** The ETL will retry with backoff, but if you hit persistent limits, consider increasing the interval or reducing frequency.
- **Environment variables:** Missing or incorrect values will cause connection/auth errors.
- **SSL/LibreSSL warnings:** If you see warnings about LibreSSL, consider using a Python build linked with OpenSSL for full compatibility.

---

## License

MIT

---
