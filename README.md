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

## License
MIT 