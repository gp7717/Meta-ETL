import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date
import json
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse
import pytz
import re
import time

# Configure logging
logging.basicConfig(filename='metrics.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Parse Supabase database URL or use individual variables
DB_URL = os.getenv('DATABASE_URL')
if DB_URL:
    parsed_url = urlparse(DB_URL)
    DB_CONFIG = {
        'host': parsed_url.hostname or os.getenv('DB_HOST'),
        'port': parsed_url.port or int(os.getenv('DB_PORT', 5432)),
        'database': parsed_url.path.lstrip('/') or os.getenv('DB_NAME'),
        'user': parsed_url.username or os.getenv('DB_USER'),
        'password': parsed_url.password or os.getenv('DB_PASSWORD')
    }
else:
    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

logger.info(f"Database configuration: host={DB_CONFIG['host']}, port={DB_CONFIG['port']}, database={DB_CONFIG['database']}, user={DB_CONFIG['user']}")

# Meta API configuration
BASE_URL = "https://graph.facebook.com/v22.0"
ACCESS_TOKEN = os.getenv('FACEBOOK_ACCESS_TOKEN')
ACCOUNT_ID = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

# Rate limiting interval (seconds)
RATE_LIMIT_INTERVAL = float(os.getenv('META_API_RATE_LIMIT', 0.5))

# Campaign schema
def validate_schema(data, schema):
    errors = {}
    for field, field_schema in schema.items():
        if field_schema.get('required', False) and field not in data:
            errors.setdefault('required', []).append(f"Missing required field: {field}")
    for field, value in data.items():
        if field in schema:
            field_schema = schema[field]
            expected_type = field_schema.get('type')
            if value is None and field_schema.get('nullable', False):
                continue
            if expected_type == 'string':
                if isinstance(value, date):
                    continue
                elif not isinstance(value, str):
                    errors.setdefault('type', []).append(f"Field '{field}' should be a string, got {type(value).__name__}")
            elif expected_type == 'array' and not isinstance(value, list):
                errors.setdefault('type', []).append(f"Field '{field}' should be an array, got {type(value).__name__}")
            if expected_type == 'string' and isinstance(value, str) and 'format' in field_schema:
                format_type = field_schema['format']
                if format_type == 'datetime':
                    try:
                        if value.endswith('Z'):
                            value = value[:-1] + '+00:00'
                        if value.endswith('+0000'):
                            value = value[:-5] + '+00:00'
                        datetime.fromisoformat(value)
                    except Exception:
                        errors.setdefault('format', []).append(f"Field '{field}' should be a valid datetime")
    return errors

CAMPAIGN_SCHEMA = {
    'id': {'type': 'string', 'required': True},
    'name': {'type': 'string', 'required': True},
    'status': {'type': 'string', 'required': True},
    'objective': {'type': 'string', 'nullable': True},
    'buying_type': {'type': 'string', 'nullable': True},
    'special_ad_categories': {'type': 'array', 'nullable': True},
    'start_time': {'type': 'string', 'nullable': True, 'format': 'datetime'},
    'end_time': {'type': 'string', 'nullable': True, 'format': 'datetime'}
}

# --- BEGIN: Ad Insights Hourly Snapshot Schema and Table ---
AD_INSIGHTS_HOURLY_SNAPSHOT_SCHEMA = {
    'snapshot_hour': {'type': 'string', 'required': True, 'format': 'datetime'},
    'ad_id': {'type': 'string', 'required': True},
    'adset_id': {'type': 'string', 'required': True},
    'campaign_id': {'type': 'string', 'required': True},
    'account_id': {'type': 'string', 'required': True},
    'date_start': {'type': 'string', 'required': True, 'format': 'date'},
    'date_stop': {'type': 'string', 'required': True, 'format': 'date'},
    'clicks': {'type': 'integer', 'nullable': True},
    'impressions': {'type': 'integer', 'nullable': True},
    'spend': {'type': 'number', 'nullable': True},
    'reach': {'type': 'integer', 'nullable': True},
    'page_engagement': {'type': 'integer', 'nullable': True},
    'post_engagement': {'type': 'integer', 'nullable': True},
    'video_view': {'type': 'integer', 'nullable': True},
    'landing_page_view': {'type': 'integer', 'nullable': True},
    'purchase': {'type': 'integer', 'nullable': True},
    'add_to_cart': {'type': 'integer', 'nullable': True},
    'link_click': {'type': 'integer', 'nullable': True},
    'post_reaction': {'type': 'integer', 'nullable': True},
    'outbound_click': {'type': 'integer', 'nullable': True},
    'purchase_value': {'type': 'number', 'nullable': True},
    'view_content_value': {'type': 'number', 'nullable': True},
    'add_to_cart_value': {'type': 'number', 'nullable': True},
    'ctr': {'type': 'number', 'nullable': True},
    'cpp': {'type': 'number', 'nullable': True},
    'video_p25_watched': {'type': 'integer', 'nullable': True},
    'video_thruplay_watched': {'type': 'integer', 'nullable': True}
}

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS dim_campaign (
        campaign_id VARCHAR(50) PRIMARY KEY,
        account_id VARCHAR(50) NOT NULL,
        campaign_name VARCHAR(255) NOT NULL,
        objective VARCHAR(50),
        status VARCHAR(20),
        buying_type VARCHAR(20),
        special_ad_categories VARCHAR(100)[],
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        budget_remaining NUMERIC,
        daily_budget NUMERIC,
        lifetime_budget NUMERIC,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ad_account_activity_history (
        id SERIAL PRIMARY KEY,
        object_id VARCHAR(50),
        object_name TEXT,
        object_type VARCHAR(50),
        event_type VARCHAR(100),
        changed_fields TEXT,
        extra_data TEXT,
        actor_id VARCHAR(50),
        actor_name TEXT,
        event_time TIMESTAMP,
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(object_id, event_type, event_time, hour)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ad_insights_regionwise (
        id SERIAL PRIMARY KEY,
        ad_id VARCHAR(50),
        impressions VARCHAR(50),
        clicks VARCHAR(50),
        ctr VARCHAR(50),
        date_start VARCHAR(20),
        date_stop VARCHAR(20),
        region VARCHAR(100),
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ad_id, date_start, date_stop, region, hour)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ad_creatives (
        id VARCHAR(50),
        name TEXT,
        body TEXT,
        title TEXT,
        object_story_spec JSONB,
        call_to_action_type VARCHAR(50),
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_data JSONB,
        PRIMARY KEY (id, hour)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS adsets (
        id VARCHAR(50),
        name TEXT,
        campaign_id VARCHAR(50),
        account_id VARCHAR(50),
        status VARCHAR(32),
        optimization_goal VARCHAR(64),
        billing_event VARCHAR(64),
        bid_strategy VARCHAR(64),
        bid_amount NUMERIC,
        daily_budget VARCHAR(64),
        lifetime_budget VARCHAR(64),
        budget_remaining VARCHAR(64),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        created_time TIMESTAMP,
        updated_time TIMESTAMP,
        effective_status VARCHAR(32),
        destination_type VARCHAR(64),
        learning_stage_info JSONB,
        attribution_spec JSONB,
        promoted_object JSONB,
        targeting JSONB,
        pacing_type JSONB,
        adlabels JSONB,
        bid_adjustments JSONB,
        bid_constraints JSONB,
        adset_schedule JSONB,
        issues_info JSONB,
        creative_sequence JSONB,
        daily_spend_cap VARCHAR(64),
        lifetime_spend_cap VARCHAR(64),
        daily_min_spend_target VARCHAR(64),
        lifetime_min_spend_target VARCHAR(64),
        is_dynamic_creative BOOLEAN,
        rf_prediction_id VARCHAR(64),
        time_based_ad_rotation_id_blocks JSONB,
        time_based_ad_rotation_intervals JSONB,
        frequency_control_specs JSONB,
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_data JSONB,
        PRIMARY KEY (id, hour)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ads (
        id VARCHAR(50),
        name TEXT,
        adset_id VARCHAR(50),
        targeting JSONB,
        insights JSONB,
        hour VARCHAR(32),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_data JSONB,
        PRIMARY KEY (id, hour)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ad_insights_hourly_snapshots (
        id SERIAL PRIMARY KEY,
        snapshot_hour TIMESTAMP NOT NULL,
        ad_id VARCHAR(50) NOT NULL,
        adset_id VARCHAR(50),
        campaign_id VARCHAR(50),
        account_id VARCHAR(50),
        date_start DATE,
        date_stop DATE,
        clicks INTEGER,
        impressions INTEGER,
        spend NUMERIC,
        reach INTEGER,
        page_engagement INTEGER,
        post_engagement INTEGER,
        video_view INTEGER,
        landing_page_view INTEGER,
        purchase INTEGER,
        add_to_cart INTEGER,
        link_click INTEGER,
        post_reaction INTEGER,
        outbound_click INTEGER,
        purchase_value NUMERIC,
        view_content_value NUMERIC,
        add_to_cart_value NUMERIC,
        ctr NUMERIC,
        cpp NUMERIC,
        video_p25_watched INTEGER,
        video_thruplay_watched INTEGER,
        roas NUMERIC GENERATED ALWAYS AS (CASE WHEN spend > 0 THEN purchase_value / spend ELSE 0 END) STORED,
        calc_ctr NUMERIC GENERATED ALWAYS AS (CASE WHEN impressions > 0 THEN clicks::NUMERIC / impressions * 100 ELSE 0 END) STORED,
        calc_cpp NUMERIC GENERATED ALWAYS AS (CASE WHEN purchase > 0 THEN spend / purchase ELSE 0 END) STORED,
        calc_cpr NUMERIC GENERATED ALWAYS AS (CASE WHEN reach > 0 THEN spend / reach ELSE 0 END) STORED,
        hook_rate NUMERIC GENERATED ALWAYS AS (CASE WHEN impressions > 0 THEN page_engagement::NUMERIC / impressions * 100 ELSE 0 END) STORED,
        hold_rate NUMERIC GENERATED ALWAYS AS (CASE WHEN page_engagement > 0 THEN video_view::NUMERIC / page_engagement * 100 ELSE 0 END) STORED,
        conversion_rate NUMERIC GENERATED ALWAYS AS (CASE WHEN landing_page_view > 0 THEN purchase::NUMERIC / landing_page_view * 100 ELSE 0 END) STORED,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(snapshot_hour, ad_id, date_start, date_stop)
    )
    """
]

def setup_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
        for sql in CREATE_TABLES_SQL:
            cursor.execute(sql)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to set up database: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def api_request_with_backoff(request_func, *args, **kwargs):
    max_retries = 5
    delay = RATE_LIMIT_INTERVAL
    for attempt in range(max_retries):
        try:
            return request_func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            try:
                error_json = e.response.json()
                code = error_json.get('error', {}).get('code')
            except Exception:
                code = None
            if code == 80004:  # Rate limit error
                logger.warning(f"Rate limit hit (attempt {attempt+1}/{max_retries}), retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise
    logger.error(f"API rate limit error after {max_retries} retries, skipping step.")
    return None

def fetch_campaigns():
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/campaigns"
    params = {
        "fields": "id,name,status,objective,buying_type,special_ad_categories,start_time,end_time",
        "access_token": ACCESS_TOKEN
    }
    def normalize_datetime(dt_str):
        if not isinstance(dt_str, str):
            return dt_str
        if dt_str.endswith('Z'):
            return dt_str[:-1] + '+00:00'
        match = re.match(r'(.*)([+-]\d{2})(\d{2})$', dt_str)
        if match:
            base, hour, minute = match.groups()
            return f'{base}{hour}:{minute}'
        return dt_str
    try:
        response = api_request_with_backoff(requests.get, url, params=params)
        logger.info(f"Fetched campaigns raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json().get("data", [])
        valid_campaigns = []
        now_ist = datetime.now(IST)
        hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
        for item in data:
            logger.info(f"Fetched campaign data: {json.dumps(item)}")
            if 'start_time' in item and item['start_time']:
                item['start_time'] = normalize_datetime(item['start_time'])
            if 'end_time' in item and item['end_time']:
                item['end_time'] = normalize_datetime(item['end_time'])
            errors = validate_schema(item, CAMPAIGN_SCHEMA)
            if errors:
                logger.warning(f"Raw start_time for campaign {item.get('id', 'unknown')}: {repr(item.get('start_time', None))}")
                logger.warning(f"Raw end_time for campaign {item.get('id', 'unknown')}: {repr(item.get('end_time', None))}")
                logger.warning(f"Schema validation failed for campaign {item.get('id', 'unknown')}: {errors}")
            else:
                logger.info(f"Valid campaign: id={item['id']}, name={item['name']}")
                valid_campaigns.append((
                    str(item['id']),
                    ACCOUNT_ID,
                    item['name'],
                    item.get('objective', None),
                    item['status'],
                    item.get('buying_type', None),
                    item.get('special_ad_categories', None),
                    item.get('start_time', None),
                    item.get('end_time', None),
                    item.get('budget_remaining', None),
                    item.get('daily_budget', None),
                    item.get('lifetime_budget', None),
                    now_ist,
                    now_ist,
                    hour_str,
                    now_ist
                ))
        logger.info(f"Fetched {len(valid_campaigns)} valid campaigns")
        return valid_campaigns
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to fetch campaigns: {e}")
        return []

def fetch_activity_history(since, until):
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/activities"
    params = {
        "fields": "object_id,object_name,object_type,event_type,changed_fields,extra_data,actor_id,actor_name,event_time",
        "filtering": json.dumps([{"field": "event_type", "operator": "IN", "value": ["UPDATE", "CREATE"]}]),
        "since": since,
        "until": until,
        "access_token": ACCESS_TOKEN
    }
    try:
        response = api_request_with_backoff(requests.get, url, params=params)
        logger.info(f"Fetched activity history raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json().get("data", [])
        valid_activities = []
        now_ist = datetime.now(IST)
        hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
        for item in data:
            logger.info(f"Fetched activity data: {json.dumps(item)}")
            valid_activities.append((
                item.get('object_id'),
                item.get('object_name'),
                item.get('object_type'),
                item.get('event_type'),
                item.get('changed_fields'),
                item.get('extra_data'),
                item.get('actor_id'),
                item.get('actor_name'),
                normalize_activity_datetime(item.get('event_time')),
                hour_str,
                now_ist
            ))
        logger.info(f"Fetched {len(valid_activities)} valid activities")
        return valid_activities
    except Exception as e:
        logger.error(f"Failed to fetch activity history: {e}")
        return []

def normalize_activity_datetime(dt_str):
    if not isinstance(dt_str, str):
        return dt_str
    if dt_str.endswith('Z'):
        return dt_str[:-1] + '+00:00'
    match = re.match(r'(.*)([+-]\d{2})(\d{2})$', dt_str)
    if match:
        base, hour, minute = match.groups()
        return f'{base}{hour}:{minute}'
    return dt_str

def upsert_activity_history(activities, conn):
    if not activities:
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    query = """
    INSERT INTO ad_account_activity_history (
        object_id, object_name, object_type, event_type, changed_fields, extra_data, actor_id, actor_name, event_time, hour, fetched_at
    ) VALUES %s
    ON CONFLICT (object_id, event_type, event_time, hour) DO UPDATE SET
        object_name=EXCLUDED.object_name,
        object_type=EXCLUDED.object_type,
        changed_fields=EXCLUDED.changed_fields,
        extra_data=EXCLUDED.extra_data,
        actor_id=EXCLUDED.actor_id,
        actor_name=EXCLUDED.actor_name,
        fetched_at=EXCLUDED.fetched_at
    """
    try:
        execute_values(cursor, query, activities)
        conn.commit()
        logger.info(f"Upserted {len(activities)} activity history records")
    except Exception as e:
        logger.error(f"Failed to upsert activity history: {str(e)}")
        raise
    finally:
        cursor.close()

def fetch_regionwise_insights():
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/insights"
    params = {
        "level": "ad",
        "date_preset": "today",
        "breakdowns": "region",
        "fields": "ad_id,impressions,clicks,ctr",
        "access_token": ACCESS_TOKEN
    }
    try:
        response = api_request_with_backoff(requests.get, url, params=params)
        logger.info(f"Fetched regionwise insights raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json().get("data", [])
        valid_rows = []
        now_ist = datetime.now(IST)
        hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
        for item in data:
            logger.info(f"Fetched regionwise insight: {json.dumps(item)}")
            valid_rows.append((
                item.get('ad_id'),
                item.get('impressions'),
                item.get('clicks'),
                item.get('ctr'),
                item.get('date_start'),
                item.get('date_stop'),
                item.get('region'),
                hour_str,
                now_ist
            ))
        logger.info(f"Fetched {len(valid_rows)} regionwise insights")
        return valid_rows
    except Exception as e:
        logger.error(f"Failed to fetch regionwise insights: {e}")
        return []

def upsert_regionwise_insights(rows, conn):
    if not rows:
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    query = """
    INSERT INTO ad_insights_regionwise (
        ad_id, impressions, clicks, ctr, date_start, date_stop, region, hour, fetched_at
    ) VALUES %s
    ON CONFLICT (ad_id, date_start, date_stop, region, hour) DO UPDATE SET
        impressions=EXCLUDED.impressions,
        clicks=EXCLUDED.clicks,
        ctr=EXCLUDED.ctr,
        fetched_at=EXCLUDED.fetched_at
    """
    try:
        execute_values(cursor, query, rows)
        conn.commit()
        logger.info(f"Upserted {len(rows)} regionwise insights")
    except Exception as e:
        logger.error(f"Failed to upsert regionwise insights: {str(e)}")
        raise
    finally:
        cursor.close()

def fetch_adcreatives_batch():
    url = f"https://graph.facebook.com/v18.0/act_{ACCOUNT_ID}/adcreatives"
    params = {
        "fields": "id,name,body,title,object_story_spec,call_to_action_type",
        "access_token": ACCESS_TOKEN,
        "limit": 100
    }
    all_creatives = []
    now_ist = datetime.now(IST)
    hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
    while url:
        def do_request():
            return requests.get(url, params=params)
        response = api_request_with_backoff(do_request)
        if response is None:
            break
        logger.info(f"Fetched adcreatives batch raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json()
        for item in data.get("data", []):
            logger.info(f"Fetched adcreative: {json.dumps(item)}")
            all_creatives.append((
                item.get('id'),
                item.get('name'),
                item.get('body'),
                item.get('title'),
                json.dumps(item.get('object_story_spec')) if item.get('object_story_spec') is not None else None,
                item.get('call_to_action_type'),
                hour_str,
                now_ist,
                json.dumps(item)
            ))
        url = data.get("paging", {}).get("next")
        params = {}  # Clear params for next page (URL already has them)
    logger.info(f"Fetched {len(all_creatives)} adcreatives in total")
    return all_creatives

def upsert_adcreatives(creatives, conn):
    if not creatives:
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    query = """
    INSERT INTO ad_creatives (
        id, name, body, title, object_story_spec, call_to_action_type, hour, fetched_at, raw_data
    ) VALUES %s
    ON CONFLICT (id, hour) DO UPDATE SET
        name=EXCLUDED.name,
        body=EXCLUDED.body,
        title=EXCLUDED.title,
        object_story_spec=EXCLUDED.object_story_spec,
        call_to_action_type=EXCLUDED.call_to_action_type,
        fetched_at=EXCLUDED.fetched_at,
        raw_data=EXCLUDED.raw_data
    """
    try:
        execute_values(cursor, query, creatives)
        conn.commit()
        logger.info(f"Upserted {len(creatives)} adcreatives")
    except Exception as e:
        logger.error(f"Failed to upsert adcreatives: {str(e)}")
        raise
    finally:
        cursor.close()

def fetch_adsets_batch():
    url = f"https://graph.facebook.com/v20.0/act_{ACCOUNT_ID}/adsets"
    params = {
        "fields": "optimization_goal,updated_time,billing_event,bid_strategy,lifetime_spend_cap,daily_spend_cap,learning_stage_info,effective_status,lifetime_min_spend_target,destination_type,bid_adjustments,bid_amount,id,daily_min_spend_target,campaign_id,pacing_type,created_time,attribution_spec,issues_info,lifetime_budget,creative_sequence,adset_schedule,end_time,daily_budget,is_dynamic_creative,start_time,account_id,adlabels,budget_remaining,promoted_object,name,bid_constraints,targeting{geo_locations,keywords,genders,age_min,age_max,relationship_statuses,countries,locales,device_platforms,effective_device_platforms,publisher_platforms,effective_publisher_platforms,facebook_positions,effective_facebook_positions,instagram_positions,effective_instagram_positions,audience_network_positions,effective_audience_network_positions,messenger_positions,effective_messenger_positions,education_statuses,user_adclusters,excluded_geo_locations,interested_in,interests,behaviors,connections,excluded_connections,friends_of_connections,user_os,user_device,excluded_user_device,app_install_state,wireless_carrier,site_category,college_years,work_employers,work_positions,education_majors,life_events,politics,income,home_type,home_value,ethnic_affinity,generation,household_composition,moms,office_type,family_statuses,net_worth,home_ownership,industries,education_schools,custom_audiences,excluded_custom_audiences,dynamic_audience_ids,product_audience_specs,excluded_product_audience_specs,flexible_spec,exclusions,excluded_publisher_categories,excluded_publisher_list_ids,place_page_set_ids,targeting_optimization,brand_safety_content_filter_levels,is_whatsapp_destination_ad,instream_video_skippable_excluded,targeting_relaxation_types},status,rf_prediction_id,time_based_ad_rotation_id_blocks,time_based_ad_rotation_intervals,frequency_control_specs",
        "access_token": ACCESS_TOKEN
    }
    all_adsets = []
    now_ist = datetime.now(IST)
    hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
    while url:
        def do_request():
            return requests.get(url, params=params)
        response = api_request_with_backoff(do_request)
        if response is None:
            break
        logger.info(f"Fetched adsets batch raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json()
        for item in data.get("data", []):
            logger.info(f"Fetched adset: {json.dumps(item)}")
            all_adsets.append((
                item.get('id'),
                item.get('name'),
                item.get('campaign_id'),
                item.get('account_id'),
                item.get('status'),
                item.get('optimization_goal'),
                item.get('billing_event'),
                item.get('bid_strategy'),
                item.get('bid_amount'),
                item.get('daily_budget'),
                item.get('lifetime_budget'),
                item.get('budget_remaining'),
                item.get('start_time'),
                item.get('end_time'),
                item.get('created_time'),
                item.get('updated_time'),
                item.get('effective_status'),
                item.get('destination_type'),
                json.dumps(item.get('learning_stage_info')) if item.get('learning_stage_info') is not None else None,
                json.dumps(item.get('attribution_spec')) if item.get('attribution_spec') is not None else None,
                json.dumps(item.get('promoted_object')) if item.get('promoted_object') is not None else None,
                json.dumps(item.get('targeting')) if item.get('targeting') is not None else None,
                json.dumps(item.get('pacing_type')) if item.get('pacing_type') is not None else None,
                json.dumps(item.get('adlabels')) if item.get('adlabels') is not None else None,
                json.dumps(item.get('bid_adjustments')) if item.get('bid_adjustments') is not None else None,
                json.dumps(item.get('bid_constraints')) if item.get('bid_constraints') is not None else None,
                json.dumps(item.get('adset_schedule')) if item.get('adset_schedule') is not None else None,
                json.dumps(item.get('issues_info')) if item.get('issues_info') is not None else None,
                json.dumps(item.get('creative_sequence')) if item.get('creative_sequence') is not None else None,
                item.get('daily_spend_cap'),
                item.get('lifetime_spend_cap'),
                item.get('daily_min_spend_target'),
                item.get('lifetime_min_spend_target'),
                item.get('is_dynamic_creative'),
                item.get('rf_prediction_id'),
                json.dumps(item.get('time_based_ad_rotation_id_blocks')) if item.get('time_based_ad_rotation_id_blocks') is not None else None,
                json.dumps(item.get('time_based_ad_rotation_intervals')) if item.get('time_based_ad_rotation_intervals') is not None else None,
                json.dumps(item.get('frequency_control_specs')) if item.get('frequency_control_specs') is not None else None,
                hour_str,
                now_ist,
                json.dumps(item)
            ))
        url = data.get("paging", {}).get("next")
        params = {}  # Clear params for next page
    logger.info(f"Fetched {len(all_adsets)} adsets in total")
    return all_adsets

def filter_adsets_with_existing_campaigns(adsets, conn):
    if not adsets:
        return []
    cursor = conn.cursor()
    cursor.execute("SELECT campaign_id FROM dim_campaign")
    existing_campaign_ids = set(row[0] for row in cursor.fetchall())
    # adset[2] is campaign_id in the adsets tuple structure
    filtered = [adset for adset in adsets if adset[2] in existing_campaign_ids]
    missing = [adset for adset in adsets if adset[2] not in existing_campaign_ids]
    if missing:
        logger.warning(f"Filtered out {len(missing)} adsets with missing campaign_id(s): {[adset[2] for adset in missing]}")
    cursor.close()
    return filtered

def upsert_adsets(adsets, conn):
    if not adsets:
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    query = """
    INSERT INTO adsets (
        id, name, campaign_id, account_id, status, optimization_goal, billing_event, bid_strategy, bid_amount, daily_budget, lifetime_budget, budget_remaining, start_time, end_time, created_time, updated_time, effective_status, destination_type, learning_stage_info, attribution_spec, promoted_object, targeting, pacing_type, adlabels, bid_adjustments, bid_constraints, adset_schedule, issues_info, creative_sequence, daily_spend_cap, lifetime_spend_cap, daily_min_spend_target, lifetime_min_spend_target, is_dynamic_creative, rf_prediction_id, time_based_ad_rotation_id_blocks, time_based_ad_rotation_intervals, frequency_control_specs, hour, fetched_at, raw_data
    ) VALUES %s
    ON CONFLICT (id, hour) DO UPDATE SET
        name=EXCLUDED.name,
        campaign_id=EXCLUDED.campaign_id,
        account_id=EXCLUDED.account_id,
        status=EXCLUDED.status,
        optimization_goal=EXCLUDED.optimization_goal,
        billing_event=EXCLUDED.billing_event,
        bid_strategy=EXCLUDED.bid_strategy,
        bid_amount=EXCLUDED.bid_amount,
        daily_budget=EXCLUDED.daily_budget,
        lifetime_budget=EXCLUDED.lifetime_budget,
        budget_remaining=EXCLUDED.budget_remaining,
        start_time=EXCLUDED.start_time,
        end_time=EXCLUDED.end_time,
        created_time=EXCLUDED.created_time,
        updated_time=EXCLUDED.updated_time,
        effective_status=EXCLUDED.effective_status,
        destination_type=EXCLUDED.destination_type,
        learning_stage_info=EXCLUDED.learning_stage_info,
        attribution_spec=EXCLUDED.attribution_spec,
        promoted_object=EXCLUDED.promoted_object,
        targeting=EXCLUDED.targeting,
        pacing_type=EXCLUDED.pacing_type,
        adlabels=EXCLUDED.adlabels,
        bid_adjustments=EXCLUDED.bid_adjustments,
        bid_constraints=EXCLUDED.bid_constraints,
        adset_schedule=EXCLUDED.adset_schedule,
        issues_info=EXCLUDED.issues_info,
        creative_sequence=EXCLUDED.creative_sequence,
        daily_spend_cap=EXCLUDED.daily_spend_cap,
        lifetime_spend_cap=EXCLUDED.lifetime_spend_cap,
        daily_min_spend_target=EXCLUDED.daily_min_spend_target,
        lifetime_min_spend_target=EXCLUDED.lifetime_min_spend_target,
        is_dynamic_creative=EXCLUDED.is_dynamic_creative,
        rf_prediction_id=EXCLUDED.rf_prediction_id,
        time_based_ad_rotation_id_blocks=EXCLUDED.time_based_ad_rotation_id_blocks,
        time_based_ad_rotation_intervals=EXCLUDED.time_based_ad_rotation_intervals,
        frequency_control_specs=EXCLUDED.frequency_control_specs,
        fetched_at=EXCLUDED.fetched_at,
        raw_data=EXCLUDED.raw_data
    """
    try:
        execute_values(cursor, query, adsets)
        conn.commit()
        logger.info(f"Upserted {len(adsets)} adsets")
    except Exception as e:
        logger.error(f"Failed to upsert adsets: {str(e)}")
        raise
    finally:
        cursor.close()

def fetch_ads_batch():
    url = f"https://graph.facebook.com/v18.0/act_{ACCOUNT_ID}"
    params = {
        "fields": "ads{id,name,adset_id,targeting{age_min,age_max,genders,geo_locations,device_platforms,publisher_platforms},insights.date_preset(maximum){impressions,clicks,ctr,spend,actions,action_values,conversion_rate_ranking,purchase_roas,mobile_app_purchase_roas}}",
        "access_token": ACCESS_TOKEN
    }
    all_ads = []
    now_ist = datetime.now(IST)
    hour_str = now_ist.replace(minute=0, second=0, microsecond=0).isoformat()
    while url:
        def do_request():
            return requests.get(url, params=params)
        response = api_request_with_backoff(do_request)
        if response is None:
            break
        logger.info(f"Fetched ads batch raw response: {response.text}")
        time.sleep(RATE_LIMIT_INTERVAL)
        response.raise_for_status()
        data = response.json()
        ads_data = data.get("ads", {}).get("data", [])
        for item in ads_data:
            logger.info(f"Fetched ad: {json.dumps(item)}")
            all_ads.append((
                item.get('id'),
                item.get('name'),
                item.get('adset_id'),
                json.dumps(item.get('targeting')) if item.get('targeting') is not None else None,
                json.dumps(item.get('insights')) if item.get('insights') is not None else None,
                hour_str,
                now_ist,
                json.dumps(item)
            ))
        url = data.get("ads", {}).get("paging", {}).get("next")
        params = {}  # Clear params for next page
    logger.info(f"Fetched {len(all_ads)} ads in total")
    return all_ads

def upsert_ads(ads, conn):
    if not ads:
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    query = """
    INSERT INTO ads (
        id, name, adset_id, targeting, insights, hour, fetched_at, raw_data
    ) VALUES %s
    ON CONFLICT (id, hour) DO UPDATE SET
        name=EXCLUDED.name,
        adset_id=EXCLUDED.adset_id,
        targeting=EXCLUDED.targeting,
        insights=EXCLUDED.insights,
        fetched_at=EXCLUDED.fetched_at,
        raw_data=EXCLUDED.raw_data
    """
    try:
        execute_values(cursor, query, ads)
        conn.commit()
        logger.info(f"Upserted {len(ads)} ads")
    except Exception as e:
        logger.error(f"Failed to upsert ads: {str(e)}")
        raise
    finally:
        cursor.close()

def fetch_ad_insights(ad_ids):
    if not ad_ids:
        return []
    url = f"{BASE_URL}/act_{ACCOUNT_ID}/insights"
    params = {
        'access_token': ACCESS_TOKEN,
        'level': 'ad',
        'filtering': json.dumps([{'field': 'ad.id', 'operator': 'IN', 'value': ad_ids}]),
        'date_preset': 'today',
        'action_breakdowns': 'action_type',
        'fields': 'ad_id,adset_id,campaign_id,account_id,date_start,date_stop,clicks,impressions,spend,reach,actions,action_values,outbound_clicks,ctr,cpp,video_p25_watched_actions,video_thruplay_watched_actions'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json().get('data', [])
        processed_insights = []
        for insight in data:
            # Convert string values to correct types
            try:
                if 'clicks' in insight:
                    insight['clicks'] = int(insight['clicks']) if insight['clicks'] else 0
                if 'impressions' in insight:
                    insight['impressions'] = int(insight['impressions']) if insight['impressions'] else 0
                if 'reach' in insight:
                    insight['reach'] = int(insight['reach']) if insight['reach'] else 0
                if 'spend' in insight:
                    insight['spend'] = float(insight['spend']) if insight['spend'] else 0.0
                if 'ctr' in insight:
                    insight['ctr'] = float(insight['ctr']) if insight['ctr'] else 0.0
                if 'cpp' in insight:
                    insight['cpp'] = float(insight['cpp']) if insight['cpp'] else 0.0
            except (ValueError, TypeError) as e:
                logger.warning(f"Type conversion failed for insight {insight.get('ad_id', 'unknown')}: {e}")
                continue  # Skip insight if conversion fails
            processed_insights.append(insight)
        logger.info(f"Fetched {len(processed_insights)} insights (validation deferred)")
        return processed_insights
    except Exception as e:
        logger.error(f"Error fetching ad insights: {e}")
        return []

def upsert_hourly_ad_insights(insights, conn):
    if not insights:
        logger.warning("No insights to upsert into ad_insights_hourly_snapshots")
        return
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Kolkata';")
    current_hour_ist = datetime.now(IST).replace(minute=0, second=0, microsecond=0)
    query = """
    INSERT INTO ad_insights_hourly_snapshots (
        snapshot_hour, ad_id, adset_id, campaign_id, account_id, date_start, date_stop,
        clicks, impressions, spend, reach, page_engagement, post_engagement, video_view, landing_page_view,
        purchase, add_to_cart, link_click, post_reaction, outbound_click, purchase_value, view_content_value,
        add_to_cart_value, ctr, cpp, video_p25_watched, video_thruplay_watched, created_at
    ) VALUES %s
    ON CONFLICT (snapshot_hour, ad_id, date_start, date_stop) DO UPDATE SET
        clicks=EXCLUDED.clicks, impressions=EXCLUDED.impressions, spend=EXCLUDED.spend, reach=EXCLUDED.reach,
        page_engagement=EXCLUDED.page_engagement, post_engagement=EXCLUDED.post_engagement, video_view=EXCLUDED.video_view,
        landing_page_view=EXCLUDED.landing_page_view, purchase=EXCLUDED.purchase, add_to_cart=EXCLUDED.add_to_cart,
        link_click=EXCLUDED.link_click, post_reaction=EXCLUDED.post_reaction, outbound_click=EXCLUDED.outbound_click,
        purchase_value=EXCLUDED.purchase_value, view_content_value=EXCLUDED.view_content_value, add_to_cart_value=EXCLUDED.add_to_cart_value,
        ctr=EXCLUDED.ctr, cpp=EXCLUDED.cpp, video_p25_watched=EXCLUDED.video_p25_watched, video_thruplay_watched=EXCLUDED.video_thruplay_watched,
        created_at=NOW()
    """
    values = []
    valid_count = 0
    for insight in insights:
        try:
            insight['snapshot_hour'] = current_hour_ist.isoformat()
            errors = validate_schema(insight, AD_INSIGHTS_HOURLY_SNAPSHOT_SCHEMA)
            if errors:
                logger.warning(f"Schema validation failed for insight {insight.get('ad_id', 'unknown')}: {errors}")
                continue
            values.append((
                current_hour_ist,
                insight.get('ad_id'),
                insight.get('adset_id'),
                insight.get('campaign_id'),
                insight.get('account_id'),
                insight.get('date_start'),
                insight.get('date_stop'),
                int(insight.get('clicks', 0)),
                int(insight.get('impressions', 0)),
                float(insight.get('spend', 0)),
                int(insight.get('reach', 0)),
                insight.get('page_engagement', 0),
                insight.get('post_engagement', 0),
                insight.get('video_view', 0),
                insight.get('landing_page_view', 0),
                insight.get('purchase', 0),
                insight.get('add_to_cart', 0),
                insight.get('link_click', 0),
                insight.get('post_reaction', 0),
                insight.get('outbound_click', 0),
                insight.get('purchase_value', 0.0),
                insight.get('view_content_value', 0.0),
                insight.get('add_to_cart_value', 0.0),
                float(insight.get('ctr', 0)),
                float(insight.get('cpp', 0)),
                insight.get('video_p25_watched', 0),
                insight.get('video_thruplay_watched', 0),
                datetime.now(IST)
            ))
            valid_count += 1
        except Exception as e:
            logger.warning(f"Failed to prepare insight for upsert: {e}")
            continue
    try:
        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Upserted {valid_count} hourly ad insights into ad_insights_hourly_snapshots")
        else:
            logger.warning("No valid insights to upsert into ad_insights_hourly_snapshots")
    except Exception as e:
        logger.error(f"Failed to upsert hourly ad insights: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    setup_database()
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'Asia/Kolkata';")
        # Campaign ETL
        try:
            logger.info("Starting Campaign ETL step")
            campaigns = fetch_campaigns()
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO dim_campaign (
                        campaign_id, account_id, campaign_name, objective, status, buying_type,
                        special_ad_categories, start_time, end_time, budget_remaining,
                        daily_budget, lifetime_budget, created_at, updated_at, hour, fetched_at
                    )
                    VALUES %s
                    ON CONFLICT (campaign_id) DO UPDATE SET
                        account_id=EXCLUDED.account_id,
                        campaign_name=EXCLUDED.campaign_name,
                        objective=EXCLUDED.objective,
                        status=EXCLUDED.status,
                        buying_type=EXCLUDED.buying_type,
                        special_ad_categories=EXCLUDED.special_ad_categories,
                        start_time=EXCLUDED.start_time,
                        end_time=EXCLUDED.end_time,
                        budget_remaining=EXCLUDED.budget_remaining,
                        daily_budget=EXCLUDED.daily_budget,
                        lifetime_budget=EXCLUDED.lifetime_budget,
                        updated_at=EXCLUDED.updated_at,
                        hour=EXCLUDED.hour,
                        fetched_at=EXCLUDED.fetched_at
                """, campaigns)
            logger.info("Campaign ETL step completed successfully")
        except Exception as e:
            logger.error(f"Campaign ETL step failed: {e}")
        # Activity History ETL
        try:
            logger.info("Starting Activity History ETL step")
            today_ist = datetime.now(IST).date().isoformat()
            activities = fetch_activity_history(since=today_ist, until=today_ist)
            upsert_activity_history(activities, conn)
            logger.info("Activity History ETL step completed successfully")
        except Exception as e:
            logger.error(f"Activity History ETL step failed: {e}")
        # Regionwise Insights ETL
        try:
            logger.info("Starting Regionwise Insights ETL step")
            regionwise_insights = fetch_regionwise_insights()
            upsert_regionwise_insights(regionwise_insights, conn)
            logger.info("Regionwise Insights ETL step completed successfully")
        except Exception as e:
            logger.error(f"Regionwise Insights ETL step failed: {e}")
        # AdCreatives ETL
        try:
            logger.info("Starting AdCreatives ETL step")
            adcreatives = fetch_adcreatives_batch()
            upsert_adcreatives(adcreatives, conn)
            logger.info("AdCreatives ETL step completed successfully")
        except Exception as e:
            logger.error(f"AdCreatives ETL step failed: {e}")
        # AdSets ETL
        try:
            logger.info("Starting AdSets ETL step")
            adsets = fetch_adsets_batch()
            adsets = filter_adsets_with_existing_campaigns(adsets, conn)
            upsert_adsets(adsets, conn)
            logger.info("AdSets ETL step completed successfully")
        except Exception as e:
            logger.error(f"AdSets ETL step failed: {e}")
        # Ads ETL
        try:
            logger.info("Starting Ads ETL step")
            ads = fetch_ads_batch()
            upsert_ads(ads, conn)
            logger.info("Ads ETL step completed successfully")
        except Exception as e:
            logger.error(f"Ads ETL step failed: {e}")
            ads = []  # Ensure ads is always defined

        # Hourly Ad Insights ETL (direct to snapshots)
        try:
            logger.info("Starting Hourly Ad Insights ETL step (direct to ad_insights_hourly_snapshots)")
            insights = fetch_ad_insights([a[0] for a in ads])
            # Add default insights for ads with no insight
            ad_id_to_ad = {a[0]: a for a in ads}  # a[0] is ad_id
            insight_ad_ids = set(i['ad_id'] for i in insights)
            all_ad_ids = set(ad_id_to_ad.keys())
            today_str = datetime.now(IST).date().isoformat()
            for missing_ad_id in all_ad_ids - insight_ad_ids:
                ad = ad_id_to_ad[missing_ad_id]
                # ad tuple: (ad_id, name, adset_id, targeting, insights, hour, fetched_at, raw_data)
                default_insight = {
                    'ad_id': ad[0],
                    'adset_id': ad[2],
                    'campaign_id': None,
                    'account_id': None,
                    'date_start': today_str,
                    'date_stop': today_str,
                    'clicks': 0,
                    'impressions': 0,
                    'spend': 0.0,
                    'reach': 0,
                    'page_engagement': 0,
                    'post_engagement': 0,
                    'video_view': 0,
                    'landing_page_view': 0,
                    'purchase': 0,
                    'add_to_cart': 0,
                    'link_click': 0,
                    'post_reaction': 0,
                    'outbound_click': 0,
                    'purchase_value': 0.0,
                    'view_content_value': 0.0,
                    'add_to_cart_value': 0.0,
                    'ctr': 0.0,
                    'cpp': 0.0,
                    'video_p25_watched': 0,
                    'video_thruplay_watched': 0
                }
                insights.append(default_insight)
            upsert_hourly_ad_insights(insights, conn)
            logger.info("Hourly Ad Insights ETL step completed successfully")
        except Exception as e:
            logger.error(f"Hourly Ad Insights ETL step failed: {e}")
        logger.info("All ETL steps attempted")
    except Exception as e:
        logger.error(f"ETL script failed at connection/setup: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
