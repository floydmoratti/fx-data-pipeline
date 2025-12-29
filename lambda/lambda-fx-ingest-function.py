import json
import os
import logging
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import boto3


# ---------- Setup ----------
BUCKET_NAME = os.environ["BUCKET_NAME"]
CURRENCY_PAIRS = os.environ.get("CURRENCY_PAIRS", "USDJPY").split(",")  # convert string to a list
FX_API_URL = os.environ["FX_API_URL"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
s3 = boto3.client("s3")


# ---------- Helper Functions ----------
def get_api_key():
    logger.info("Retrieving API Key from Parameter Store")
    response = ssm.get_parameter(Name="/fx/api/access_key", WithDecryption=True)

    return response["Parameter"]["Value"]


def get_run_date(event):
    raw_run_date = event["run_date"]
    dt = datetime.fromisoformat(raw_run_date.replace("Z", "+00:00"))
    fx_dt = dt - timedelta(days=1)
    year = f"{fx_dt.year:04d}"
    month = f"{fx_dt.month:02d}"
    day = f"{fx_dt.day:02d}"

    return year, month, day


def list_quote_currencies(CURRENCY_PAIRS):
    # Determine all unique currencies needed for USD-based API request
    logger.info("Creating set of unique quote currencies")
    quote_currencies = set()
    for pair in CURRENCY_PAIRS:
        base, quote = pair[:3], pair[3:]
        if base != "USD":
            quote_currencies.add(base)
        if quote != "USD":
            quote_currencies.add(quote)
    
    quote_currencies.discard("USD")  # Ensure "USD" is not in quote currencies
    logger.info(f"Quote currencies: {quote_currencies}")

    return list(quote_currencies)



def fetch_fx_rates(api_key, quote_currencies):
    # Fetches FX rates from public API
    params = {
        "access_key": api_key,
        "source": "USD",
        "currencies": ",".join(quote_currencies)  # convert list into single string
    }

    url = FX_API_URL + "?" + urllib.parse.urlencode(params)
    logger.info(f"Requesting FX data from {url}")

    with urllib.request.urlopen(url, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f"FX API returned status {response.status}")
        data = json.loads(response.read())
    
    logger.info(data)

    return data


def calculate_pairs(data, pairs):
    # Given USD-based quotes, calculate all requested pairs including inverses
    logger.info("Calculating pair quotes with raw API data")
    results = {}
    usd_quotes = data["quotes"]
    logger.info(f"usd_quotes = {usd_quotes}")

    for pair in pairs:
        base, quote = pair[:3], pair[3:]
        logger.info(f"base = {base}, quote = {quote}")
        if base == "USD":     # Assign direct quote from USD
            results[pair] = usd_quotes.get(pair)
            logger.info(f"{results[pair]} = {usd_quotes.get(pair)}")
        elif quote == "USD":  # Calculate inverse of USD-based rate
            results[pair] = 1 / usd_quotes.get("USD" + base)
        else:                 # Non-USD pair, calculate via USD cross rate
            results[pair] = usd_quotes.get("USD" + quote) / usd_quotes.get("USD" + base)
    
    logger.info(f"Calculated results: {results}")

    # Select required fields, dropping unnessacery fields from data
    payload = {
        "timestamp": data.get("timestamp"),
        "quotes": results
    }

    logger.info(f"Result to return: {payload}")

    return payload


def validate_fx_data(data, pairs):
    # Basic validation to catch bad or partial responses
    required_fields = ["timestamp", "quotes"]
    logger.info(f"Validating required fields are in data: {required_fields}")

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    for pair in pairs:
        if pair not in data["quotes"]:
            raise ValueError(f"Missing FX quote for: {pair}")


def build_s3_key(year, month, day):
    # Build partitioned S3 key

    return f"raw/year={year}/month={month}/day={day}/rates.json"


def write_to_s3(s3_key, data):
    # Write raw FX JSON to S3

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    logger.info("FX ingestion lambda started")
    logger.info("Received event: %s", json.dumps(event, indent=2, default=str))

    try:
        api_key = get_api_key()
        quote_currencies = list_quote_currencies(CURRENCY_PAIRS)

        raw_fx_data = fetch_fx_rates(api_key, quote_currencies)
        pairs_fx_data = calculate_pairs(raw_fx_data, CURRENCY_PAIRS)

        validate_fx_data(pairs_fx_data, CURRENCY_PAIRS)

        year, month, day = get_run_date(event)
        s3_key = build_s3_key(year, month, day)
        write_to_s3(s3_key, pairs_fx_data)

        logger.info(f"FX data successfully written to s3://{BUCKET_NAME}/{s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "FX data ingested successfully",
                "s3_key": s3_key
            })
        }
    
    except Exception as e:
        logger.exception("FX ingestion failed")
        raise e