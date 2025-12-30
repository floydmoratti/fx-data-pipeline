import json
import os
import boto3
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP


# ---------- Setup ----------
bucket = os.environ["BUCKET_NAME"]
CURRENCY_PAIRS = os.environ.get("CURRENCY_PAIRS", "USDJPY").split(",")  # convert string to a list

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


# ---------- Helper Functions ----------
def get_run_date(event):
    raw_run_date = event["run_date"]
    dt = datetime.fromisoformat(raw_run_date.replace("Z", "+00:00"))
    fx_dt = dt - timedelta(days=1)
    year = f"{fx_dt.year:04d}"
    month = f"{fx_dt.month:02d}"
    day = f"{fx_dt.day:02d}"

    return year, month, day, fx_dt


def build_s3_key(year, month, day):
    logger.info("Generating key from event record")
    read_key = f"raw/year={year}/month={month}/day={day}/rates.json"

    return read_key


def read_raw_object(read_key):
    logger.info("Reading S3 object from bucket")

    response = s3.get_object(Bucket=bucket, Key=read_key)
    data = json.loads(response["Body"].read())
    return data


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


def is_weekday(fx_dt):
    # outputs true for weekday and false for weekends
    return fx_dt.weekday() < 5  # 5 = Saturday, 6 = Sunday


def get_decimal_places(pair):
    # Returns the number of decimal places based on FX trading conventions
    if pair.endswith("JPY"):
        return 3
    return 5


def format_fx_rate(rate, decimals):
    # Formats an FX rate using Decimal for financial accuracy.
    quantizer = Decimal("1." + "0" * decimals)
    return float(Decimal(str(rate)).quantize(quantizer, rounding=ROUND_HALF_UP))
        

def normalize_and_write(data, year, month, day, fx_dt):
    quotes = data["quotes"]
    written_files = []

    for pair, rate in quotes.items():

        decimals = get_decimal_places(pair)
        formatted_rate = format_fx_rate(rate, decimals)

        record = {
            "rate": formatted_rate,
            "date": f"{year}-{month}-{day}",
            "market_open": is_weekday(fx_dt)
        }

        write_key = (
            f"processed/pair={pair}/year={year}/month={month}/day={day}/data.json"
        )

        logger.info(f"Writing file to bucket: {write_key}")

        s3.put_object(
            Bucket=bucket,
            Key=write_key,
            Body=json.dumps(record),
            ContentType="application/json"
        )
    
        written_files.append(write_key)

    return written_files


# ---------- Lambda Handler ----------
def lambda_handler(event, context):

    logger.info("Received event: %s", json.dumps(event, indent=2, default=str))

    year, month, day, fx_dt = get_run_date(event)
    read_key = build_s3_key(year, month, day)

    # Only process raw files and prevent infinite loop
    if not read_key.startswith("raw/"):
        print(f"Skipping non-raw object: {read_key}")
        return
    
    data = read_raw_object(read_key)
    validate_fx_data(data, CURRENCY_PAIRS)

    output_keys = normalize_and_write(data, year, month, day, fx_dt)

    return {
        "statusCode": 200,
        "files_written": output_keys
    }