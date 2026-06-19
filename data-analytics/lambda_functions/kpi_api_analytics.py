import json
import os
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from aws_lambda_powertools.utilities import parameters
except ImportError:
    parameters = None

SCHEMA_NAME = "virginia_dev_saayam_rdbms"


SLA = {
    "target_days": 10,
    "target_hours": 240,
    "warning_days": 8.33,
    "warning_hours": 200
}


def get_default_response():
    return {
        "request_status_distribution": [],
        "total_requests": 0,
        "average_resolution_time_by_category": [],
        "sla": SLA
    }


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body, default=str)
    }


def get_db_connection():
    local_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT", "5432"),
        "sslmode": os.getenv("DB_SSLMODE", "require")
    }

    required_local_settings = ("host", "database", "user", "password")
    if all(local_config[key] for key in required_local_settings):
        return psycopg2.connect(**local_config)

    if parameters is None:
        raise RuntimeError(
            "Local database settings are incomplete. Set DB_HOST, DB_NAME, "
            "DB_USER, and DB_PASSWORD."
        )

    creds = json.loads(parameters.get_parameter(
        os.getenv(
            "DB_CREDENTIALS_PARAMETER",
            "/dev/saayam/db/Virginia/Analytics/user"
        ),
        decrypt=True,
        max_age=3600
    ))

    db_name = creds["DATABASE NAME"]

    return psycopg2.connect(
        host=creds["HOST"],
        database=db_name,
        user=creds["USERNAME"],
        password=creds["PASSWORD"],
        port=creds["PORT"],
        sslmode="require"
    )


def build_date_filter(time_range, start_date=None, end_date=None):
    normalized_time_range = (time_range or "All").strip().upper()

    if normalized_time_range == "7D":
        return "r.submission_date >= CURRENT_DATE - INTERVAL '7 days'", []
    if normalized_time_range == "30D":
        return "r.submission_date >= CURRENT_DATE - INTERVAL '30 days'", []
    if normalized_time_range == "1Y":
        return "r.submission_date >= CURRENT_DATE - INTERVAL '1 year'", []
    if normalized_time_range == "ALL":
        return "", []
    if normalized_time_range == "CUSTOM":
        if not start_date or not end_date:
            raise ValueError(
                "Custom time_range requires both start_date and end_date."
            )

        try:
            parsed_start_date = date.fromisoformat(start_date)
            parsed_end_date = date.fromisoformat(end_date)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "start_date and end_date must use YYYY-MM-DD format."
            ) from error

        if parsed_start_date > parsed_end_date:
            raise ValueError("start_date cannot be after end_date.")

        return (
            "r.submission_date::date BETWEEN %s::date AND %s::date",
            [parsed_start_date, parsed_end_date]
        )

    raise ValueError(
        "time_range must be one of 7D, 30D, 1Y, All, or Custom."
    )


def fetch_request_status_distribution(
    cursor,
    time_range,
    start_date=None,
    end_date=None
):
    date_condition, date_params = build_date_filter(
        time_range,
        start_date,
        end_date
    )
    where_clause = f"WHERE {date_condition}" if date_condition else ""

    query = f"""
        SELECT
            rs.req_status AS status,
            COUNT(r.req_id) AS count
        FROM {SCHEMA_NAME}.request r
        JOIN {SCHEMA_NAME}.request_status rs
            ON r.req_status_id = rs.req_status_id
        {where_clause}
        GROUP BY rs.req_status
        ORDER BY rs.req_status;
    """

    cursor.execute(query, date_params)
    rows = cursor.fetchall()

    return [
        {
            "status": row["status"],
            "count": int(row["count"])
        }
        for row in rows
    ]


def fetch_total_requests(
    cursor,
    time_range,
    start_date=None,
    end_date=None
):
    date_condition, date_params = build_date_filter(
        time_range,
        start_date,
        end_date
    )
    where_clause = f"WHERE {date_condition}" if date_condition else ""

    query = f"""
        SELECT COUNT(req_id) AS total_requests
        FROM {SCHEMA_NAME}.request r
        {where_clause};
    """

    cursor.execute(query, date_params)
    row = cursor.fetchone()

    return int(row["total_requests"]) if row and row["total_requests"] is not None else 0


def fetch_average_resolution_time_by_category(
    cursor,
    time_range,
    start_date=None,
    end_date=None
):
    date_condition, date_params = build_date_filter(
        time_range,
        start_date,
        end_date
    )
    date_filter_clause = f"AND {date_condition}" if date_condition else ""

    query = f"""
        SELECT
            hc.cat_name AS category,
            ROUND(
                AVG(
                    EXTRACT(EPOCH FROM (r.serviced_date - r.submission_date)) / 3600
                )::numeric,
                2
            ) AS avg_hours
        FROM {SCHEMA_NAME}.request r
        JOIN {SCHEMA_NAME}.help_categories hc
            ON r.req_cat_id = hc.cat_id
        JOIN {SCHEMA_NAME}.request_status rs
            ON r.req_status_id = rs.req_status_id
        WHERE r.submission_date IS NOT NULL
          AND r.serviced_date IS NOT NULL
          AND r.serviced_date >= r.submission_date
          AND UPPER(rs.req_status) IN ('COMPLETED', 'RESOLVED')
          {date_filter_clause}
        GROUP BY hc.cat_name
        ORDER BY avg_hours DESC;
    """

    cursor.execute(query, date_params)
    rows = cursor.fetchall()

    return [
        {
            "category": row["category"],
            "avg_hours": float(row["avg_hours"]) if row["avg_hours"] is not None else 0
        }
        for row in rows
    ]


def lambda_handler(event, context):
    conn = None
    cursor = None
    response_body = get_default_response()
    event = event or {}
    time_range = event.get("time_range", "All")
    start_date = event.get("start_date")
    end_date = event.get("end_date")

    try:
        build_date_filter(time_range, start_date, end_date)
    except ValueError as error:
        print(f"Invalid date filter: {error}")
        return build_response(200, response_body)

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            response_body["request_status_distribution"] = (
                fetch_request_status_distribution(
                    cursor,
                    time_range,
                    start_date,
                    end_date
                )
            )
        except Exception as error:
            print(f"Status distribution query failed: {error}")
            conn.rollback()
            response_body["request_status_distribution"] = []

        try:
            response_body["total_requests"] = fetch_total_requests(
                cursor,
                time_range,
                start_date,
                end_date
            )
        except Exception as error:
            print(f"Total request count query failed: {error}")
            conn.rollback()
            response_body["total_requests"] = 0

        try:
            response_body["average_resolution_time_by_category"] = (
                fetch_average_resolution_time_by_category(
                    cursor,
                    time_range,
                    start_date,
                    end_date
                )
            )
        except Exception as error:
            print(f"Average resolution time query failed: {error}")
            conn.rollback()
            response_body["average_resolution_time_by_category"] = []

        return build_response(200, response_body)

    except Exception as error:
        print(f"DB connection failed: {error}")
        return build_response(500, response_body)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))
