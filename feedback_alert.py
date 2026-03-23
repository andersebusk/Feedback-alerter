import os
import psycopg2
import requests
from datetime import date

DATABASE_URL = os.environ["DATABASE_URL"]
ALERT_TO_EMAIL = os.environ["ALERT_TO_EMAIL"]
GRAPH_FROM_EMAIL = os.environ["GRAPH_FROM_EMAIL"]

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]


def get_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    response = requests.post(token_url, data=data, timeout=30)
    response.raise_for_status()

    token_data = response.json()
    return token_data["access_token"]


def send_email(subject: str, body: str, recipients: list[str]) -> None:
    access_token = get_access_token()

    url = f"https://graph.microsoft.com/v1.0/users/{GRAPH_FROM_EMAIL}/sendMail"

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body,
            },
            "toRecipients": [
                {"emailAddress": {"address": email}} for email in recipients
            ],
        },
        "saveToSentItems": True,
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code not in (200, 202):
        raise Exception(
            f"Graph sendMail failed: {response.status_code} {response.text}"
        )


def get_vessels_to_alert(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                vessel_name,
                MAX(feedback_received_at::date) AS latest_feedback_date,
                CURRENT_DATE - MAX(feedback_received_at::date) AS days_since_latest
            FROM kpi_data
            WHERE feedback_received_at IS NOT NULL
              AND COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY vessel_name
            HAVING
                CURRENT_DATE - MAX(feedback_received_at::date) >= 7
            ORDER BY vessel_name;
        """)
        return cur.fetchall()


def main():
    recipients = [email.strip() for email in ALERT_TO_EMAIL.split(",") if email.strip()]
    today = date.today()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        rows = get_vessels_to_alert(conn)

        if not rows:
            print("No vessels matched alert criteria today.")
            return

        for vessel_name, latest_feedback_date, days_since_latest in rows:
            subject = f"{vessel_name} feedback report status"
            body = (
                f"{vessel_name} feedback report status:\n\n"
                f"It is now {days_since_latest} days ago we received data from {vessel_name}.\n\n"
                f"Latest feedback received date: {latest_feedback_date}\n"
                f"Today's date: {today}\n"
            )

            send_email(subject, body, recipients)
            print(f"Email sent for {vessel_name} ({days_since_latest} days).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()