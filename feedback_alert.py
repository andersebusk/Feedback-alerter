import os
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date

DATABASE_URL = os.environ["DATABASE_URL"]
ALERT_TO_EMAIL = os.environ["ALERT_TO_EMAIL"]
ALERT_FROM_EMAIL = os.environ["ALERT_FROM_EMAIL"]
SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true").lower() == "true"


def send_email(subject: str, body: str, recipients: list[str]) -> None:
    msg = MIMEMultipart()
    msg["From"] = ALERT_FROM_EMAIL
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_USE_TLS:
            server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(ALERT_FROM_EMAIL, recipients, msg.as_string())


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
                CURRENT_DATE - MAX(feedback_received_at::date) IN (25, 28, 30)
                OR CURRENT_DATE - MAX(feedback_received_at::date) > 30
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