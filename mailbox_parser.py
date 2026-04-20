import os
import re
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG — set these as environment variables on Render
# ---------------------------------------------------------------------------
TENANT_ID     = os.environ["AZURE_TENANT_ID"]
CLIENT_ID     = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
MAILBOX       = os.environ["CC_MAILBOX"]          # e.g. no_reply_mft@marinefluid.dk
DATABASE_URL  = os.environ["DATABASE_URL"]         # PostgreSQL connection string

# ---------------------------------------------------------------------------
# SUBJECT LINE PATTERNS
# ---------------------------------------------------------------------------
PATTERNS = {
    "DATA REQUEST": re.compile(r"^\[DATA REQUEST\]\s+(.+)$", re.IGNORECASE),
    "FOLLOW-UP":    re.compile(r"^\[FOLLOW-UP\]\s+(.+)$",    re.IGNORECASE),
    "FB REPORT":    re.compile(r"^\[FB REPORT\]\s+(.+)$",    re.IGNORECASE),
}

# ---------------------------------------------------------------------------
# STEP 1: Get Microsoft Graph API access token
# ---------------------------------------------------------------------------
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

# ---------------------------------------------------------------------------
# STEP 2: Fetch unread emails from the CC mailbox
# ---------------------------------------------------------------------------
def fetch_unread_emails(token):
    url = (
        f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/mailFolders/inbox/messages"
        f"?$filter=isRead eq false"
        f"&$select=id,subject,receivedDateTime"
        f"&$top=50"
    )
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("value", [])

# ---------------------------------------------------------------------------
# STEP 3: Mark an email as read after processing
# ---------------------------------------------------------------------------
def mark_as_read(token, message_id):
    url = f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    requests.patch(url, headers=headers, json={"isRead": True})

# ---------------------------------------------------------------------------
# STEP 4: Parse subject line — returns (email_type, vessel_name) or None
# ---------------------------------------------------------------------------
def parse_subject(subject):
    for email_type, pattern in PATTERNS.items():
        match = pattern.match(subject.strip())
        if match:
            vessel_name = match.group(1).strip()
            return email_type, vessel_name
    return None

# ---------------------------------------------------------------------------
# STEP 5: Upsert into fb_dates
# ---------------------------------------------------------------------------
def upsert_fb_dates(cursor, email_type, vessel_name, received_at):
    if email_type == "DATA REQUEST":
        sql = """
            INSERT INTO public.fb_dates (vessel_name, last_outreach_sent, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (vessel_name)
            DO UPDATE SET
                last_outreach_sent = EXCLUDED.last_outreach_sent,
                updated_at = NOW();
        """
        cursor.execute(sql, (vessel_name, received_at))
        print(f"  → Updated last_outreach_sent for '{vessel_name}'")

    elif email_type == "FOLLOW-UP":
        sql = """
            INSERT INTO public.fb_dates (vessel_name, last_followup_sent, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (vessel_name)
            DO UPDATE SET
                last_followup_sent = EXCLUDED.last_followup_sent,
                updated_at = NOW();
        """
        cursor.execute(sql, (vessel_name, received_at))
        print(f"  → Updated last_followup_sent for '{vessel_name}'")

    elif email_type == "FB REPORT":
        sql = """
            INSERT INTO public.fb_dates (vessel_name, last_report_sent, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (vessel_name)
            DO UPDATE SET
                last_report_sent = EXCLUDED.last_report_sent,
                updated_at = NOW();
        """
        cursor.execute(sql, (vessel_name, received_at))
        print(f"  → Updated last_report_sent for '{vessel_name}'")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print(f"[{datetime.now()}] Starting mailbox parser...")

    token = get_access_token()
    emails = fetch_unread_emails(token)
    print(f"  Found {len(emails)} unread email(s)")

    if not emails:
        print("  Nothing to process.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    for email in emails:
        subject     = email.get("subject", "")
        message_id  = email["id"]
        received_at = email["receivedDateTime"]  # ISO 8601 string

        print(f"\n  Processing: '{subject}'")

        parsed = parse_subject(subject)
        if not parsed:
            print(f"  → Subject did not match any pattern, skipping")
            mark_as_read(token, message_id)
            continue

        email_type, vessel_name = parsed
        print(f"  → Type: {email_type} | Vessel: {vessel_name}")

        # Check vessel exists in fb_dates or other tables
        cursor.execute(
            "SELECT vessel_name FROM public.fb_dates WHERE vessel_name = %s",
            (vessel_name,)
        )
        existing = cursor.fetchone()
        if not existing:
            # Check if vessel exists in your main data table
            cursor.execute(
                "SELECT vessel_name FROM public.fb_report_data WHERE vessel_name = %s LIMIT 1",
                (vessel_name,)
            )
            known_vessel = cursor.fetchone()
            if not known_vessel:
                print(f"  → WARNING: Vessel '{vessel_name}' not found in DB — skipping")
                mark_as_read(token, message_id)
                continue

        upsert_fb_dates(cursor, email_type, vessel_name, received_at)
        mark_as_read(token, message_id)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n[{datetime.now()}] Parser finished.")

if __name__ == "__main__":
    main()