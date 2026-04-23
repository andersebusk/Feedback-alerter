import os
import requests
import psycopg2
import psycopg2.extras
import mailbox_parser
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG — set these as environment variables on Render
# ---------------------------------------------------------------------------
TENANT_ID     = os.environ["AZURE_TENANT_ID"]
CLIENT_ID     = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SENDER        = os.environ["CC_MAILBOX"]          # no_reply_mft@marinefluid.dk
DATABASE_URL  = os.environ["DATABASE_URL"]
DIGEST_RECIPIENT = os.environ["DIGEST_RECIPIENT"]
APP_BASE_URL     = os.environ["APP_BASE_URL"]        # e.g. https://feedback-report-generator.onrender.com
# ---------------------------------------------------------------------------
# CUSTOMER OUTREACH CONFIG
# To activate a customer: add vessel names to their list below.
# Chief engineer name and email are read per vessel from legacy_vessels
# (columns: chief_engineer_name, chief_engineer_email).
# Customers with no vessels listed, or vessels missing chief engineer data,
# are silently skipped.
# ---------------------------------------------------------------------------
CUSTOMER_OUTREACH = {
    "MSC": {
        "vessels": [
            # Add MSC vessel names here once legacy_vessels has the data, e.g.:
            # "MSC VESSEL NAME",
        ],
    },
    # "NEXT CUSTOMER": {
    #     "vessels": ["VESSEL A", "VESSEL B"],
    # },
}

# ---------------------------------------------------------------------------
# BUSINESS DAY HELPERS
# ---------------------------------------------------------------------------
def business_days_since(dt):
    """Count business days (Mon-Fri) between a past datetime and now."""
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    count = 0
    current = dt.date()
    end = now.date()
    while current < end:
        if current.weekday() < 5:  # Mon=0, Fri=4
            count += 1
        current += timedelta(days=1)
    return count

def calendar_days_since(dt):
    """Count calendar days between a past datetime and now."""
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    # Handle both date and datetime objects
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now - dt).days
    else:
        # It's a date object, compare dates directly
        return (now.date() - dt).days

# ---------------------------------------------------------------------------
# GRAPH API: Get access token
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
# GRAPH API: Send email
# ---------------------------------------------------------------------------
def send_email(token, to_address, subject, html_body):
    url = f"https://graph.microsoft.com/v1.0/users/{SENDER}/sendMail"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body,
            },
            "toRecipients": [
                {"emailAddress": {"address": to_address}}
            ],
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    print(f"  → Email sent to {to_address}")

# ---------------------------------------------------------------------------
# MAILTO LINK BUILDERS
# ---------------------------------------------------------------------------
_DATA_REQUEST_BODY = (
    "Dear Chief Engineer,\n \n"
    "I hope this message finds you well. As part of our ongoing data feedback service, "
    "we are now due to receive the next set of operational data for the above vessel. "
    "This information is essential for us to provide you with accurate, vessel-specific "
    "recommendations on cylinder condition, lube oil feed rates, and overall engine performance.\n \n"
    "We kindly request the following items by:\n \n"
    "• Completed feedback sheet (see attached form)\n"
    "• Most recent scavenge port inspection report\n"
    "• Recent crankcase photos\n"
    "• Updated lube oil analysis reports\n \n"
    "Each report we produce is tailored to your engine’s operating profile. "
    "The more complete and timelier the data, the more precise our recommendations.\n \n"
    "Your ongoing cooperation is greatly appreciated. Please feel free to reach out with "
    "any questions or if any of the above items are unavailable.\n \n"
    "Thank you in advance and wishing you a safe and efficient onward voyage."
)

def make_data_request_mailto(vessel_name):
    subject = quote(f"[Data request – {vessel_name}]")
    bcc     = quote(SENDER)
    body    = quote(_DATA_REQUEST_BODY)
    return f"mailto:?subject={subject}&bcc={bcc}&body={body}"

def make_followup_mailto(vessel_name):
    # TODO: replace with follow-up template when available
    subject = quote(f"[Follow up – {vessel_name}]")
    bcc     = quote(SENDER)
    return f"mailto:?subject={subject}&bcc={bcc}"

# ---------------------------------------------------------------------------
# BUILD EMAIL BODY
# ---------------------------------------------------------------------------
def build_digest_email(overdue_vessels, escalated_vessels, critical_vessels):
    def vessel_rows(vessels, mail_type):
        if not vessels:
            return "<tr><td colspan='4' style='color:#888;padding:8px;'>None</td></tr>"
        rows = ""
        for v in vessels:
            if mail_type == "DATA REQUEST":
                mailto = make_data_request_mailto(v["vessel_name"])
            elif mail_type == "FOLLOW-UP":
                mailto = make_followup_mailto(v["vessel_name"])
            else:
                mailto = f"mailto:?subject={quote('[' + mail_type + '] ' + v['vessel_name'])}"
            rows += f"""
            <tr>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['responsible']}</td>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['vessel_name']}</td>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['days_since']} days</td>
                <td style='padding:8px;border-bottom:1px solid #eee;'>
                    <a href='{mailto}'
                       style='background:#003366;color:white;padding:4px 10px;
                              border-radius:4px;text-decoration:none;font-size:12px;'>
                        Send Email
                    </a>
                </td>
            </tr>"""
        return rows

    def section_header(color, title, count, description):
        return f"""
        <table style='width:100%;border-collapse:collapse;margin-bottom:4px;'>
            <tr>
                <td style='width:12px;background:{color};'>&nbsp;</td>
                <td style='padding-left:8px;'>
                    <span style='font-size:16px;font-weight:bold;color:{color};'>{title} ({count} vessels)</span>
                </td>
            </tr>
        </table>
        <p style='font-size:13px;color:#555;margin-top:2px;'>{description}</p>
        """

    inactive_link = f"""
        <p style='font-size:13px;color:#555;margin-top:8px;'>
            Do you want to change any of these vessels to inactive?
            <a href='{APP_BASE_URL}/vessel-inactive'
               style='color:#003366;'>Use the following link</a>.
        </p>
    """

    return f"""
    <html>
    <body style='font-family:Arial,sans-serif;color:#333;max-width:700px;margin:0 auto;padding:20px;'>
        <h2 style='color:#003366;border-bottom:2px solid #003366;padding-bottom:8px;'>
            Daily Vessel Follow-up
        </h2>
        <p>Good morning,<br>Here is your daily summary of vessels requiring attention.</p>

        {section_header('#cc0000', 'Overdue — Needs Outreach', len(overdue_vessels),
            'Data has not been received in 28+ days and no outreach has been sent.')}
        <table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:24px;'>
            <tr style='background:#f5f5f5;'>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Responsible</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Vessel</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Days Since Last Feedback</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Action</th>
            </tr>
            {vessel_rows(overdue_vessels, 'DATA REQUEST')}
        </table>

        {section_header('#e68a00', 'Escalated — No Response to Outreach', len(escalated_vessels),
            'Outreach was sent but no data received within 3 business days.')}
        <table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:8px;'>
            <tr style='background:#f5f5f5;'>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Responsible</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Vessel</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Days Since Outreach Sent</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Action</th>
            </tr>
            {vessel_rows(escalated_vessels, 'FOLLOW-UP')}
        </table>
        {inactive_link}
        <br>

        {section_header('#660000', 'Critical — Follow-up Also Ignored', len(critical_vessels),
            'Follow-up was sent but still no data received. Further action required.')}
        <table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:8px;'>
            <tr style='background:#f5f5f5;'>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Responsible</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Vessel</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Days Since Follow-up Sent</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Action</th>
            </tr>
            {vessel_rows(critical_vessels, 'FOLLOW-UP')}
        </table>
        {inactive_link}
        <br>

        <p style='font-size:13px;color:#555;'>
            You can see a full overview of all vessels here:
            <a href='{APP_BASE_URL}/vessel-overview'
               style='color:#003366;'>Vessel Overview</a>
        </p>

        <p style='font-size:12px;color:#aaa;border-top:1px solid #eee;padding-top:12px;'>
            This is an automated message from the Marine Fluid Technology data tracking system.
        </p>
    </body>
    </html>
    """

# ---------------------------------------------------------------------------
# CUSTOMER OUTREACH EMAILS
# Replace the body strings below with the actual templates when available.
# ---------------------------------------------------------------------------
def build_customer_overdue_email(vessel_name, chief_engineer_name):
    # TODO: replace with actual overdue template
    return f"<p>Placeholder: data request for <strong>{vessel_name}</strong> (attn. {chief_engineer_name}).</p>"

def build_customer_followup_email(vessel_name, chief_engineer_name):
    # TODO: replace with actual follow-up template
    return f"<p>Placeholder: follow-up for <strong>{vessel_name}</strong> (attn. {chief_engineer_name}).</p>"

def send_customer_outreach_emails(token, overdue_vessels, escalated_vessels):
    for customer, config in CUSTOMER_OUTREACH.items():
        customer_vessels = set(config["vessels"])

        if not customer_vessels:
            print(f"  -> Customer '{customer}' skipped (no vessels configured)")
            continue

        for v in overdue_vessels:
            if v["vessel_name"] not in customer_vessels:
                continue
            contact = v["chief_engineer_email"]
            if not contact:
                print(f"  -> '{v['vessel_name']}' skipped (no chief engineer email in DB)")
                continue
            send_email(
                token,
                to_address=contact,
                subject=f"[DATA REQUEST] {v['vessel_name']}",
                html_body=build_customer_overdue_email(v["vessel_name"], v["chief_engineer_name"]),
            )
            print(f"  -> Customer overdue mail sent to {customer} ({contact}) for '{v['vessel_name']}'")

        for v in escalated_vessels:
            if v["vessel_name"] not in customer_vessels:
                continue
            contact = v["chief_engineer_email"]
            if not contact:
                print(f"  -> '{v['vessel_name']}' skipped (no chief engineer email in DB)")
                continue
            send_email(
                token,
                to_address=contact,
                subject=f"[FOLLOW-UP] {v['vessel_name']}",
                html_body=build_customer_followup_email(v["vessel_name"], v["chief_engineer_name"]),
            )
            print(f"  -> Customer follow-up mail sent to {customer} ({contact}) for '{v['vessel_name']}'")

# ---------------------------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------------------------
def main():
    now = datetime.now(timezone.utc)

    # Skip weekends
    if now.weekday() >= 5:
        print(f"[{now}] Weekend — skipping cron job.")
        return

    # Run mailbox parser first so latest email dates are in the DB
    print("Running mailbox parser...")
    mailbox_parser.main()

    print(f"[{now}] Starting daily status evaluation...")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ------------------------------------------------------------------
    # Fetch all vessels with their tracking data
    # ------------------------------------------------------------------
    cursor.execute("""
        SELECT
            fd.vessel_name,
            fd.status,
            fd.last_outreach_sent,
            fd.last_followup_sent,
            fd.inaktiv_until,
            lv.responsible,
            lv.chief_engineer_name,
            lv.chief_engineer_email,
            latest_data.last_data_received
        FROM public.fb_dates fd
        LEFT JOIN public.legacy_vessels lv
            ON fd.vessel_name = lv.vessel_name
        LEFT JOIN (
            SELECT vessel_name, MAX(feedback_received_at) AS last_data_received
            FROM (
                SELECT vessel_name, feedback_received_at FROM public.fb_report_data
                UNION ALL
                SELECT vessel_name, feedback_received_at FROM public.scrape_lab
                UNION ALL
                SELECT vessel_name, feedback_received_at FROM public.me_sys_data
                UNION ALL
                SELECT vessel_name, feedback_received_at FROM public.scavenge_data
            ) all_data
            GROUP BY vessel_name
        ) latest_data ON fd.vessel_name = latest_data.vessel_name
        WHERE lv.priority = 1
          AND (fd.status != 'inaktiv'
           OR (fd.status = 'inaktiv' AND fd.inaktiv_until <= NOW()))
    """)
    vessels = cursor.fetchall()
    print(f"  Found {len(vessels)} vessel(s) to evaluate")

    # ------------------------------------------------------------------
    # Digest buckets — one list per status, all vessels in one email
    # ------------------------------------------------------------------
    digest = {"overdue": [], "escalated": [], "critical": []}

    def add_to_digest(responsible, bucket, vessel_name, days_since, chief_engineer_name=None, chief_engineer_email=None):
        digest[bucket].append({
            "vessel_name":           vessel_name,
            "days_since":            days_since if days_since is not None else "N/A",
            "responsible":           responsible,
            "chief_engineer_name":   chief_engineer_name,
            "chief_engineer_email":  chief_engineer_email,
        })

    for v in vessels:
        vessel_name   = v["vessel_name"]
        status        = v["status"]
        last_data     = v["last_data_received"]
        last_outreach = v["last_outreach_sent"]
        last_followup = v["last_followup_sent"]
        inaktiv_until = v["inaktiv_until"]
        responsible          = v["responsible"] or "no contact"
        chief_engineer_name  = v["chief_engineer_name"]
        chief_engineer_email = v["chief_engineer_email"]

        days_since_data     = calendar_days_since(last_data)
        days_since_outreach = business_days_since(last_outreach)
        days_since_followup = business_days_since(last_followup)

        # ----------------------------------------------------------
        # Any status → aktiv
        # New data received after the last outreach/followup
        # ----------------------------------------------------------
        if status in ('overdue', 'eskaleret', 'kritisk'):
            if days_since_data is not None and days_since_data < 28:
                cursor.execute("""
                    UPDATE public.fb_dates
                    SET status = 'aktiv', updated_at = NOW()
                    WHERE vessel_name = %s
                """, (vessel_name,))
                status = "aktiv"
                print(f"  → '{vessel_name}' moved back to aktiv (data received)")
                continue  # No need to evaluate further for this vessel

        # Reactivate inactive vessels whose freeze period has ended
        if status == "inaktiv" and inaktiv_until and inaktiv_until <= now:
            cursor.execute("""
                UPDATE public.fb_dates
                SET status = 'aktiv', updated_at = NOW()
                WHERE vessel_name = %s
            """, (vessel_name,))
            print(f"  → '{vessel_name}' reactivated after inactive period")
            status = "aktiv"

        # ----------------------------------------------------------
        # aktiv → overdue
        # 28+ calendar days since last data, no outreach sent yet
        # ----------------------------------------------------------
        if status == "aktiv":
            if days_since_data is not None and days_since_data >= 28:
                if last_outreach is None or (last_data and last_outreach < last_data):
                    cursor.execute("""
                        UPDATE public.fb_dates
                        SET status = 'overdue', updated_at = NOW()
                        WHERE vessel_name = %s
                    """, (vessel_name,))
                    status = "overdue"
                    print(f"  → '{vessel_name}' moved to overdue")

        # ----------------------------------------------------------
        # overdue — check if should escalate, otherwise remind
        # ----------------------------------------------------------
        if status == "overdue":
            if last_outreach is not None and (last_data is None or last_outreach > last_data):
                if days_since_outreach is not None and days_since_outreach >= 3:
                    cursor.execute("""
                        UPDATE public.fb_dates
                        SET status = 'eskaleret', updated_at = NOW()
                        WHERE vessel_name = %s
                    """, (vessel_name,))
                    status = "eskaleret"
                    print(f"  → '{vessel_name}' moved to eskaleret")
            else:
                add_to_digest(responsible, "overdue", vessel_name, days_since_data, chief_engineer_name, chief_engineer_email)

        # ----------------------------------------------------------
        # eskaleret — check if should go kritisk, otherwise remind
        # ----------------------------------------------------------
        if status == "eskaleret":
            if last_followup is not None and (last_data is None or last_followup > last_data):
                if days_since_followup is not None and days_since_followup >= 3:
                    cursor.execute("""
                        UPDATE public.fb_dates
                        SET status = 'kritisk', updated_at = NOW()
                        WHERE vessel_name = %s
                    """, (vessel_name,))
                    status = "kritisk"
                    print(f"  → '{vessel_name}' moved to kritisk")
            else:
                add_to_digest(responsible, "escalated", vessel_name, days_since_outreach, chief_engineer_name, chief_engineer_email)

        # ----------------------------------------------------------
        # kritisk — flag daily for manual action
        # ----------------------------------------------------------
        if status == "kritisk":
            add_to_digest(responsible, "critical", vessel_name, days_since_followup, chief_engineer_name, chief_engineer_email)

    conn.commit()

    # ------------------------------------------------------------------
    # Send single digest email to mftservice
    # ------------------------------------------------------------------
    token = get_access_token()

    total = sum(len(b) for b in digest.values())
    if total == 0:
        print("  Nothing to report today.")
    else:
        print(f"\n  Sending digest to {DIGEST_RECIPIENT} ({total} vessel(s))")
        html = build_digest_email(
            overdue_vessels=sorted(digest["overdue"], key=lambda v: v["responsible"]),
            escalated_vessels=sorted(digest["escalated"], key=lambda v: v["responsible"]),
            critical_vessels=sorted(digest["critical"], key=lambda v: v["responsible"]),
        )
        send_email(
            token,
            to_address=DIGEST_RECIPIENT,
            subject=f"Daily Vessel Follow-up Digest — {now.strftime('%d %b %Y')}",
            html_body=html,
        )

    send_customer_outreach_emails(token, digest["overdue"], digest["escalated"])

    cursor.close()
    conn.close()
    print(f"\n[{datetime.now()}] Cron job finished.")

if __name__ == "__main__":
    main()
