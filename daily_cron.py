import os
import requests
import psycopg2
import psycopg2.extras
import mailbox_parser
from datetime import datetime, timezone, timedelta
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
# BUILD EMAIL BODY
# ---------------------------------------------------------------------------
def build_digest_email(overdue_vessels, escalated_vessels, critical_vessels):
    def vessel_rows(vessels):
        if not vessels:
            return "<tr><td colspan='3' style='color:#888;padding:8px;'>None</td></tr>"
        rows = ""
        for v in vessels:
            rows += f"""
            <tr>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['responsible']}</td>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['vessel_name']}</td>
                <td style='padding:8px;border-bottom:1px solid #eee;'>{v['days_since']} days</td>
            </tr>"""
        return rows

    def section_header(color, title, count, description):
        return f"""
        <table style='width:100%;border-collapse:collapse;margin-bottom:4px;'>
            <tr>
                <td style='width:14px;'>
                    <div style='width:12px;height:12px;background:{color};border-radius:2px;'></div>
                </td>
                <td style='padding-left:8px;'>
                    <span style='font-size:16px;font-weight:bold;color:{color};'>{title} ({count} vessels)</span>
                </td>
            </tr>
        </table>
        <p style='font-size:13px;color:#555;margin-top:2px;'>{description}</p>
        """

    inactive_link = """
        <p style='font-size:13px;color:#555;margin-top:8px;'>
            Do you want to change any of these vessels to inactive?
            <a href='https://feedback-report-generator.onrender.com/vessel-inactive'
               style='color:#003366;'>Use the following link</a>.
        </p>
    """

    return f"""
    <html>
    <body style='font-family:Arial,sans-serif;color:#333;max-width:700px;margin:0 auto;padding:20px;'>
        <h2 style='color:#003366;border-bottom:2px solid #003366;padding-bottom:8px;'>
            Daily Vessel Follow-up Digest
        </h2>
        <p>Good morning,<br>Here is your daily summary of vessels requiring attention.</p>

        {section_header('#cc0000', 'Overdue — Needs Outreach', len(overdue_vessels),
            'Data has not been received in 28+ days and no outreach has been sent.')}
        <table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:24px;'>
            <tr style='background:#f5f5f5;'>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Responsible</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Vessel</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Days Since Last Feedback</th>
            </tr>
            {vessel_rows(overdue_vessels)}
        </table>

        {section_header('#e68a00', 'Escalated — No Response to Outreach', len(escalated_vessels),
            'Outreach was sent but no data received within 3 business days.')}
        <table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:8px;'>
            <tr style='background:#f5f5f5;'>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Responsible</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Vessel</th>
                <th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;'>Days Since Outreach Sent</th>
            </tr>
            {vessel_rows(escalated_vessels)}
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
            </tr>
            {vessel_rows(critical_vessels)}
        </table>
        {inactive_link}
        <br>

        <p style='font-size:13px;color:#555;'>
            You can see a full overview of all vessels here:
            <a href='https://feedback-report-generator.onrender.com/vessel-overview'
               style='color:#003366;'>Vessel Overview</a>
        </p>

        <p style='font-size:12px;color:#aaa;border-top:1px solid #eee;padding-top:12px;'>
            This is an automated message from the Marine Fluid Technology data tracking system.
        </p>
    </body>
    </html>
    """

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
    # print("Running mailbox parser...")
    # mailbox_parser.main()

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
            MAX(frd.feedback_received_at) AS last_data_received
        FROM public.fb_dates fd
        LEFT JOIN public.legacy_vessels lv
            ON fd.vessel_name = lv.vessel_name
        LEFT JOIN public.fb_report_data frd
            ON fd.vessel_name = frd.vessel_name
        WHERE fd.status != 'inaktiv'
           OR (fd.status = 'inaktiv' AND fd.inaktiv_until <= NOW())
        GROUP BY
            fd.vessel_name,
            fd.status,
            fd.last_outreach_sent,
            fd.last_followup_sent,
            fd.inaktiv_until,
            lv.responsible
    """)
    vessels = cursor.fetchall()
    print(f"  Found {len(vessels)} vessel(s) to evaluate")

    # ------------------------------------------------------------------
    # Digest buckets — one list per status, all vessels in one email
    # ------------------------------------------------------------------
    digest = {"overdue": [], "escalated": [], "critical": []}

    def add_to_digest(responsible, bucket, vessel_name, days_since):
        digest[bucket].append({
            "vessel_name": vessel_name,
            "days_since":  days_since if days_since is not None else "N/A",
            "responsible": responsible,
        })

    for v in vessels:
        vessel_name   = v["vessel_name"]
        status        = v["status"]
        last_data     = v["last_data_received"]
        last_outreach = v["last_outreach_sent"]
        last_followup = v["last_followup_sent"]
        inaktiv_until = v["inaktiv_until"]
        responsible   = v["responsible"] or "no contact"

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

        days_since_data     = calendar_days_since(last_data)
        days_since_outreach = business_days_since(last_outreach)
        days_since_followup = business_days_since(last_followup)

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
                add_to_digest(responsible, "overdue", vessel_name, days_since_data)

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
                add_to_digest(responsible, "escalated", vessel_name, days_since_outreach)

        # ----------------------------------------------------------
        # kritisk — flag daily for manual action
        # ----------------------------------------------------------
        if status == "kritisk":
            add_to_digest(responsible, "critical", vessel_name, days_since_followup)

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
            overdue_vessels=digest["overdue"],
            escalated_vessels=digest["escalated"],
            critical_vessels=digest["critical"],
        )
        send_email(
            token,
            to_address=DIGEST_RECIPIENT,
            subject=f"Daily Vessel Follow-up Digest — {now.strftime('%d %b %Y')}",
            html_body=html,
        )

    cursor.close()
    conn.close()
    print(f"\n[{datetime.now()}] Cron job finished.")

if __name__ == "__main__":
    main()
