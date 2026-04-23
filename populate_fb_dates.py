import os
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATABASE_URL  = os.environ["DATABASE_URL"]

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch all vessels from legacy_vessels
    cursor.execute("""
        SELECT DISTINCT vessel_name
        FROM public.legacy_vessels
        WHERE vessel_name IS NOT NULL
    """)
    vessels = cursor.fetchall()
    print(f"Found {len(vessels)} vessel(s) in legacy_vessels")

    inserted = 0
    skipped = 0

    for v in vessels:
        vessel_name = v["vessel_name"]

        # Insert only if not already in fb_dates
        cursor.execute("""
            INSERT INTO public.fb_dates (vessel_name, status, created_at, updated_at)
            VALUES (%s, 'aktiv', NOW(), NOW())
            ON CONFLICT (vessel_name) DO NOTHING
        """, (vessel_name,))

        if cursor.rowcount == 1:
            inserted += 1
            print(f"  → Inserted: '{vessel_name}'")
        else:
            skipped += 1
            print(f"  → Skipped (already exists): '{vessel_name}'")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\nDone. {inserted} inserted, {skipped} skipped.")

if __name__ == "__main__":
    main()