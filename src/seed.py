"""
Seed — populates the database with patients, studies, EHR endpoints, and enrollments.

Safe to run multiple times — all inserts use ON CONFLICT DO NOTHING.
EHR base URL is read from EHR_API_URL env var so it works in both Docker
(http://mock-ehr:8000) and local dev (http://localhost:8000).
"""
import os
import psycopg2
import psycopg2.extras

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/healthex")


def seed(conn):
    with conn.cursor() as cur:

        # EHR endpoints
        ehr_url = os.getenv("EHR_API_URL", "http://mock-ehr:8000")
        cur.execute("""
            INSERT INTO ehr_endpoints (id, name, base_url, rate_limit_rpm) VALUES
                ('00000000-0000-0000-0000-000000000001', 'epic',     %s, 100),
                ('00000000-0000-0000-0000-000000000002', 'cerner',   %s, 30),
                ('00000000-0000-0000-0000-000000000003', 'regional', %s, 30)
            ON CONFLICT DO NOTHING
        """, (ehr_url, ehr_url, ehr_url))

        # Studies — including a fast-cycling one for demo
        cur.execute("""
            INSERT INTO studies (id, name, refresh_frequency) VALUES
                ('00000000-0000-0000-0001-000000000001', 'Daily Cardiology Study',   '1 day'),
                ('00000000-0000-0000-0001-000000000002', 'Weekly Oncology Study',    '7 days'),
                ('00000000-0000-0000-0001-000000000003', 'Bi-weekly Diabetes Study', '14 days'),
                ('00000000-0000-0000-0001-000000000004', 'Continuous Monitoring',    '30 seconds')
            ON CONFLICT DO NOTHING
        """)

        # 30 patients
        patients = [
            ("pat-001", "Alice Johnson"),
            ("pat-002", "Bob Smith"),
            ("pat-003", "Carol White"),
            ("pat-004", "David Brown"),
            ("pat-005", "Eva Martinez"),
            ("pat-006", "Frank Lee"),
            ("pat-007", "Grace Kim"),
            ("pat-008", "Henry Davis"),
            ("pat-009", "Iris Wilson"),
            ("pat-010", "Jack Taylor"),
            ("pat-011", "Karen Moore"),
            ("pat-012", "Leo Garcia"),
            ("pat-013", "Maya Patel"),
            ("pat-014", "Nathan Clark"),
            ("pat-015", "Olivia Scott"),
            ("pat-016", "Paul Adams"),
            ("pat-017", "Quinn Baker"),
            ("pat-018", "Rachel Hall"),
            ("pat-019", "Sam Turner"),
            ("pat-020", "Tina Wright"),
            ("pat-021", "Uma Harris"),
            ("pat-022", "Victor Lewis"),
            ("pat-023", "Wendy Young"),
            ("pat-024", "Xander King"),
            ("pat-025", "Yara Lopez"),
            ("pat-026", "Zoe Hill"),
            ("pat-027", "Aaron Green"),
            ("pat-028", "Bella Cruz"),
            ("pat-029", "Carlos Reed"),
            ("pat-030", "Diana Foster"),
        ]
        patient_ids = {}
        for ext_id, name in patients:
            cur.execute("""
                INSERT INTO patients (external_id, name)
                VALUES (%s, %s)
                ON CONFLICT (external_id) DO NOTHING
                RETURNING id, external_id
            """, (ext_id, name))
            row = cur.fetchone()
            if row:
                patient_ids[ext_id] = row[0]

        cur.execute("SELECT id, external_id FROM patients WHERE external_id = ANY(%s)",
                    ([p[0] for p in patients],))
        for row in cur.fetchall():
            patient_ids[row[1]] = row[0]

        # Assign endpoints — spread across epic/cerner/regional
        endpoint_name_to_id = {
            "epic":     "00000000-0000-0000-0000-000000000001",
            "cerner":   "00000000-0000-0000-0000-000000000002",
            "regional": "00000000-0000-0000-0000-000000000003",
        }
        endpoint_assignments = {
            "pat-001": ["epic"],           "pat-002": ["cerner"],
            "pat-003": ["epic", "cerner"], "pat-004": ["regional"],
            "pat-005": ["epic"],           "pat-006": ["cerner", "regional"],
            "pat-007": ["epic"],           "pat-008": ["regional"],
            "pat-009": ["epic", "regional"], "pat-010": ["cerner"],
            "pat-011": ["epic"],           "pat-012": ["cerner"],
            "pat-013": ["regional"],       "pat-014": ["epic", "cerner"],
            "pat-015": ["epic"],           "pat-016": ["regional"],
            "pat-017": ["cerner"],         "pat-018": ["epic"],
            "pat-019": ["regional"],       "pat-020": ["epic", "cerner"],
            "pat-021": ["epic"],           "pat-022": ["cerner"],
            "pat-023": ["regional"],       "pat-024": ["epic"],
            "pat-025": ["cerner", "regional"], "pat-026": ["epic"],
            "pat-027": ["regional"],       "pat-028": ["cerner"],
            "pat-029": ["epic"],           "pat-030": ["regional"],
        }
        for ext_id, endpoint_names in endpoint_assignments.items():
            for ename in endpoint_names:
                cur.execute("""
                    INSERT INTO patient_ehr_endpoints (patient_id, ehr_endpoint_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (patient_ids[ext_id], endpoint_name_to_id[ename]))

        # Enroll all patients in the continuous monitoring study (all overdue)
        # and a subset in the other studies
        study_daily    = "00000000-0000-0000-0001-000000000001"
        study_weekly   = "00000000-0000-0000-0001-000000000002"
        study_biweekly = "00000000-0000-0000-0001-000000000003"
        study_continuous = "00000000-0000-0000-0001-000000000004"

        # All 30 in continuous monitoring — always overdue after 30s
        for ext_id, _ in patients:
            pid = patient_ids[ext_id]
            cur.execute("""
                INSERT INTO patient_study_enrollments (patient_id, study_id, last_refreshed_at, consent_expires_at)
                VALUES (%s, %s, NULL, NOW() + INTERVAL '90 days')
                ON CONFLICT (patient_id, study_id) DO NOTHING
            """, (pid, study_continuous))

        # First 10 in daily/weekly/biweekly with varied refresh states
        legacy_enrollments = [
            ("pat-001", study_daily,    "NOW() - INTERVAL '2 days'",  "NOW() + INTERVAL '30 days'"),
            ("pat-002", study_daily,    "NOW() - INTERVAL '3 days'",  "NOW() + INTERVAL '30 days'"),
            ("pat-003", study_weekly,   "NOW() - INTERVAL '8 days'",  "NOW() + INTERVAL '60 days'"),
            ("pat-004", study_weekly,   None,                          "NOW() + INTERVAL '60 days'"),
            ("pat-005", study_biweekly, "NOW() - INTERVAL '15 days'", "NOW() + INTERVAL '7 days'"),
            ("pat-006", study_daily,    "NOW() - INTERVAL '1 hour'",  "NOW() + INTERVAL '30 days'"),
            ("pat-007", study_weekly,   None,                          "NOW() + INTERVAL '45 days'"),
            ("pat-008", study_biweekly, "NOW() - INTERVAL '20 days'", "NOW() + INTERVAL '90 days'"),
            ("pat-009", study_daily,    "NOW() - INTERVAL '2 days'",  "NOW() + INTERVAL '30 days'"),
            ("pat-010", study_weekly,   "NOW() - INTERVAL '10 days'", "NOW() + INTERVAL '14 days'"),
        ]
        for ext_id, study_id, last_refreshed, consent_expires in legacy_enrollments:
            pid = patient_ids[ext_id]
            if last_refreshed:
                cur.execute(f"""
                    INSERT INTO patient_study_enrollments (patient_id, study_id, last_refreshed_at, consent_expires_at)
                    VALUES (%s, %s, {last_refreshed}, {consent_expires})
                    ON CONFLICT (patient_id, study_id) DO NOTHING
                """, (pid, study_id))
            else:
                cur.execute(f"""
                    INSERT INTO patient_study_enrollments (patient_id, study_id, last_refreshed_at, consent_expires_at)
                    VALUES (%s, %s, NULL, {consent_expires})
                    ON CONFLICT (patient_id, study_id) DO NOTHING
                """, (pid, study_id))

        conn.commit()
        total_enrollments = len(patients) + len(legacy_enrollments)
        print("Seed complete.")
        print(f"  {len(patients)} patients")
        print(f"  3 EHR endpoints (epic, cerner, regional)")
        print(f"  4 studies (daily, weekly, bi-weekly, continuous 30s)")
        print(f"  {total_enrollments} enrollments")


if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    try:
        seed(conn)
    finally:
        conn.close()
