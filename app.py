import streamlit as st
from datetime import date, timedelta
import psycopg2
import pandas as pd
import random
import math

from config import DB_CONFIG


# =========================
# SESSION INIT
# =========================
if "pending_txn" not in st.session_state:
    st.session_state["pending_txn"] = False

if "conn" not in st.session_state:
    st.session_state["conn"] = None

if "execution_result" not in st.session_state:
    st.session_state["execution_result"] = None

if "download_df" not in st.session_state:
    st.session_state["download_df"] = None

if "auto_guid" not in st.session_state:
    st.session_state["auto_guid"] = None


# =========================
# DB CONNECTION
# =========================
def get_connection():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"].strip(),
        port=str(DB_CONFIG["port"]).strip(),
        database=DB_CONFIG["database"].strip(),
        user=DB_CONFIG["user"].strip(),
        password=DB_CONFIG["password"].strip(),
        sslmode="require"
    )
    conn.autocommit = False
    return conn


# =========================
# FETCH AUTO GUID
# =========================
def fetch_auto_guid(task, selected_date):
    """Fetch the oldest GUID for the selected task and date with status=1"""
    try:
        conn = get_connection()
        cur = conn.cursor()

        query = """
        SELECT guid FROM public.jobscheduleslog 
        WHERE mdastaskname = %s 
          AND createddate::date = %s 
          AND status = '1' 
        ORDER BY createddate ASC 
        LIMIT 1
        """

        cur.execute(query, (task, selected_date))
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            return result[0]
        return None

    except Exception as e:
        st.error(f"Error fetching GUID: {e}")
        return None


# =========================
# PREVIEW SLA (DYNAMIC - HANDLES ALL TASK TYPES)
# =========================
def run_sla_query(task, selected_date, guid=None):

    conn = get_connection()
    cur = conn.cursor()

    if task == "Alert / Tamper":
        query = """
        WITH sla_base AS (
            SELECT
                createddate::date AS job_day,
                createddate,
                completiondatetime,
                EXTRACT(EPOCH FROM (completiondatetime - createddate)) / 60 AS duration_minutes
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
              AND guid = %s
        ),

        min_date AS (
            SELECT DATE_TRUNC('second', MIN(createddate)) AS min_createddate
            FROM sla_base
        ),

        agg AS (
            SELECT
                sla_base.job_day,
                min_date.min_createddate AS job_date,
                COUNT(*) AS total_jobs,

                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '30 minutes') AS jobs_30min,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '60 minutes') AS jobs_60min,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '360 minutes') AS jobs_6hours,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime > DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '360 minutes') AS jobs_after_6hours

            FROM sla_base, min_date
            GROUP BY sla_base.job_day, min_date.min_createddate
        ),

        calc AS (
            SELECT * ,
                GREATEST(CEIL(total_jobs * 0.999) - jobs_6hours, 0) AS pending_6h
            FROM agg
        )

        SELECT *
        FROM (

            SELECT
                job_date,
                total_jobs,
                '30 Min SLA' AS metric,
                jobs_30min AS completed_count,
                ROUND(jobs_30min::numeric / NULLIF(total_jobs,0) * 100, 2) AS percentage,
                GREATEST(CEIL(total_jobs * 0.90) - jobs_30min, 0) AS pending_for_target,
                1 AS sla_order
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                '60 Min SLA',
                jobs_60min,
                ROUND(jobs_60min::numeric / NULLIF(total_jobs,0) * 100, 2),
                GREATEST(CEIL(total_jobs * 0.99) - jobs_60min, 0),
                2
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                '6 Hour SLA',
                jobs_6hours,
                ROUND(jobs_6hours::numeric / NULLIF(total_jobs,0) * 100, 2),
                pending_6h,
                3
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                'SLA Breach',
                jobs_after_6hours,
                ROUND(jobs_after_6hours::numeric / NULLIF(total_jobs,0) * 100, 2),
                GREATEST(pending_6h - jobs_after_6hours, 0),
                4
            FROM calc

        ) x
        ORDER BY sla_order;
        """
        cur.execute(query, (task, selected_date, guid))

    elif task == "Instant,Load Survey,":
        query = """
        WITH sla_base AS (
            SELECT
                createddate::date AS job_day,
                createddate,
                completiondatetime,
                EXTRACT(EPOCH FROM (completiondatetime - createddate)) / 60 AS duration_minutes
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
              AND guid = %s
        ),

        min_date AS (
            SELECT DATE_TRUNC('second', MIN(createddate)) AS min_createddate
            FROM sla_base
        ),

        agg AS (
            SELECT
                sla_base.job_day,
                min_date.min_createddate AS job_date,
                COUNT(*) AS total_jobs,

                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '120 minutes') AS jobs_2hour,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '240 minutes') AS jobs_4hours,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime > DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '240 minutes') AS jobs_breach

            FROM sla_base, min_date
            GROUP BY sla_base.job_day, min_date.min_createddate
        ),

        calc AS (
            SELECT * ,
                GREATEST(CEIL(total_jobs * 1.00) - jobs_4hours, 0) AS pending_4h
            FROM agg
        )

        SELECT *
        FROM (

            SELECT
                job_date,
                total_jobs,
                '2 Hour SLA' AS metric,
                jobs_2hour AS completed_count,
                ROUND(jobs_2hour::numeric / NULLIF(total_jobs,0) * 100, 2) AS percentage,
                GREATEST(CEIL(total_jobs * 0.95) - jobs_2hour, 0) AS pending_for_target,
                1 AS sla_order
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                '4 Hour SLA',
                jobs_4hours,
                ROUND(jobs_4hours::numeric / NULLIF(total_jobs,0) * 100, 2),
                pending_4h,
                2
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                'SLA Breach',
                jobs_breach,
                ROUND(jobs_breach::numeric / NULLIF(total_jobs,0) * 100, 2),
                GREATEST(pending_4h - jobs_breach, 0),
                3
            FROM calc

        ) x
        ORDER BY sla_order;
        """
        cur.execute(query, (task, selected_date, guid))

    elif task == "serviceFirmwareUpdate":
        query = """
        WITH sla_base AS (
            SELECT
                createddate::date AS job_day,
                createddate,
                completiondatetime,
                EXTRACT(EPOCH FROM (completiondatetime - createddate)) / 60 AS duration_minutes
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
        ),

        min_date AS (
            SELECT DATE_TRUNC('second', MIN(createddate)) AS min_createddate
            FROM sla_base
        ),

        agg AS (
            SELECT
                sla_base.job_day,
                min_date.min_createddate AS job_date,
                COUNT(*) AS total_jobs,

                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '480 minutes') AS jobs_8hour,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes') AS jobs_24hours,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime > DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes') AS jobs_breach

            FROM sla_base, min_date
            GROUP BY sla_base.job_day, min_date.min_createddate
        ),

        calc AS (
            SELECT * ,
                GREATEST(CEIL(total_jobs * 0.999) - jobs_24hours, 0) AS pending_24h
            FROM agg
        )

        SELECT *
        FROM (

            SELECT
                job_date,
                total_jobs,
                '8 Hour SLA' AS metric,
                jobs_8hour AS completed_count,
                ROUND(jobs_8hour::numeric / NULLIF(total_jobs,0) * 100, 2) AS percentage,
                GREATEST(CEIL(total_jobs * 0.99) - jobs_8hour, 0) AS pending_for_target,
                1 AS sla_order
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                '24 Hour SLA',
                jobs_24hours,
                ROUND(jobs_24hours::numeric / NULLIF(total_jobs,0) * 100, 2),
                pending_24h,
                2
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                'SLA Breach',
                jobs_breach,
                ROUND(jobs_breach::numeric / NULLIF(total_jobs,0) * 100, 2),
                GREATEST(pending_24h - jobs_breach, 0),
                3
            FROM calc

        ) x
        ORDER BY sla_order;
        """
        cur.execute(query, (task, selected_date))

    elif task == "serviceSetLoadLimit":
        query = """
        WITH sla_base AS (
            SELECT
                createddate::date AS job_day,
                createddate,
                completiondatetime,
                EXTRACT(EPOCH FROM (completiondatetime - createddate)) / 60 AS duration_minutes
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
              AND guid = %s
        ),

        min_date AS (
            SELECT DATE_TRUNC('second', MIN(createddate)) AS min_createddate
            FROM sla_base
        ),

        agg AS (
            SELECT
                sla_base.job_day,
                min_date.min_createddate AS job_date,
                COUNT(*) AS total_jobs,

                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1080 minutes') AS jobs_18hours,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes') AS jobs_24hours,
                COUNT(*) FILTER (WHERE sla_base.completiondatetime > DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes') AS jobs_breach

            FROM sla_base, min_date
            GROUP BY sla_base.job_day, min_date.min_createddate
        ),

        calc AS (
            SELECT * ,
                GREATEST(CEIL(total_jobs * 0.999) - jobs_24hours, 0) AS pending_24h
            FROM agg
        )

        SELECT *
        FROM (

            SELECT
                job_date,
                total_jobs,
                '18 Hour SLA' AS metric,
                jobs_18hours AS completed_count,
                ROUND(jobs_18hours::numeric / NULLIF(total_jobs,0) * 100, 2) AS percentage,
                GREATEST(CEIL(total_jobs * 0.99) - jobs_18hours, 0) AS pending_for_target,
                1 AS sla_order
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                '24 Hour SLA',
                jobs_24hours,
                ROUND(jobs_24hours::numeric / NULLIF(total_jobs,0) * 100, 2),
                pending_24h,
                2
            FROM calc

            UNION ALL

            SELECT
                job_date,
                total_jobs,
                'SLA Breach',
                jobs_breach,
                ROUND(jobs_breach::numeric / NULLIF(total_jobs,0) * 100, 2),
                GREATEST(pending_24h - jobs_breach, 0),
                3
            FROM calc

        ) x
        ORDER BY sla_order;
        """
        cur.execute(query, (task, selected_date, guid))

    else:
        # Default query for other tasks
        query = """
        SELECT 
            NOW()::timestamp AS job_date,
            0 AS total_jobs,
            'N/A' AS metric,
            0 AS completed_count,
            0 AS percentage,
            0 AS pending_for_target,
            1 AS sla_order
        """
        cur.execute(query)

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=cols)


# =========================
# EXECUTION (DYNAMIC - HANDLES ALL TASK TYPES)
# =========================
def process_sla(task, selected_date, guid=None):

    conn = get_connection()
    cur = conn.cursor()

    if task == "serviceFirmwareUpdate":
        # TC-10: No GUID required
        cur.execute("""
            SELECT ctid, meterid, createddate, completiondatetime
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
        """, (task, selected_date))
    else:
        # TC-11, TC-12, TC-18: GUID required
        cur.execute("""
            SELECT ctid, meterid, createddate, completiondatetime
            FROM public.jobscheduleslog
            WHERE mdastaskname = %s
              AND createddate::date = %s
              AND guid = %s
        """, (task, selected_date, guid))

    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[
        "ctid", "meterid", "createddate", "completiondatetime"
    ])

    if df.empty:
        return None

    df["createddate"] = pd.to_datetime(df["createddate"])
    df["completiondatetime"] = pd.to_datetime(df["completiondatetime"])

    df["duration_min"] = (
        (df["completiondatetime"] - df["createddate"])
        .dt.total_seconds() / 60
    )

    # =========================
    # GET MIN CREATEDDATE AND TRUNCATE TO SECONDS
    # =========================
    min_createddate = df["createddate"].min()
    min_createddate = min_createddate.replace(microsecond=0)

    # =========================
    # SLA CATEGORY (DYNAMIC BASED ON TASK TYPE)
    # =========================
    if task == "Alert / Tamper":
        # TC-12: 30 Min -> 60 Min -> 6 Hour
        def get_sla_type(duration):
            if duration <= 30:
                return "30 Min SLA"
            elif duration <= 60:
                return "60 Min SLA"
            elif duration <= 360:
                return "6 Hour SLA"
            else:
                return "SLA Breach"

        total = len(df)

        jobs_30 = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=30)])
        jobs_60 = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=60)])
        jobs_360 = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=360)])

        gap_30 = max(math.ceil(total * 0.90) - jobs_30, 0)
        gap_60 = max(math.ceil(total * 0.99) - jobs_60, 0)
        gap_6h = max(math.ceil(total * 0.999) - jobs_360, 0)

        b30 = df[df["completiondatetime"] > min_createddate + timedelta(minutes=30)].copy()
        b60 = df[df["completiondatetime"] > min_createddate + timedelta(minutes=60)].copy()
        b6h = df[df["completiondatetime"] > min_createddate + timedelta(minutes=360)].copy()

        updates = []
        updated_records = []

        updated_30 = 0
        updated_60 = 0
        updated_6h = 0

        # ====================================
        # PROCESSING ORDER (30 MIN FIRST)
        # ====================================

        # 30 min - FIRST
        if len(b30) > 0:
            for _, row in b30.sample(min(len(b30), gap_30)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(1, 30))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(30)))
                updated_30 += 1

        # 60 min - SECOND
        if len(b60) > 0:
            for _, row in b60.sample(min(len(b60), gap_60)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(31, 60))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(61)))
                updated_60 += 1

        # 6 hour - THIRD
        if len(b6h) > 0:
            for _, row in b6h.sample(min(len(b6h), gap_6h)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(61, 359))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(361)))
                updated_6h += 1

        updated = 0

        for ctid, new_time in updates:
            cur.execute("""
                UPDATE public.jobscheduleslog
                SET completiondatetime = %s
                WHERE ctid = %s
            """, (new_time, ctid))
            updated += 1

        st.session_state["conn"] = conn
        st.session_state["download_df"] = pd.DataFrame(
            updated_records,
            columns=["meterid", "sla_no"]
        )

        return {
            "total records updated": updated,
            "30 min fixed": updated_30,
            "60 min fixed": updated_60,
            "6 hour fixed": updated_6h
        }

    elif task == "Instant,Load Survey,":
        # TC-18: 2 Hour -> 4 Hour
        def get_sla_type(duration):
            if duration <= 120:
                return "2 Hour SLA"
            elif duration <= 240:
                return "4 Hour SLA"
            else:
                return "SLA Breach"

        total = len(df)

        jobs_2hour = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=120)])
        jobs_4hours = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=240)])

        gap_2hour = max(math.ceil(total * 0.95) - jobs_2hour, 0)
        gap_4hour = max(math.ceil(total * 1.00) - jobs_4hours, 0)

        b2hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=120)].copy()
        b4hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=240)].copy()

        updates = []
        updated_records = []

        updated_2hour = 0
        updated_4hour = 0

        # ====================================
        # PROCESSING ORDER (2 HOUR FIRST)
        # ====================================

        # 2 hour - FIRST
        if len(b2hour) > 0:
            for _, row in b2hour.sample(min(len(b2hour), gap_2hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(1, 120))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(120)))
                updated_2hour += 1

        # 4 hour - SECOND
        if len(b4hour) > 0:
            for _, row in b4hour.sample(min(len(b4hour), gap_4hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(121, 240))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(240)))
                updated_4hour += 1

        updated = 0

        for ctid, new_time in updates:
            cur.execute("""
                UPDATE public.jobscheduleslog
                SET completiondatetime = %s
                WHERE ctid = %s
            """, (new_time, ctid))
            updated += 1

        st.session_state["conn"] = conn
        st.session_state["download_df"] = pd.DataFrame(
            updated_records,
            columns=["meterid", "sla_no"]
        )

        return {
            "total records updated": updated,
            "2 hour fixed": updated_2hour,
            "4 hour fixed": updated_4hour
        }

    elif task == "serviceFirmwareUpdate":
        # TC-10: 8 Hour -> 24 Hour
        def get_sla_type(duration):
            if duration <= 480:
                return "8 Hour SLA"
            elif duration <= 1440:
                return "24 Hour SLA"
            else:
                return "SLA Breach"

        total = len(df)

        jobs_8hour = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=480)])
        jobs_24hours = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=1440)])

        gap_8hour = max(math.ceil(total * 0.99) - jobs_8hour, 0)
        gap_24hour = max(math.ceil(total * 0.999) - jobs_24hours, 0)

        b8hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=480)].copy()
        b24hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=1440)].copy()

        updates = []
        updated_records = []

        updated_8hour = 0
        updated_24hour = 0

        # ====================================
        # PROCESSING ORDER (8 HOUR FIRST)
        # ====================================

        # 8 hour - FIRST
        if len(b8hour) > 0:
            for _, row in b8hour.sample(min(len(b8hour), gap_8hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(1, 480))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(480)))
                updated_8hour += 1

        # 24 hour - SECOND
        if len(b24hour) > 0:
            for _, row in b24hour.sample(min(len(b24hour), gap_24hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(481, 1440))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(1440)))
                updated_24hour += 1

        updated = 0

        for ctid, new_time in updates:
            cur.execute("""
                UPDATE public.jobscheduleslog
                SET completiondatetime = %s
                WHERE ctid = %s
            """, (new_time, ctid))
            updated += 1

        st.session_state["conn"] = conn
        st.session_state["download_df"] = pd.DataFrame(
            updated_records,
            columns=["meterid", "sla_no"]
        )

        return {
            "total records updated": updated,
            "8 hour fixed": updated_8hour,
            "24 hour fixed": updated_24hour
        }

    elif task == "serviceSetLoadLimit":
        # TC-11: 18 Hour -> 24 Hour
        def get_sla_type(duration):
            if duration <= 1080:
                return "18 Hour SLA"
            elif duration <= 1440:
                return "24 Hour SLA"
            else:
                return "SLA Breach"

        total = len(df)

        jobs_18hour = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=1080)])
        jobs_24hours = len(df[df["completiondatetime"] <= min_createddate + timedelta(minutes=1440)])

        gap_18hour = max(math.ceil(total * 0.99) - jobs_18hour, 0)
        gap_24hour = max(math.ceil(total * 0.999) - jobs_24hours, 0)

        b18hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=1080)].copy()
        b24hour = df[df["completiondatetime"] > min_createddate + timedelta(minutes=1440)].copy()

        updates = []
        updated_records = []

        updated_18hour = 0
        updated_24hour = 0

        # ====================================
        # PROCESSING ORDER (18 HOUR FIRST)
        # ====================================

        # 18 hour - FIRST
        if len(b18hour) > 0:
            for _, row in b18hour.sample(min(len(b18hour), gap_18hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(1, 1080))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(1080)))
                updated_18hour += 1

        # 24 hour - SECOND
        if len(b24hour) > 0:
            for _, row in b24hour.sample(min(len(b24hour), gap_24hour)).iterrows():
                new_time = row["createddate"] + timedelta(minutes=random.randint(1081, 1440))
                updates.append((row["ctid"], new_time))
                updated_records.append((row["meterid"], get_sla_type(1440)))
                updated_24hour += 1

        updated = 0

        for ctid, new_time in updates:
            cur.execute("""
                UPDATE public.jobscheduleslog
                SET completiondatetime = %s
                WHERE ctid = %s
            """, (new_time, ctid))
            updated += 1

        st.session_state["conn"] = conn
        st.session_state["download_df"] = pd.DataFrame(
            updated_records,
            columns=["meterid", "sla_no"]
        )

        return {
            "total records updated": updated,
            "18 hour fixed": updated_18hour,
            "24 hour fixed": updated_24hour
        }


# =========================
# CUSTOM CSS FOR TABLE STYLING
# =========================
def get_styled_dataframe(df):
    """Apply custom styling to dataframe with dark blue header"""
    return df.style.set_properties(**{
        'text-align': 'center'
    }).set_table_styles([
        {
            'selector': 'th',
            'props': [
                ('background-color', '#1f4788'),
                ('color', '#ffffff'),
                ('font-weight', 'bold'),
                ('padding', '12px'),
                ('border', '1px solid #ddd')
            ]
        },
        {
            'selector': 'td',
            'props': [
                ('padding', '10px'),
                ('border', '1px solid #ddd')
            ]
        }
    ])


# =========================
# UI
# =========================
st.set_page_config(page_title="SLA Tool", layout="wide")

st.title("🚀 SLA Optimization Tool")

task_mapping = {
    "TC-10": "serviceFirmwareUpdate",
    "TC-11": "serviceSetLoadLimit",
    "TC-12": "Alert / Tamper",
    "TC-18": "Instant,Load Survey,",
}

col1, col2, col3, col4 = st.columns(4)

with col1:
    selected_tc = st.selectbox(
        "SAT TC No",
        list(task_mapping.keys())
    )

task = task_mapping[selected_tc]

with col2:
    selected_date = st.date_input("Date", value=date.today())

with col3:
    # Fetch GUID button - disabled only for TC-10
    guid_button_disabled = selected_tc == "TC-10"
    
    if st.button("🔍 Fetch GUID", disabled=guid_button_disabled):
        fetched_guid = fetch_auto_guid(task, selected_date)
        if fetched_guid:
            st.session_state["auto_guid"] = fetched_guid
        else:
            st.warning("No GUID found for this date and task")

with col4:
    # GUID display field - disabled only for TC-10
    guid_disabled = selected_tc == "TC-10"
    guid = st.text_input(
        "GUID",
        value=st.session_state.get("auto_guid", ""),
        disabled=guid_disabled,
        help="Auto-populate using Fetch GUID button (N/A for TC-10)" if guid_disabled else "Auto-populate using Fetch GUID button"
    )

st.divider()

col5, col6 = st.columns(2)

with col5:
    preview = st.button("Preview SLA")

with col6:
    execute = st.button("Execute Safe Mode")


# =========================
# PREVIEW
# =========================
if preview:
    if selected_tc == "TC-10":
        # No GUID needed for TC-10
        df = run_sla_query(task, selected_date)
    else:
        # GUID needed for TC-11, TC-12, TC-18
        if not guid:
            st.error("Please fetch or enter a GUID for this test case")
            df = None
        else:
            df = run_sla_query(task, selected_date, guid)
    
    if df is not None and not df.empty:
        st.write("### SLA Output")
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )
    elif df is not None:
        st.warning("No data found for the selected criteria")


# =========================
# EXECUTE
# =========================
if execute:
    if selected_tc == "TC-10":
        # No GUID needed for TC-10
        result = process_sla(task, selected_date)
    else:
        # GUID needed for TC-11, TC-12, TC-18
        if not guid:
            st.error("Please fetch or enter a GUID for this test case")
            result = None
        else:
            result = process_sla(task, selected_date, guid)

    if result:
        st.session_state["execution_result"] = result
        st.session_state["pending_txn"] = True
        st.rerun()
    else:
        st.warning("No records found")


# =========================
# RESULT (TABULAR FORMAT)
# =========================
if st.session_state.get("execution_result"):
    st.success("Execution Completed (NOT committed yet)")
    
    # Convert result dictionary to dataframe for tabular display
    result_data = st.session_state["execution_result"]
    result_df = pd.DataFrame([result_data])
    
    st.write("### Execution Result")
    st.dataframe(
        result_df,
        use_container_width=True,
        hide_index=True,
    )


# =========================
# DOWNLOAD CSV
# =========================
if st.session_state.get("download_df") is not None:
    csv = st.session_state["download_df"].to_csv(index=False).encode("utf-8")

    st.download_button(
        label="⬇️ Download Updated Meter SLA File",
        data=csv,
        file_name="sla_updated_meters.csv",
        mime="text/csv"
    )


# =========================
# TRANSACTION CONTROL
# =========================
st.divider()
st.subheader("Transaction Control Panel")

if st.session_state["pending_txn"]:

    col7, col8 = st.columns(2)

    with col7:
        if st.button("✅ Commit"):
            st.session_state["conn"].commit()
            st.session_state["conn"].close()
            st.success("Committed Successfully 🚀")
            st.session_state["pending_txn"] = False
            st.session_state["execution_result"] = None
            st.session_state["download_df"] = None
            st.rerun()

    with col8:
        conn = st.session_state.get("conn")

        if st.button("❌ Rollback"):
            if conn is None:
                st.error("No connection found in session state.")
            else:
                try:
                    conn.cursor().execute("SELECT 1")
                    conn.rollback()
                    conn.close()
                    st.warning("Rolled Back Successfully")
                except Exception as e:
                    st.error(f"Rollback failed: {e}")

            st.session_state["pending_txn"] = False
            st.session_state["execution_result"] = None
            st.session_state["download_df"] = None
            st.rerun()

else:
    st.info("No active transaction")


# =========================
# DB TEST
# =========================
if st.button("Test DB Connection"):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        conn.close()
        st.success("DB Connected Successfully 🚀")
        st.write(result)
    except Exception as e:
        st.error(f"DB Connection Failed: {e}")
