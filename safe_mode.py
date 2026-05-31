import streamlit as st
from datetime import date, timedelta
import psycopg2
import pandas as pd
import random
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

if "power_off_analysis" not in st.session_state:
    st.session_state["power_off_analysis"] = None

if "eligible_meters_by_sla" not in st.session_state:
    st.session_state["eligible_meters_by_sla"] = {}


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
# FETCH METERS FOR EACH SLA TIER
# =========================
def fetch_meters_by_sla(task, selected_date, guid=None):
    """Fetch meter IDs for each SLA tier"""
    try:
        conn = get_connection()
        cur = conn.cursor()

        if task == "Alert / Tamper":
            query = """
            WITH sla_base AS (
                SELECT
                    createddate::date AS job_day,
                    meterid,
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

            categorized AS (
                SELECT
                    sla_base.meterid,
                    CASE
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '30 minutes' 
                            THEN '30 Min SLA'
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '60 minutes' 
                            THEN '60 Min SLA'
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '360 minutes' 
                            THEN '6 Hour SLA'
                        ELSE 'SLA Breach'
                    END AS sla_tier
                FROM sla_base, min_date
            )

            SELECT sla_tier, ARRAY_AGG(DISTINCT meterid) as meter_ids
            FROM categorized
            GROUP BY sla_tier
            ORDER BY CASE 
                WHEN sla_tier = '30 Min SLA' THEN 1
                WHEN sla_tier = '60 Min SLA' THEN 2
                WHEN sla_tier = '6 Hour SLA' THEN 3
                ELSE 4
            END
            """
            cur.execute(query, (task, selected_date, guid))

        elif task == "Instant,Load Survey,":
            query = """
            WITH sla_base AS (
                SELECT
                    createddate::date AS job_day,
                    meterid,
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

            categorized AS (
                SELECT
                    sla_base.meterid,
                    CASE
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '120 minutes' 
                            THEN '2 Hour SLA'
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '240 minutes' 
                            THEN '4 Hour SLA'
                        ELSE 'SLA Breach'
                    END AS sla_tier
                FROM sla_base, min_date
            )

            SELECT sla_tier, ARRAY_AGG(DISTINCT meterid) as meter_ids
            FROM categorized
            GROUP BY sla_tier
            ORDER BY CASE 
                WHEN sla_tier = '2 Hour SLA' THEN 1
                WHEN sla_tier = '4 Hour SLA' THEN 2
                ELSE 3
            END
            """
            cur.execute(query, (task, selected_date, guid))

        elif task == "serviceFirmwareUpdate":
            query = """
            WITH sla_base AS (
                SELECT
                    createddate::date AS job_day,
                    meterid,
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

            categorized AS (
                SELECT
                    sla_base.meterid,
                    CASE
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '480 minutes' 
                            THEN '8 Hour SLA'
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes' 
                            THEN '24 Hour SLA'
                        ELSE 'SLA Breach'
                    END AS sla_tier
                FROM sla_base, min_date
            )

            SELECT sla_tier, ARRAY_AGG(DISTINCT meterid) as meter_ids
            FROM categorized
            GROUP BY sla_tier
            ORDER BY CASE 
                WHEN sla_tier = '8 Hour SLA' THEN 1
                WHEN sla_tier = '24 Hour SLA' THEN 2
                ELSE 3
            END
            """
            cur.execute(query, (task, selected_date))

        elif task == "serviceSetLoadLimit":
            query = """
            WITH sla_base AS (
                SELECT
                    createddate::date AS job_day,
                    meterid,
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

            categorized AS (
                SELECT
                    sla_base.meterid,
                    CASE
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1080 minutes' 
                            THEN '18 Hour SLA'
                        WHEN sla_base.completiondatetime <= DATE_TRUNC('second', min_date.min_createddate) + INTERVAL '1440 minutes' 
                            THEN '24 Hour SLA'
                        ELSE 'SLA Breach'
                    END AS sla_tier
                FROM sla_base, min_date
            )

            SELECT sla_tier, ARRAY_AGG(DISTINCT meterid) as meter_ids
            FROM categorized
            GROUP BY sla_tier
            ORDER BY CASE 
                WHEN sla_tier = '18 Hour SLA' THEN 1
                WHEN sla_tier = '24 Hour SLA' THEN 2
                ELSE 3
            END
            """
            cur.execute(query, (task, selected_date, guid))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        meters_by_sla = {}
        for row in rows:
            sla_tier = row[0]
            meter_ids = row[1] if row[1] else []
            meters_by_sla[sla_tier] = meter_ids

        return meters_by_sla

    except Exception as e:
        st.error(f"Error fetching meters by SLA: {e}")
        return {}


# =========================
# CHECK POWER OFF METERS (PARALLEL EXECUTION)
# =========================
def check_power_off_meters(meterid_list, test_start, test_end):
    """
    Check which meters are powered off during the test window.
    Returns list of (meterid, meter_power_state) tuples
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Format meter IDs for SQL query
        if not meterid_list:
            return []

        meter_ids_str = "','".join(str(m) for m in meterid_list)

        query = f"""
        WITH params AS (
            SELECT
                TIMESTAMP %s AS test_start,
                TIMESTAMP %s AS test_end
        ),

        tmp_meters AS (
            SELECT UNNEST(ARRAY['{meter_ids_str}']) AS meterid
        ),

        pre_status AS (
            SELECT
                e.meterid,
                MAX(CASE WHEN e.alarm_names LIKE '%METER_FIRST_BREATH%' 
                         THEN e.alarm_date END) AS last_on_before,
                MAX(CASE WHEN e.alarm_names LIKE '%METER_LAST_GASP%' 
                         THEN e.alarm_date END) AS last_off_before
            FROM profile.alarm_notification_details e
            JOIN params p ON TRUE
            WHERE e.alarm_date < p.test_start
              AND e.meterid IN (SELECT meterid FROM tmp_meters)
            GROUP BY e.meterid
        ),

        pre_status_code AS (
            SELECT
                meterid,
                CASE
                    WHEN last_on_before IS NULL AND last_off_before IS NULL THEN 0
                    WHEN last_on_before > last_off_before THEN 1
                    ELSE 0
                END AS pre_status_code,
                GREATEST(
                    COALESCE(last_on_before, '1900-01-01'),
                    COALESCE(last_off_before, '1900-01-01')
                ) AS pre_status_time,
                CASE
                    WHEN last_on_before > last_off_before THEN last_off_before
                    ELSE last_on_before
                END AS pre_opposite_status_time
            FROM pre_status
        ),

        all_flips AS (
            SELECT
                e.meterid,
                e.alarm_date,
                CASE
                    WHEN e.alarm_names LIKE '%METER_FIRST_BREATH%' THEN 1
                    ELSE 0
                END AS state
            FROM profile.alarm_notification_details e
            JOIN params p ON TRUE
            WHERE e.alarm_date BETWEEN p.test_start AND p.test_end
              AND e.meterid IN (SELECT meterid FROM tmp_meters)
              AND (
                    e.alarm_names LIKE '%METER_FIRST_BREATH%'
                 OR e.alarm_names LIKE '%METER_LAST_GASP%'
              )
        ),

        flip_count_calc AS (
            SELECT meterid, COUNT(*) AS flip_count
            FROM all_flips
            GROUP BY meterid
        ),

        flip_agg AS (
            SELECT
                meterid,
                STRING_AGG(
                    'METER_FIRST_BREATH @ ' ||
                    TO_CHAR(alarm_date,'DD-MM-YYYY HH24:MI:SS'),
                    ', ' ORDER BY alarm_date DESC
                ) FILTER (WHERE state = 1) AS first_breath_times,
                STRING_AGG(
                    'METER_LAST_GASP @ ' ||
                    TO_CHAR(alarm_date,'DD-MM-YYYY HH24:MI:SS'),
                    ', ' ORDER BY alarm_date DESC
                ) FILTER (WHERE state = 0) AS last_gasp_times
            FROM all_flips
            GROUP BY meterid
        ),

        latest_event AS (
            SELECT DISTINCT ON (meterid)
                meterid,
                state AS latest_state,
                alarm_date AS latest_event_time
            FROM all_flips
            ORDER BY meterid, alarm_date DESC
        ),

        import_check AS (
            SELECT
                tm.meterid,
                CASE WHEN COUNT(fd.*) > 0 THEN 'Yes' ELSE 'No' END AS import_in_window,
                MIN(fd.das_rtc) AS first_import_time
            FROM tmp_meters tm
            LEFT JOIN profile.file_details fd
              ON fd.meter_id = tm.meterid
             AND fd.das_rtc BETWEEN (SELECT test_start FROM params)
                                 AND (SELECT test_end FROM params)
            GROUP BY tm.meterid
        )

        SELECT
            tm.meterid,
            CASE
                WHEN COALESCE(fc.flip_count,0) >= 1 THEN
                    'Meter is Powered On but communication is intermittent'
                WHEN ic.import_in_window = 'Yes'
                     AND COALESCE(le.latest_state, psc.pre_status_code) = 1 THEN
                    'Meter is Powered On'
                WHEN ic.import_in_window = 'Yes'
                     AND COALESCE(le.latest_state, psc.pre_status_code) = 0 THEN
                    'Meter is Powered On (First Breath alert possibly missed)'
                WHEN ic.import_in_window = 'No'
                     AND COALESCE(le.latest_state, psc.pre_status_code) = 0 THEN
                    'Meter is Powered Off'
                WHEN ic.import_in_window = 'No'
                     AND COALESCE(le.latest_state, psc.pre_status_code) = 1 THEN
                    'Meter is Powered Off (Last Gasp alert possibly missed)'
                ELSE
                    'Other than rules defined'
            END AS meter_power_state

        FROM tmp_meters tm
        LEFT JOIN pre_status_code psc ON psc.meterid = tm.meterid
        LEFT JOIN flip_count_calc fc ON fc.meterid = tm.meterid
        LEFT JOIN flip_agg fa ON fa.meterid = tm.meterid
        LEFT JOIN latest_event le ON le.meterid = tm.meterid
        LEFT JOIN import_check ic ON ic.meterid = tm.meterid
        ORDER BY tm.meterid
        """

        cur.execute(query, (str(test_start), str(test_end)))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return rows

    except Exception as e:
        st.error(f"Error checking power off meters: {e}")
        return []


def run_parallel_power_check(meters_by_sla, min_createddate, task):
    """
    Run power off meter checks in parallel for each SLA tier
    """
    results = {}
    lock = threading.Lock()

    def check_sla_tier(sla_tier, meter_ids):
        if not meter_ids or len(meter_ids) == 0:
            return sla_tier, [], []

        # Calculate test window based on SLA tier
        if task == "Alert / Tamper":
            if sla_tier == "30 Min SLA":
                test_end = min_createddate + timedelta(minutes=30)
            elif sla_tier == "60 Min SLA":
                test_end = min_createddate + timedelta(minutes=60)
            elif sla_tier == "6 Hour SLA":
                test_end = min_createddate + timedelta(minutes=360)
            else:
                test_end = min_createddate + timedelta(minutes=360)

        elif task == "Instant,Load Survey,":
            if sla_tier == "2 Hour SLA":
                test_end = min_createddate + timedelta(minutes=120)
            elif sla_tier == "4 Hour SLA":
                test_end = min_createddate + timedelta(minutes=240)
            else:
                test_end = min_createddate + timedelta(minutes=240)

        elif task == "serviceFirmwareUpdate":
            if sla_tier == "8 Hour SLA":
                test_end = min_createddate + timedelta(minutes=480)
            elif sla_tier == "24 Hour SLA":
                test_end = min_createddate + timedelta(minutes=1440)
            else:
                test_end = min_createddate + timedelta(minutes=1440)

        elif task == "serviceSetLoadLimit":
            if sla_tier == "18 Hour SLA":
                test_end = min_createddate + timedelta(minutes=1080)
            elif sla_tier == "24 Hour SLA":
                test_end = min_createddate + timedelta(minutes=1440)
            else:
                test_end = min_createddate + timedelta(minutes=1440)

        # Run power off check
        power_check_results = check_power_off_meters(
            meter_ids,
            min_createddate,
            test_end
        )

        # Separate powered off and eligible meters
        powered_off = []
        eligible = []

        for meterid, power_state in power_check_results:
            if "Powered Off" in power_state:
                powered_off.append(meterid)
            else:
                eligible.append(meterid)

        return sla_tier, powered_off, eligible

    # Execute parallel checks
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(check_sla_tier, sla_tier, meter_ids): sla_tier
            for sla_tier, meter_ids in meters_by_sla.items()
        }

        for future in as_completed(futures):
            sla_tier, powered_off, eligible = future.result()
            with lock:
                results[sla_tier] = {
                    "powered_off": powered_off,
                    "eligible": eligible
                }

    return results


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
            "Total records to update": updated,
            "Under 30 mins SLA": updated_30,
            "Under 60 mins SLA": updated_60,
            "Under 6 hours SLA": updated_6h
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
            "Total records to update": updated,
            "Under 2 hour SLA": updated_2hour,
            "Under 4 hour SLA": updated_4hour
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
            "Total records to update": updated,
            "Under 8 hours SLA": updated_8hour,
            "Under 24 hours SLA": updated_24hour
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
            "Total records to update": updated,
            "Under 18 hours SLA": updated_18hour,
            "Under 24 hours SLA": updated_24hour
        }


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

col1, col2, col3, col4 = st.columns([2.2, 2.2, 1.6, 2.8], gap="small")

# -------------------------
# TC NO
# -------------------------
with col1:
    st.caption("SAT TC No")
    selected_tc = st.selectbox(
        "",
        list(task_mapping.keys()),
        label_visibility="collapsed"
    )

task = task_mapping[selected_tc]

# -------------------------
# DATE
# -------------------------
with col2:
    st.caption("Date")
    selected_date = st.date_input(
        "",
        value=date.today(),
        label_visibility="collapsed"
    )

# -------------------------
# FETCH GUID BUTTON
# -------------------------
with col3:
    st.caption("Action")

    guid_button_disabled = selected_tc == "TC-10"

    if st.button(
        "🔍 Fetch GUID",
        use_container_width=True,
        disabled=guid_button_disabled
    ):
        fetched_guid = fetch_auto_guid(task, selected_date)
        if fetched_guid:
            st.session_state["auto_guid"] = fetched_guid
        else:
            st.warning("No GUID found for this date and task")

# -------------------------
# GUID INPUT
# -------------------------
with col4:
    st.caption("GUID")

    guid = st.text_input(
        "",
        value=st.session_state.get("auto_guid", ""),
        disabled=(selected_tc == "TC-10"),
        label_visibility="collapsed"
    )

st.divider()

# -------------------------
# ACTION BUTTONS
# -------------------------
col5, col6 = st.columns(2)

with col5:
    preview = st.button("Preview SLA", use_container_width=True)

with col6:
    execute = st.button("Execute Safe Mode", use_container_width=True)


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
        else:
            df = run_sla_query(task, selected_date, guid)
    
    if not df.empty:
        st.dataframe(df)
    else:
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

        # ====================================
        # FETCH METERS BY SLA AND RUN POWER CHECK IN PARALLEL
        # ====================================
        with st.spinner("Analyzing meter power states..."):
            try:
                # Get min createddate for test window calculation
                conn = get_connection()
                cur = conn.cursor()

                if task == "serviceFirmwareUpdate":
                    cur.execute("""
                        SELECT DATE_TRUNC('second', MIN(createddate))
                        FROM public.jobscheduleslog
                        WHERE mdastaskname = %s AND createddate::date = %s
                    """, (task, selected_date))
                else:
                    cur.execute("""
                        SELECT DATE_TRUNC('second', MIN(createddate))
                        FROM public.jobscheduleslog
                        WHERE mdastaskname = %s AND createddate::date = %s AND guid = %s
                    """, (task, selected_date, guid))

                min_result = cur.fetchone()
                min_createddate = min_result[0] if min_result and min_result[0] else pd.Timestamp.now()
                cur.close()
                conn.close()

                # Fetch meters by SLA tier
                meters_by_sla = fetch_meters_by_sla(task, selected_date, guid if task != "serviceFirmwareUpdate" else None)

                # Run parallel power checks
                power_analysis = run_parallel_power_check(meters_by_sla, min_createddate, task)

                st.session_state["power_off_analysis"] = power_analysis
                st.session_state["eligible_meters_by_sla"] = {
                    sla: analysis.get("eligible", [])
                    for sla, analysis in power_analysis.items()
                }

            except Exception as e:
                st.error(f"Error during power analysis: {e}")

        st.rerun()
    else:
        st.warning("No records found")


# =========================
# RESULT
# =========================
if st.session_state.get("execution_result"):
    st.success("Execution Completed (NOT committed yet)")

    result_dict = st.session_state["execution_result"]

    result_df = pd.DataFrame(
        list(result_dict.items()),
        columns=["Metric", "Value"]
    )

    st.dataframe(result_df, use_container_width=True)

    # ====================================
    # POWER OFF ANALYSIS DISPLAY
    # ====================================
    if st.session_state.get("power_off_analysis"):
        st.subheader("⚡ Meter Power State Analysis")

        power_analysis = st.session_state["power_off_analysis"]

        # Create analysis summary table
        analysis_data = []
        for sla_tier, analysis in sorted(power_analysis.items()):
            powered_off = analysis.get("powered_off", [])
            eligible = analysis.get("eligible", [])
            total = len(powered_off) + len(eligible)

            analysis_data.append({
                "SLA Tier": sla_tier,
                "Total Meters": total,
                "Power Off Meters": len(powered_off),
                "After Removing Power Off": len(eligible)
            })

        analysis_summary_df = pd.DataFrame(analysis_data)
        st.dataframe(analysis_summary_df, use_container_width=True)

        st.divider()

        # ====================================
        # DOWNLOAD OPTIONS
        # ====================================
        st.subheader("📥 Download Options")

        col_downloads_1, col_downloads_2 = st.columns(2)

        # Download Updated Meter SLA File (Eligible Meters)
        with col_downloads_1:
            if st.session_state.get("download_df") is not None:
                csv = st.session_state["download_df"].to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="⬇️ Download Updated Meter SLA File",
                    data=csv,
                    file_name="sla_updated_meters.csv",
                    mime="text/csv",
                    use_container_width=True
                )

        # Download Power Off Meters Summary
        with col_downloads_2:
            if power_analysis:
                poweroff_summary_list = []
                for sla_tier, analysis in power_analysis.items():
                    powered_off = analysis.get("powered_off", [])
                    for meter in powered_off:
                        poweroff_summary_list.append({
                            "SLA Tier": sla_tier,
                            "Meter ID": meter,
                            "Status": "Powered Off"
                        })

                if poweroff_summary_list:
                    poweroff_df = pd.DataFrame(poweroff_summary_list)
                    csv_poweroff = poweroff_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="⬇️ Download Power Off Meters",
                        data=csv_poweroff,
                        file_name="power_off_meters.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No powered off meters found")

        st.divider()

        # ====================================
        # DETAILED VIEW BY SLA TIER
        # ====================================
        st.subheader("🔍 Detailed Meter List by SLA Tier")

        for sla_tier, analysis in sorted(power_analysis.items()):
            with st.expander(f"**{sla_tier}**"):
                tab1, tab2 = st.tabs(["Eligible Meters", "Power Off Meters"])

                with tab1:
                    eligible = analysis.get("eligible", [])
                    if eligible:
                        eligible_df = pd.DataFrame(
                            {"Meter ID": eligible}
                        )
                        st.dataframe(eligible_df, use_container_width=True)
                    else:
                        st.info("No eligible meters in this tier")

                with tab2:
                    powered_off = analysis.get("powered_off", [])
                    if powered_off:
                        poweroff_df = pd.DataFrame(
                            {"Meter ID": powered_off}
                        )
                        st.dataframe(poweroff_df, use_container_width=True)
                    else:
                        st.info("No powered off meters in this tier")


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
            st.session_state["power_off_analysis"] = None
            st.session_state["eligible_meters_by_sla"] = {}
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
            st.session_state["power_off_analysis"] = None
            st.session_state["eligible_meters_by_sla"] = {}
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
