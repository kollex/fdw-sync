#!/usr/bin/env python3
import os
import json
import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── LOGGING SETUP ─────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='[%(asctime)s] [%(levelname)s] [fdw_sync] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("fdw_sync")


# ─── UTILITY FUNCTIONS ────────────────────────────────────────────────────────
def get_table_names(conn, schema, is_foreign=False):
    """Return a sorted list of table names in the given schema."""
    sql = """
        SELECT foreign_table_name AS table_name
          FROM information_schema.foreign_tables
         WHERE foreign_table_schema = %s;
    """ if is_foreign else """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = %s
           AND table_type = 'BASE TABLE';
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (schema,))
        return sorted(r["table_name"] for r in cur.fetchall())


def get_columns(conn, schema, table):
    """Return a sorted list of column names for a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name = %s
             ORDER BY ordinal_position;
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]


def drop_foreign_table(conn, schema, table):
    """Drop a foreign table if it exists."""
    with conn.cursor() as cur:
        cur.execute(f"DROP FOREIGN TABLE IF EXISTS {schema}.{table};")
    conn.commit()


def import_tables(conn, server, schema, tables, source_schema="public"):
    """Import missing tables via IMPORT FOREIGN SCHEMA ... LIMIT TO(...)."""
    if not tables:
        return False
    quoted = ", ".join(f'"{t}"' for t in tables)
    sql = (
        f"IMPORT FOREIGN SCHEMA {source_schema} "
        f"LIMIT TO ({quoted}) "
        f"FROM SERVER {server} "
        f"INTO {schema};"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    return True


def grant_to_bi_readonly(conn, schema, tables=None):
    """
    Grant USAGE on schema, then SELECT on each table to bi_readonly.
    If `tables` is None, grant SELECT on *all* user tables in `schema`.
    """
    role = "bi_readonly"
    with conn.cursor() as cur:
        # ensure the role exists
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (role,))
        if cur.fetchone() is None:
            logger.warning("Role %r not found; skipping grants.", role)
            return

        # 1) USAGE on schema
        cur.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role};")

        # 2) If no explicit list, fetch *all* non-pg_* tables
        if tables is None:
            cur.execute("""
                SELECT tablename
                  FROM pg_tables
                 WHERE schemaname = %s
                   AND tablename NOT LIKE 'pg_%%';
            """, (schema,))
            tables = [r[0] for r in cur.fetchall()]

        # 3) Grant SELECT per table
        for t in tables:
            cur.execute(f"GRANT SELECT ON {schema}.{t} TO {role};")

    conn.commit()


# ─── MAIN SYNC LOOP ────────────────────────────────────────────────────────────
def run_sync(sources):
    summary = {"imported": [], "reimported": [], "skipped": []}

    for cfg in sources:
        src_cfg     = cfg["source"]
        dwh_cfg     = cfg["dwh"]
        srv         = cfg["foreign_server"]
        fdw_schema  = cfg["foreign_schema"]
        src_schema  = cfg["source_schema"]

        logger.info("─" * 70)
        logger.info("Sync %s.%s → %s.%s",
                    src_cfg["dbname"], src_schema,
                    dwh_cfg["dbname"], fdw_schema)

        # 1) Connect
        try:
            src_conn = psycopg2.connect(**src_cfg)
        except Exception as e:
            logger.error("Source connect failed: %s", e)
            continue

        try:
            dwh_conn = psycopg2.connect(**dwh_cfg)
        except Exception as e:
            logger.error("DWH connect failed: %s", e)
            src_conn.close()
            continue

        with src_conn, dwh_conn:
            # 2) List source tables
            src_tbls = get_table_names(src_conn, src_schema, False)
            logger.info("Found %d source tables:", len(src_tbls))
            for t in src_tbls:
                logger.info("  - %s", t)

            # 3) List FDW tables
            dwh_tbls = get_table_names(dwh_conn, fdw_schema, True)
            logger.info("Found %d foreign tables:", len(dwh_tbls))
            for t in dwh_tbls:
                logger.info("  - %s", t)

            newly, reimpt, skipped = [], [], []

            # 4) Import missing
            missing = sorted(set(src_tbls) - set(dwh_tbls))
            if missing:
                logger.info("Missing → import: %s", missing)
                try:
                    if import_tables(dwh_conn, srv, fdw_schema, missing, src_schema):
                        logger.info("Imported: %s", missing)
                        newly += missing
                except Exception as e:
                    logger.error("Import failure %s: %s", missing, e)
                    dwh_conn.rollback()
            else:
                logger.info("No missing tables.")

            # 5) Re-import on schema drift
            for tbl in sorted(set(src_tbls) & set(dwh_tbls)):
                try:
                    a = get_columns(src_conn, src_schema, tbl)
                    b = get_columns(dwh_conn, fdw_schema, tbl)
                    if a != b:
                        logger.info("Drift %s: %d→%d cols", tbl, len(b), len(a))
                        drop_foreign_table(dwh_conn, fdw_schema, tbl)
                        logger.info("Dropped FDW %s.%s", fdw_schema, tbl)
                        if import_tables(dwh_conn, srv, fdw_schema, [tbl], src_schema):
                            logger.info("Re-imported: %s", tbl)
                            reimpt.append(tbl)
                    else:
                        skipped.append(tbl)
                except Exception as e:
                    logger.error("Error inspecting %s: %s", tbl, e)
                    dwh_conn.rollback()
                    skipped.append(tbl)

            # 6) Grant SELECT on newly imported *source* tables
            if newly:
                logger.info("Granting SELECT on new source tables to bi_readonly: %s", newly)
                grant_to_bi_readonly(src_conn, src_schema, newly)

            # 7) Grant SELECT on *all* FDW tables
            logger.info("Granting FDW SELECT to bi_readonly on %s", fdw_schema)
            grant_to_bi_readonly(dwh_conn, fdw_schema, None)

            # 8) Tally for summary
            summary["imported"]   += [(fdw_schema, t) for t in newly]
            summary["reimported"] += [(fdw_schema, t) for t in reimpt]
            summary["skipped"]    += [(fdw_schema, t) for t in skipped]

    # ─── FINAL SUMMARY ───────────────────────────────────────────────────────────
    logger.info("========== FDW Sync Summary ==========")
    if summary["imported"]:
        logger.info("Imported:")
        for s, t in summary["imported"]:
            logger.info("  - %s.%s", s, t)
    else:
        logger.info("Imported: None")

    if summary["reimported"]:
        logger.info("Re-imported:")
        for s, t in summary["reimported"]:
            logger.info("  - %s.%s", s, t)
    else:
        logger.info("Re-imported: None")

    if summary["skipped"]:
        logger.info("Skipped:")
        for s, t in summary["skipped"]:
            logger.info("  - %s.%s", s, t)
    else:
        logger.info("Skipped: None")
    logger.info("=======================================")

    # ─── STRUCTURED SUMMARY EVENT ────────────────────────────────────────────────
    summary_event = {
        "event":      "fdw_sync_summary",
        "new_tables": [t for (_, t) in summary["imported"]],
        "reimported": [t for (_, t) in summary["reimported"]],
    }
    # emits one JSON line you can alert on in Datadog
    logger.info(json.dumps(summary_event))


if __name__ == "__main__":
    try:
        cfg_path = os.environ.get("CONFIG_FILE", "/config/connections.json")
        with open(cfg_path) as f:
            sources = json.load(f)
        run_sync(sources)
        logger.info("FDW sync job completed successfully.")
    except Exception:
        logger.exception("FDW sync job crashed unexpectedly.")
        raise
