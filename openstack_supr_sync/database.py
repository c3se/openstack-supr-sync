import psycopg
from psycopg.types.json import Jsonb
from .config import config, secrets
from contextlib import contextmanager
from datetime import timedelta, datetime
import logging

logger = logging.getLogger(__name__)


CREATE_USAGE_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage
    (project_id TEXT,
    instance_id TEXT PRIMARY KEY,
    metadata JSONB,
    usage DOUBLE PRECISION,
    last_measurement DOUBLE PRECISION,
    measurement_range TSRANGE);
    """

CREATE_USAGE_RECORD_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage_record
    (project_id TEXT,
    instance_id TEXT,
    metadata JSONB,
    usage DOUBLE PRECISION,
    measurement_range TSRANGE);
    """

CREATE_USAGE_ARCHIVE_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage_archive
    (project_id TEXT,
    instance_id TEXT,
    metadata JSONB,
    usage DOUBLE PRECISION,
    measurement_range TSRANGE,
    sgas_record TEXT);
    """

CREATE_BLOCK_STORAGE_RECORD_TABLE = """
    CREATE TABLE IF NOT EXISTS block_storage_record
    (project_id TEXT,
    instance_usage DOUBLE PRECISION,
    volume_usage DOUBLE PRECISION,
    backup_usage DOUBLE PRECISION,
    record_time TIMESTAMP);
    """

CREATE_BLOCK_STORAGE_ARCHIVE_TABLE = """
    CREATE TABLE IF NOT EXISTS block_storage_archive
    (project_id TEXT,
    instance_usage DOUBLE PRECISION,
    volume_usage DOUBLE PRECISION,
    backup_usage DOUBLE PRECISION,
    record_time TIMESTAMP,
    sgas_record TEXT);
    """

CREATE_BLOCK_STORAGE_RECORD = """
    INSERT INTO block_storage_record
    (project_id, instance_usage, volume_usage, backup_usage,
    record_time)
    VALUES (%s, %s, %s, %s, %s);
    """

CREATE_USAGE_ENTRY = """
    INSERT INTO coin_usage
    (project_id, instance_id, metadata, usage, last_measurement,
    measurement_range)
    VALUES (%s, %s, %s, %s, %s, TSRANGE(%s, %s));
    """


UPDATE_USAGE_ENTRY = """
    UPDATE coin_usage SET
        usage = usage + 0.5 * (last_measurement + %(new_measurement)s) *
            (EXTRACT(EPOCH FROM (%(timestamp)s - UPPER(measurement_range)))::decimal / 3600),
        last_measurement = %(new_measurement)s,
        measurement_range = tsrange(LOWER(measurement_range), %(timestamp)s)
    WHERE instance_id = %(instance_id)s;
    """

CREATE_USAGE_RECORD = """
    INSERT INTO coin_usage_record
    (project_id, instance_id, metadata, usage,
    measurement_range)
    VALUES (%s, %s, %s, %s, tsrange(%s, %s));
    """

# The one microsecond is requires due to technicalities in postgres
MIGRATE_ENTRIES_TO_RECORD = ("""
    WITH moved_rows AS (
        SELECT * FROM coin_usage
        WHERE
            LOWER(measurement_range) <= %(since_time)s
    )
    INSERT INTO coin_usage_record (project_id, instance_id, metadata, usage, measurement_range)
    SELECT project_id, instance_id, metadata, usage, measurement_range FROM moved_rows
    """,
                             """
    UPDATE coin_usage
        SET
            usage = 0,
            measurement_range = tsrange(
                UPPER(measurement_range), UPPER(measurement_range) + '1 microsecond'::interval)
        WHERE
            LOWER(measurement_range) <= %(since_time)s
    """)

# migrate record by record to archive on generating report
MIGRATE_RECORD_TO_ARCHIVE = """
    WITH selected_row AS (
        DELETE FROM coin_usage_record
        WHERE
            instance_id = %(instance_id)s
            AND
            measurement_range = tsrange(%(lower_ts)s, %(upper_ts)s)
        RETURNING *
    )
    INSERT INTO coin_usage_archive (project_id, instance_id, metadata, usage, measurement_range, %(xmlstring)s)
    SELECT project_id, instance_id, metadata, usage, measurement_range
    FROM selected_row;
    """

ARCHIVE_BLOCK_STORAGE_RECORDS = """
    WITH selected_rows AS (
        DELETE FROM block_storage_record
        WHERE
            project_id = %(project_id)s
            AND
            record_time = %(timestamp)s
        RETURNING *
    )
    INSERT INTO block_storage_archive (project_id, instance_usage, volume_usage, backup_usage, record_time, %(xmlstring)s)
    SELECT project_id, instance_usage, volume_usage, backup_usage, record_time
    FROM selected_rows;
    """

GET_ENTRY_BY_PROJECT_ID = "SELECT * FROM coin_usage WHERE project_id = %s"
GET_ENTRY_BY_INSTANCE_ID = "SELECT * FROM coin_usage WHERE instance_id = %s"
GET_ENTRY_RECORDS_BY_PROJECT_ID = "SELECT * FROM coin_usage_record WHERE project_id = %s"
GET_ENTRY_RECORDS = "SELECT * FROM coin_usage_record"
GET_BLOCK_STORAGE_RECORDS = "SELECT * FROM block_storage_record"
GET_ENTRY_RECORDS_BY_PROJECT_ID_SINCE_TIME = """
                                  SELECT * FROM coin_usage_record
                                  WHERE
                                      project_id = %s AND
                                      UPPER(measurement_range) > %s
                                  ORDER BY UPPER(measurement_range);
                                  """

database_host = config['database']['host']
database_port = config['database']['port']
database_name = config['database']['name']
database_user = config['database']['user']
database_password = secrets['database']['password']
with psycopg.connect(f'host={database_host} port={database_port} user={database_user} password={database_password}') as conn:
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (database_name,))
    conn.commit()
    exists = cur.fetchone()
    if exists is None:
        cur.execute(f'CREATE DATABASE {database_name};')


@contextmanager
def cursor():
    """
    Convenience context manager for psycopg.
    """
    with psycopg.connect(f'host={database_host} port={database_port} user={database_user} password={database_password}') as conn:
        with conn.cursor() as cur:
            yield cur


with cursor() as cur:
    cur.execute(CREATE_USAGE_TABLE)
    cur.execute(CREATE_USAGE_RECORD_TABLE)
    cur.execute(CREATE_USAGE_ARCHIVE_TABLE)
    cur.execute(CREATE_BLOCK_STORAGE_RECORD_TABLE)
    cur.execute(CREATE_BLOCK_STORAGE_ARCHIVE_TABLE)


def get_entry_by_project_id(project_id):
    """
    Gets the current active entry for each project id.
    """
    with cursor() as cur:
        result = cur.execute(GET_ENTRY_BY_PROJECT_ID, (project_id,)).fetchall()
    if result is None:
        return None
    return [dict(project_id=r[0], instance_id=r[1], metadata=r[2], usage=r[3],
                 measurement_range=r[4]) for r in result]


def get_entry_records_by_project_id(project_id):
    """
    Gets the record of each project, consisting of "chunks" of usage
    integrated over timepsan.
    """
    with cursor() as cur:
        result = cur.execute(GET_ENTRY_RECORDS_BY_PROJECT_ID, (project_id,)).fetchall()
    if result is None:
        return None
    return [dict(project_id=r[0], instance_id=r[1], metadata=r[2], usage=r[3],
                 measurement_range=r[4]) for r in result]


def get_usage_since_time(project_id: str, since_time: datetime):
    """
    Get the usage since a point in time. Interpolation with assumption
    of constant usage within the interval of a record will be used in the estimate.
    For example, if there are 10 records of 100 units of usage, each 1 hour long,
    and usage since the last 9.4 hours is requested, the result will be
    9 * 100 + 0.4 * 100 = 940.
    """
    def estimate_fraction(entry):
        """
        Estimate fraction of oldest measurement to include
        by assuming constant usage over interval
        """
        dt1 = (entry['measurement_range'].upper - entry['measurement_range'].lower).seconds
        if dt1 == 0:
            return 0
        dt2 = (entry['measurement_range'].upper - since_time).seconds
        return min(max(dt2 / dt1, 0), 1.)

    last_entries = get_entry_by_project_id(project_id)
    if len(last_entries) == 0:
        return None
    total_usage = 0.
    with cursor() as cur:
        records = cur.execute(GET_ENTRY_RECORDS_BY_PROJECT_ID_SINCE_TIME, (project_id, since_time)).fetchall()
    records = last_entries + [dict(usage=r[3],
                                   measurement_range=r[4]) for r in records]
    records = sorted(records, lambda r: r['measurement_range'].upper)
    if len(records) > 0:
        for entry in records[1:]:
            total_usage += entry['usage'] * estimate_fraction(entry)
    return total_usage


def update_usage(project_id: str, instance_id: str, metadata: dict,
                 usage_rate: float, timestamp: datetime):
    """
    Updates the resource usage of the project specified by the project ID,
    by giving the current usage rate and the timestamp of the measurement.
    The table is updated by trapezoidally integrating the usage rate (coins per hour)
    over the time from the time of the previous measurement to the time
    specified by `timestamp`, giving the estimated total usage for this time period.
    """
    with cursor() as cur:
        entry = cur.execute(GET_ENTRY_BY_INSTANCE_ID, (instance_id,)).fetchone()
        if entry is None:
            # Set upper bound of timestamp range to one microsecond ahead
            # corresponding to a single measured point in time
            # since range does not include upper bound
            one_microsecond_ahead = timestamp + timedelta(microseconds=1)
            # Integral over a point is always zero but rate is used
            # in next measurement
            cur.execute(CREATE_USAGE_ENTRY, (project_id, instance_id, Jsonb(metadata),
                        0., usage_rate, timestamp, one_microsecond_ahead))
        else:
            cur.execute(UPDATE_USAGE_ENTRY, dict(new_measurement=usage_rate,
                        timestamp=timestamp, instance_id=instance_id))


def migrate_usage_entries_to_record(since_time: datetime):
    """
    This migrates all entries which have been integrated over since at least the specified time
    to the `usage_records` table. The last measurement is kept track of in the `coin_usage`
    table for the purpose of trapezoidal quadrature.
    """
    with cursor() as cur:
        cur.execute(MIGRATE_ENTRIES_TO_RECORD[0],
                    dict(since_time=since_time,))
        cur.execute(MIGRATE_ENTRIES_TO_RECORD[1],
                    dict(since_time=since_time,))


def create_block_storage_record(project_id: str, instance_usage: float,
                                volume_usage: float, backup_usage: float,
                                timestamp: datetime):
    with cursor() as cur:
        cur.execute(CREATE_BLOCK_STORAGE_RECORD,
                    (project_id, instance_usage, volume_usage,
                     backup_usage, timestamp))


def get_block_storage_records():
    with cursor() as cur:
        records = cur.execute(GET_BLOCK_STORAGE_RECORDS).fetchall()
    return [dict(project_id=r[0], instance_usage=r[1], volume_usage=r[2],
                 backup_usage=r[3], timestamp=r[4]) for r in records]


def archive_block_storage_record(project_id: str, timestamp: datetime, xmlstring: str):
    with cursor() as cur:
        cur.execute(ARCHIVE_BLOCK_STORAGE_RECORDS,
                    dict(project_id=project_id, timestamp=timestamp, xmlstring=xmlstring))


def archive_entry(instance_id: str, lower_timestamp: datetime, upper_timestamp: datetime, xmlstring: str):
    """
    Archives one record entry.
    """
    with cursor() as cur:
        cur.execute(MIGRATE_RECORD_TO_ARCHIVE,
                    dict(instance_id=instance_id, lower_ts=lower_timestamp,
                         upper_ts=upper_timestamp, xmlstring=xmlstring))


def get_entry_records():
    with cursor() as cur:
        records = cur.execute(GET_ENTRY_RECORDS).fetchall()
    return [dict(project_id=r[0],
                 instance_id=r[1],
                 usage=r[3],
                 start_time=r[4].lower,
                 stop_time=r[4].upper,
                 **r[2]) for r in records]
