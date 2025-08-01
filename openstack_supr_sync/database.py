import psycopg
from .config import config
from contextlib import contextmanager
from datetime import timedelta, datetime
import logging

logger = logging.getLogger(__name__)

database_name = config['database']['database_name']


CREATE_USAGE_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage
    (project_id TEXT PRIMARY KEY,
    usage DOUBLE PRECISION,
    last_measurement DOUBLE PRECISION,
    measurement_range TSRANGE);
    """

CREATE_USAGE_RECORD_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage_record
    (project_id TEXT,
    usage DOUBLE PRECISION,
    measurement_range TSRANGE);
    """

CREATE_USAGE_ENTRY = """
              INSERT INTO coin_usage
              (project_id, usage, last_measurement,
              measurement_range)
              VALUES (%s, %s, %s, TSRANGE(%s, %s));
              """


UPDATE_USAGE_ENTRY = """
              UPDATE coin_usage SET
                  usage = usage + 0.5 * (last_measurement + %(new_measurement)s) *
                      (EXTRACT(EPOCH FROM (%(timestamp)s - UPPER(measurement_range)))::decimal / 3600),
                  last_measurement = %(new_measurement)s,
                  measurement_range = tsrange(LOWER(measurement_range), %(timestamp)s)
              WHERE project_id = %(project_id)s;
              """

CREATE_USAGE_RECORD = """
              INSERT INTO coin_usage_record
              (project_id, usage,
              measurement_range)
              VALUES (%s, %s, tsrange(%s, %s));
              """

# The one microsecond is requires due to technicalities in postgres
MIGRATE_ENTRIES_TO_RECORD = """
        WITH moved_rows AS (
            SELECT * FROM coin_usage
            WHERE
                LOWER(measurement_range) <= %(since_time)s
        )
        INSERT INTO coin_usage_record (project_id, usage, measurement_range)
        SELECT project_id, usage, measurement_range FROM moved_rows;
        UPDATE coin_usage
        SET
            usage = 0,
            measurement_range = tsrange(UPPER(measurement_range), UPPER(measurement_range) + '1 microsecond'::interval)
        WHERE
            LOWER(measurement_range) <= %(since_time)s
    """

GET_ENTRY_BY_PROJECT_ID = "SELECT * FROM coin_usage WHERE project_id = %s"
GET_ENTRY_RECORDS_BY_PROJECT_ID = "SELECT * FROM coin_usage_record WHERE project_id = %s"
GET_ENTRY_RECORDS_BY_PROJECT_ID_SINCE_TIME = """
                                  SELECT * FROM coin_usage_record
                                  WHERE
                                      project_id = %s AND
                                      UPPER(measurement_range) > %s
                                  ORDER BY UPPER(measurement_range);
                                  """
with psycopg.connect('user=postgres') as conn:
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
    with psycopg.connect(f'dbname={database_name} user=postgres') as conn:
        with conn.cursor() as cur:
            yield cur


with cursor() as cur:
    cur.execute(CREATE_USAGE_TABLE)
    cur.execute(CREATE_USAGE_RECORD_TABLE)


def get_entry_by_project_id(project_id):
    """
    Gets the current active entry for each project id.
    """
    with cursor() as cur:
        result = cur.execute(GET_ENTRY_BY_PROJECT_ID, (project_id,)).fetchone()
    return dict(project_id=result[0], usage=result[1],
                last_measurement=result[2], measurement_range=result[3])


def get_entry_records_by_project_id(project_id):
    """
    Gets the record of each project, consisting of "chunks" of usage
    integrated over timepsan.
    """
    with cursor() as cur:
        result = cur.execute(GET_ENTRY_RECORDS_BY_PROJECT_ID, (project_id,)).fetchall()
    return [dict(project_id=r[0], usage=r[1],
                 measurement_range=r[2]) for r in result]


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

    last_entry = get_entry_by_project_id(project_id)
    if last_entry['measurement_range'].lower < since_time:
        logger.warning('since_time is more recent than'
                       ' the start time of the most recent'
                       ' measurement, usage may be overestimated!')
        return last_entry['usage'] * estimate_fraction(last_entry)
    total_usage = last_entry['usage']
    with cursor() as cur:
        records = cur.execute(GET_ENTRY_RECORDS_BY_PROJECT_ID_SINCE_TIME, (project_id, since_time)).fetchall()
        records = [dict(project_id=r[0], usage=r[1],
                        measurement_range=r[2]) for r in records]
    # Already sorted
    total_usage += records[0]['usage'] * estimate_fraction(records[0])
    for entry in records[1:]:
        total_usage += entry['usage']
    return total_usage


def update_usage(project_id: str, usage_rate: float, timestamp: datetime):
    """
    Updates the resource usage of the project specified by the project ID,
    by giving the current usage rate and the timestamp of the measurement.
    The table is updated by trapezoidally integrating the usage rate (coins per hour)
    over the time from the time of the previous measurement to the time
    specified by `timestamp`, giving the estimated total usage for this time period.
    """
    with cursor() as cur:
        entry = cur.execute(GET_ENTRY_BY_PROJECT_ID, (project_id,)).fetchone()
        if entry is None:
            # Set upper bound of timestamp range to one microsecond ahead
            # corresponding to a single measured point in time
            # since range does not include upper bound
            one_microsecond_ahead = timestamp + timedelta(microseconds=1)
            # Integral over a point is always zero but rate is used
            # in next measurement
            cur.execute(CREATE_USAGE_ENTRY, (project_id, 0,
                        usage_rate, timestamp, one_microsecond_ahead))
        else:
            cur.execute(UPDATE_USAGE_ENTRY, dict(new_measurement=usage_rate,
                        timestamp=timestamp, project_id=project_id))


def migrate_usage_entries_to_record(since_time: datetime):
    """
    This migrates all entries which have been integrated over since at least the specified time
    to the `usage_records` table. The last measurement is kept track of in the `coin_usage`
    table for the purpose of trapezoidal quadrature.
    """
    with cursor() as cur:
        cur.execute(MIGRATE_ENTRIES_TO_RECORD,
                    dict(since_time=since_time,))
