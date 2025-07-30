import psycopg
from .config import config
from contextlib import contextmanager

database_name = config['database']['database_name']


CREATE_USAGE_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage
    (project_id text PRIAMRY KEY,
    usage DOUBLE PRECISION,
    last_measurement DOUBLE_PRECISION,
    measurement_range TSTZRANGE)
    """

CREATE_USAGE_RECORD_TABLE = """
    CREATE TABLE IF NOT EXISTS coin_usage_record
    (project_id text PRIAMRY KEY,
    usage DOUBLE PRECISION,
    measurement_range TSTZRANGE)
    """

CREATE_USAGE_ENTRY = """
              INSERT INTO coin_usage
              (project_id, usage, last_measurement,
              measurement_range)
              VALUES (%s, %s, %s, %s)
              """

CREATE_USAGE_RECORD = """
              INSERT INTO coin_usage_record
              (project_id, usage,
              measurement_range)
              VALUES (%s, %s, %s)
              """

GET_ENTRY_BY_PROJECT_ID = "SELECT * FROM coin_usage_record WHERE project_id = %s"
GET_USAGE_RECORD_SINCE_TIME = "SELECT * FROM coing_usage_record WHERE project_id = %s"


@contextmanager
def cursor():
    with psycopg.connect(f'dbname={database_name} user=postgres') as conn:
        with conn.cursor() as cur:
            yield cur


with cursor() as cur:
    cur.execute(CREATE_USER_TABLE)
    cur.execute(CREATE_PROJECT_TABLE)


def get_user_by_suprid(suprid):
    with cursor() as cur:
        result = cur.execute(GET_USER_BY_SUPRID, (suprid)).fetchone()
    return dict(suprid=result[0], openstackid=result[1], username=result[2])


def get_user_by_openstackid(suprid):
    with cursor() as cur:
        result = cur.execute(GET_USER_BY_OPENSTACKID, (openstackid)).fetchone()
    return dict(suprid=result[0], openstackid=result[1], username=result[2])


def create_user(suprid, openstackid, username):
    with cursor() as cur:
        cur.execute(CREATE_USER, (suprid, openstackid, username))


def create_project(suprid, openstackid, projectname):
    with cursor() as cur:
        cur.execute(CREATE_PROJECT, (suprid, openstackid, projectname))


def get_project_by_suprid(suprid):
    with cursor() as cur:
        result = cur.execute(GET_PROJECT_BY_SUPRID, (suprid)).fetchone()
    return dict(suprid=result[0], openstackid=result[1], projectname=result[2])


def get_project_by_openstackid(suprid):
    with cursor() as cur:
        result = cur.execute(GET_PROJECT_BY_OPENSTACKID, (openstackid)).fetchone()
    return dict(suprid=result[0], openstackid=result[1], projectname=result[2])
