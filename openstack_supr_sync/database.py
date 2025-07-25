import psycopg
import .config
from contextlib import contextmanager

database_name = config['database']['database_name']

# We need at least a minimal map of SUPRID -> openstackid

CREATE_USER_TABLE = """
                    CREATE TABLE IF NOT EXISTS users
                    (suprid integer PRIMARY KEY,
                    openstackid integer UNIQUE NOT NULL,
                    username TEXT)
                    """
CREATE_PROJECT_TABLE = """
                       CREATE TABLE IF NOT EXISTS projects
                       (suprid integer PRIMARY KEY,
                       openstackid integer UNIQUE NOT NULL,
                       projectname text)
                       """

CREATE_USER = """
              INSERT INTO users
              (suprid, openstackid, username)
              VALUES (%s, %s, %s)
              """
CREATE_PROJECT = "INSERT INTO projects (suprid, openstackid, projectname) VALUES (%s, %s, %s)"

GET_USER_BY_SUPRID = "SELECT * FROM users WHERE suprid = %s"
GET_USER_BY_OPENSTACKID = "SELECT * FROM users WHERE openstackid = %s"


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
