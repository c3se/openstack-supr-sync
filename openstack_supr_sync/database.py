import psycopg

# We need at least a minimal map of SUPRID -> openstackid

CREATE_USER_TABLE = "CREATE TABLE users (suprid integer PRIMARY KEY, openstackid integer UNIQUE, username TEXT)"
CREATE_PROJECT_TABLE = "CREATE TABLE projects (suprid integer PRIMARY KEY, openstackid integer UNIQUE, projectname text)"

CREATE_USER = "INSERT INTO users (suprid, openstackid, username) VALUES (%s, %s, %s)"
CREATE_PROJECT = "INSERT INTO projects (suprid, openstackid, projectname) VALUES (%s, %s, %s)"
