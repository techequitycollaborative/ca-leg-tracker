"""
Input: None
Output: None (manipulates postgreSQL database)

Initialize a postgreSQL database with the schemas, tables, views, and permissions for v1 legislation tracker tool.
Reads relevant SQL files from the database-scripts folder, replaces placeholder strings with configured schema names, and extracts
active commands (ignoring commented lines).
"""
from config import config
import psycopg2

LEGTRACKER_SCHEMA = config('postgresql_schemas')['legtracker_schema']
FRONTEND_USER = config('postgresql_schemas')['frontend_user']
BACKEND_USER = config('postgresql_schemas')['backend_user']
create_tb_fd = open('../database-scripts/create-tables.sql', 'r')
create_tb_file = create_tb_fd.read()
create_tb_fd.close()
create_view_fd = open('../database-scripts/create-views.sql', 'r')
create_view_file = create_view_fd.read()
create_view_fd.close()
drop_fd = open('../database-scripts/drop-tables.sql', 'r')
drop_file = drop_fd.read()
drop_fd.close()


def extract_commands(stream):
    result = list()
    for command in stream:
        if len(command) == 0:
            continue
        elif command[0] == "-":
            continue
        else:
            command = command.strip().replace("[LEGTRACKER_SCHEMA]", LEGTRACKER_SCHEMA)
            command = command.replace("[FRONTEND_USER]", FRONTEND_USER)
            command = command.replace("[BACKEND_USER]", BACKEND_USER)
            result.append(command)
    return result


def main():
    conn = None
    command = None
    drop_commands = extract_commands(drop_file.split(';'))
    create_tb_commands = extract_commands(create_tb_file.split(';'))
    create_view_commands = extract_commands(create_view_file.split(';'))
    try:
        # read connection parameters
        params = config("postgresql")

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        for command in drop_commands:
            if len(command):
                cur.execute(command)
        for command in create_tb_commands:
            if len(command):
                cur.execute(command)
        for command in create_view_commands:
            if len(command):
                cur.execute(command)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Failed to update records", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')

    print('Update finished')


if __name__ == "__main__":
    main()
