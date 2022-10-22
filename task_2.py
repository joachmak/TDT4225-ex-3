from sqlite3 import dbapi2
from unittest.util import sorted_list_difference
from utils.db_connector import DbConnector
from typing import Union
import utils.db as db
import utils.queries as queries
from pprint import pprint


def get_count(query: str, _db:DbConnector, msg: str=None):
    if msg is not None:
        print(msg)
    return db.execute_query_get_result(_db, query: str, print_success=False)[0][0]

def task2(db_conn:DbConnector):
    activities = get_count(queries.GET_ACTIVITY_COUNT, db_conn, "Getting activity count")
    users = get_count(queries.GET_USER_COUNT, db_conn, "Getting user count")
    print("The average amount of activities per user is " + (activities/users))

def main():
    conn:DbConnector=DbConnector()
    print("Hello")
    task2(conn)
    conn.close_connection()



if __name__ == "__main__":
    main()
