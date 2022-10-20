""" Db operations """
#A class for connecting to the mongodb database
#and performing operations on the database

from utils.db_connector import DbConnector


def insert_many(connection: DbConnector, collection: str, data: list):
    connection.db[collection].insert_many(data)


def get_all(connection: DbConnector, collection: str):
    return connection.db[collection].find({})
