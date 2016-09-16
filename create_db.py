""" Script for table creation. """

import ConfigParser
import sqlite3

config = ConfigParser.ConfigParser()
config.read('configs.conf')

conn = sqlite3.connect(config.get('db', 'sqlite_db'))
cursor = conn.cursor()

# Hack to emulate enum type in SQLite
cursor.execute("""CREATE TABLE IF NOT EXISTS object_types
                  (id INTEGER PRIMARY KEY AUTOINCREMENT, object_type TEXT)""")

cursor.execute("""INSERT INTO object_types(object_type)
                  VALUES('env'), ('farm'), ('farm_role'), ('server')""")

cursor.execute("""CREATE TABLE IF NOT EXISTS billing_aggregation
                  (object_type TEXT NOT NULL, object_id VARCHAR(32), cost REAL,
                   FOREIGN KEY(object_type) REFERENCES object_types(id))""")

conn.commit()

conn.close()
