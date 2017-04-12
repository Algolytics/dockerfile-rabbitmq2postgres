#!/usr/bin/env python
#-*- coding: utf-8 -*-

import psycopg2
import sys
import pika
import json
import logging
from sets import Set
import time

logging.basicConfig(level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')

class PostgresHelper:
    def __init__(self, postgres_dbname, postgres_user, postgres_password, postgres_host, postgres_schema):
        self.postgres_dbname = postgres_dbname
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_host = postgres_host
        self.postgres_schema = postgres_schema
        self.tableinfo = {}
        self.data_buffer = []
        self.max_data_buffer_size = 100

    def create_postgres_connection(self):
        logging.info("Creating postgres connection...")
        self.conn = psycopg2.connect(
            "dbname=" + self.postgres_dbname + " user=" + self.postgres_user + " password=" + self.postgres_password + " host=" + self.postgres_host)
        self.cursor = self.conn.cursor()
        logging.info("Postgres connection created.")

    def insert_data_element(self, table, data):
        if not table in self.tableinfo:
            self.tableinfo[table] = Set()
        for k, v in data.items():
            self.tableinfo[table].add(str(k))
        self.data_buffer.append((table, data))
        if len(self.data_buffer) >= self.max_data_buffer_size:
            self.insert_data_buffer()
            self.tableinfo.clear()
            del self.data_buffer[:]

    def insert_data_buffer(self):
        for table_name, columns_insert in self.tableinfo.items():
            try:
                logging.info("Synchronizing table " + str(table_name) + "...")
                q_create_table = "CREATE TABLE IF NOT EXISTS " + str(self.postgres_schema) + "." + str(table_name) + " (id SERIAL PRIMARY KEY, " + " text default null, ".join(
                    [str(k1) for k1 in columns_insert]) + " text default null)"
                logging.debug(str(q_create_table))
                self.cursor.execute(q_create_table)
                q_get_columns = "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + str(self.postgres_schema) + "' AND table_name = '" + str(table_name) + "'"
                logging.debug(str(q_get_columns))
                self.cursor.execute(q_get_columns)
                columns_existing = [k[0] for k in self.cursor.fetchall()]
                for k in columns_insert:
                    if not k.lower() in columns_existing:
                        q_add_column = "ALTER TABLE " + str(self.postgres_schema) + "." + str(table_name) + " ADD COLUMN " + str(k) + " text default null"
                        logging.debug(str(q_add_column))
                        self.cursor.execute(q_add_column)
                self.conn.commit()
            except Exception, e:
                logging.error("Unexpected error when synchronizing table " + str(table_name) + ": " + e.pgerror)
                raise e

            try:
                logging.info("Inserting buffered data into table " + str(table_name) + "...")
#                insert_list = []
                c = 0
                for d in self.data_buffer:
                    if d[0] == table_name:
#                        insert_list.append([str(v) for k, v in d[1].items()])
                        q_insert = "INSERT INTO " + str(self.postgres_schema) + "." + str(table_name) + " (" + ", ".join([str(k) for k, v in d[1].items()]) + ") VALUES (" + ", ".join(["%s" for k, v in d[1].items()]) + ");"
                        q_values = [v for k, v in d[1].items()]
                        self.cursor.execute(q_insert, q_values)
                        c = c + 1
#                q_insert = "INSERT INTO " + str(self.postgres_schema) + "." + table_name + ") VALUES (" + "), (".join([", ".join([str(v) for v in x]) for x in insert_list]) + ");"
#                self.cursor.execute(q_insert)
                self.conn.commit()
                logging.info("Inserted " + str(c) + " rows into table " + str(table_name))
            except Exception, e:
                logging.error("Unexpected error when inserting into table " + str(table_name) + ": " + e.pgerror)
                raise e

class RabbitHelper:
    def __init__(self, rabbit_user, rabbit_password, rabbit_host, rabbit_port, rabbit_queue, postgres_helper):
        self.rabbit_user = rabbit_user
        self.rabbit_password = rabbit_password
        self.rabbit_host = rabbit_host
        self.rabbit_port = rabbit_port
        self.rabbit_queue = rabbit_queue
        self.postgres_helper = postgres_helper
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
        self.parameters = pika.ConnectionParameters(rabbit_host, rabbit_port, '/', self.credentials)

    def callback(self, ch, method, properties, body):
        body_obj = json.loads(body)
        table = body_obj["table"]
        data = body_obj["data"]
        self.postgres_helper.insert_data_element(table, data)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def create_pika_connection(self):
        logging.info("Creating pika connection...")
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbit_queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.callback, queue=self.rabbit_queue)
        logging.info("Pika connection created.")

    def start_pika_consumer(self):
        logging.info("Waiting for messages...")
        self.channel.start_consuming()

def start_app(postgres_helper, rabbit_helper):
    logging.info("Starting application...")
    while True:
        try:
            postgres_helper.create_postgres_connection()
            break
        except:
            logging.error("Unexpected error when creating postgres connection " + str(sys.exc_info()[0]))
            time.sleep(5)

    while True:
        try:
            rabbit_helper.create_pika_connection()
            break
        except:
            logging.error("Unexpected error when creating pika connection " + str(sys.exc_info()[0]))
            time.sleep(5)

    rabbit_helper.start_pika_consumer()

if __name__ == "__main__":

    if (len(sys.argv) != 11):
	raise Exception("Invalid number of arguments: " + str(len(sys.argv)))

    postgres_helper = PostgresHelper(sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10])
    rabbit_helper = RabbitHelper(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5], postgres_helper)

    while True:
	try:
    	    start_app(postgres_helper, rabbit_helper)
	except:
    	    logging.error("Unexpected application error: " + str(sys.exc_info()[0]))
    	    time.sleep(5)
