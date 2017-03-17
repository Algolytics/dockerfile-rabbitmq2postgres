#!/usr/bin/env python
import psycopg2
import sys
import pika
import json
from sets import Set
import time


class PostgresHelper:
    def __init__(self, postgres_dbname, postgres_user, postgres_password, postgres_host):
        self.postgres_dbname = postgres_dbname
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_host = postgres_host
        self.tableinfo = {}
        self.data_buffer = []
        self.max_data_buffer_size = 1000

    def create_postgres_connection(self):
        self.conn = psycopg2.connect(
            "dbname=" + self.postgres_dbname + " user=" + self.postgres_user + " password=" + self.postgres_password + " host=" + self.postgres_host)
        self.cursor = self.conn.cursor()
        print "Postgres connection created."

    def insert_data(self, table, data):
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
                print "Inserting buffered data into table " + str(table_name) + "..."
                q_create_table = "CREATE TABLE IF NOT EXISTS " + str(
                    table_name) + " (id SERIAL PRIMARY KEY, " + " text default null, ".join(
                    [str(k1) for k1 in columns_insert]) + " text default null)"
                self.cursor.execute(q_create_table)
                q_get_columns = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + str(
                    table_name) + "'"
                self.cursor.execute(q_get_columns)
                columns_existing = [k[0] for k in self.cursor.fetchall()]
                for k in columns_insert:
                    if not k.lower() in columns_existing:
                        q_add_column = "ALTER TABLE " + str(table_name) + " ADD COLUMN " + str(k) + " text default null"
                        self.cursor.execute(q_add_column)
                c = 0
                for d in self.data_buffer:
                    if d[0] == table_name:
                        q_insert = "INSERT INTO " + d[0] + " (" + ", ".join(
                            [str(k) for k, v in d[1].items()]) + ") VALUES (" + ", ".join(
                            [str(v) for k, v in d[1].items()]) + ");"
                        self.cursor.execute(q_insert)
                        c = c + 1
                self.conn.commit()
                print "Inserted " + str(c) + " rows."
            except:
                print "Unexpected error when inserting into table " + str(table_name), sys.exc_info()[0]


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
        self.postgres_helper.insert_data(table, data)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def create_pika_connection(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbit_queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.callback, queue=self.rabbit_queue)
        print "Pika connection created."

    def start_pika_consumer(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()


if (len(sys.argv) != 10):
    raise Exception("Invalid number of arguments: " + str(len(sys.argv)))

postgres_helper = PostgresHelper(sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9])
rabbit_helper = RabbitHelper(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5], postgres_helper)

while True:
    try:
        postgres_helper.create_postgres_connection()
        break
    except:
        print "Unexpected error when creating postgres connection", sys.exc_info()[0]
        time.sleep(5)

while True:
    try:
        rabbit_helper.create_pika_connection()
        break
    except:
        print "Unexpected error when creating pika connection", sys.exc_info()[0]
        time.sleep(5)

rabbit_helper.start_pika_consumer()