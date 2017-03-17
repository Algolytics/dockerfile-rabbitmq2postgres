#!/usr/bin/env python
import psycopg2
import sys
import pika
import json
from sets import Set

if (len(sys.argv)!=10):
  raise Exception("Illegal arguments count: "+str(len(sys.argv)))

#rabbit_user='user'
rabbit_user=sys.argv[1]
#rabbit_password='bitnami'
rabbit_password=sys.argv[2]
#rabbit_host='localhost'
rabbit_host=sys.argv[3]
#rabbit_port=5672
rabbit_port=int(sys.argv[4])
#rabbit_queue='sce.postgres.data-queue'
rabbit_queue=sys.argv[5]

#postgres_dbname='postgres2'
postgres_dbname=sys.argv[6]
#postgres_user='postgres'
postgres_user=sys.argv[7]
#postgres_password='postgres'
postgres_password=sys.argv[8]
#postgres_host='localhost'
postgres_host=sys.argv[9]

max_data_buffer_size=1000

conn = psycopg2.connect("dbname="+postgres_dbname+" user="+postgres_user+" password="+postgres_password+" host="+postgres_host)
cursor = conn.cursor()

tableinfo = {}
data_buffer = []
def insert_data(table, data):
    tableinfo[table] = set()
    for k,v in data.items():
      tableinfo[table].add(str(k))
    data_buffer.append((table, data))
    if len(data_buffer) >= max_data_buffer_size:
      insert_data_buffer()
      tableinfo.clear()
      del data_buffer[:]

def insert_data_buffer():
    for table_name,columns_insert in tableinfo.items():
      try:
        print "Inserting buffered data into table " + str(table_name) + "..."
        q_create_table = "CREATE TABLE IF NOT EXISTS " + str(table_name) + " (id SERIAL PRIMARY KEY, " + " text default null, ".join([str(k1) for k1 in columns_insert]) + " text default null)"
        cursor.execute(q_create_table)
        q_get_columns = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + str(table_name) + "'"
        cursor.execute(q_get_columns)
        columns_existing = [k[0] for k in cursor.fetchall()]
        for k in columns_insert:
          if not k.lower() in columns_existing:
            q_add_column = "ALTER TABLE " + str(table_name) + " ADD COLUMN " + str(k) + " text default null"
            cursor.execute(q_add_column)
        c=0
        for d in data_buffer:
            if d[0] == table_name:
              q_insert =  "INSERT INTO " + d[0] + " (" + ", ".join([str(k) for k,v in d[1].items() ]) + ") VALUES (" + ", ".join([str(v) for k,v in d[1].items() ]) + ");"
              cursor.execute(q_insert)
              c = c+1
        conn.commit()
        print "Inserted " + str(c) + " rows."
      except:
        print "Unexpected error when inserting into table " + str(table_name), sys.exc_info()[0]

credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
parameters = pika.ConnectionParameters(rabbit_host,
                                   rabbit_port,
                                   '/',
                                   credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue=rabbit_queue, durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    body_obj = json.loads(body)
    table = body_obj["table"]
    data = body_obj["data"]
    insert_data(table, data)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=rabbit_queue)

channel.start_consuming()
