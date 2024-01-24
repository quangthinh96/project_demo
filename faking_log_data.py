import mysql.connector
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import uuid
import random
from datetime import datetime
import time


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda K:dumps(K).encode('utf-8'))


user = 'root'
password = '1'
host = 'localhost'
database = 'study_de'


mysqlconnection = mysql.connector.connect(user=user,password=password,host=host,database=database)


# Get job's data
job_query = """select id as job_id,campaign_id , group_id , company_id from job"""
job_data = pd.read_sql(job_query,mysqlconnection)
job_list = job_data['job_id'].to_list()
group_list = job_data[job_data['group_id'].notnull()]['group_id'].astype(int).to_list()
campaign_list = job_data['campaign_id'].to_list()


# Get puclisher's data
publisher_query = """select distinct(id) as publisher_id from master_publisher"""
publisher_data = pd.read_sql(publisher_query,mysqlconnection)
publisher_list = publisher_data['publisher_id'].to_list()


interact = ['click','conversion','qualified','unqualified']


def generate_log_data(n_records):
    i = 0 
    while i <= n_records:
        create_time = str(uuid.uuid1())
        event ={
        "create_time":create_time,
        "bid":random.randint(0,1),
        "custom_track":random.choices(interact,weights=(70,10,10,10))[0],
        "job_id":random.choices(job_list)[0],
        "publisher_id":random.choices(publisher_list)[0],
        "group_id":random.choices(group_list)[0],
        "campaign_id":random.choices(campaign_list)[0],
        "ts":datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
        producer.send('quangthinh', value=event)
        i = i +1
    return print("Generated {} lines of log data successfully".format(n_records))

status = "ON"
while status == "ON":
    generate_log_data(n_records = random.randint(1,20))
    time.sleep(120)