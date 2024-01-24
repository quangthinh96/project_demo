from kafka import KafkaConsumer
import cassandra
from cassandra.cluster import Cluster
import json

cluster = Cluster(['localhost'],port=9042)

keyspace = 'study_de'
session = cluster.connect(keyspace)

consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest', enable_auto_commit=False, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer.subscribe('quangthinh')

for message in consumer:
    data = message.value
    create_time = data['create_time']
    bid = data['bid']
    custom_track = data['custom_track']
    job_id = data['job_id']
    publisher_id = data['publisher_id']
    group_id = data['group_id']
    campaign_id = data['campaign_id']
    ts = data['ts']
    sql = """ INSERT INTO tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ({},{},{},'{}',{},{},{},'{}')""".format(create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts)
    session.execute(sql)
    print('Insert successfully')
