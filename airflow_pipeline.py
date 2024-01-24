#pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


commands_task1 = """
    cd /Users/admin/Desktop/QUANGTHINH/DE_LEARN/recruitment_system;
    spark-submit --packages com.mysql:mysql-connector-j:8.0.33,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 Python_ETL_Pipeline.py;
    """


dag = DAG(
    'my_dag',
    description='DAG to trigger pySpark job',
    schedule_interval= '*/5 * * * *',
    start_date= datetime(2023, 12, 12)
)


task1 = BashOperator(
    task_id='task1',
    bash_command=commands_task1,
    dag=dag,
)


task1

 
