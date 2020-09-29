from datetime import datetime, timedelta
import json
import time
import requests

from airflow.hooks import HttpHook
from airflow.hooks.mysql_hook import MySqlHook

from airflow.operators import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from airflow.models import DAG

def check_database():
    # Create database if not exists 
    connection = MySqlHook(mysql_conn_id='mysql_default')    
    sql = 'CREATE DATABASE IF NOT EXISTS `swapi_data`;'
    connection.run(sql, autocommit=True, parameters=())
    return True

def create_table():
    # Drop and Re-create table  
    connection = MySqlHook(mysql_conn_id='mysql_default') 

    sql = '''CREATE TABLE IF NOT EXISTS `swapi_data`.`swapi_people_aggregate` (
        `id` int(11) NOT NULL auto_increment,    
        `film_name` varchar(100)  NOT NULL default '', 
        `film` varchar(100)  NOT NULL default '', 
        `name`  varchar(100) NOT NULL default '',    
        `birth_year` DECIMAL(4,1) NOT NULL default 0,
        PRIMARY KEY  (`id`)
    );'''
    connection.run(sql, autocommit=True, parameters=())

    sql = '''DELETE FROM `swapi_data`.`swapi_people_aggregate`;'''
    connection.run(sql, autocommit=True, parameters=())

    return True

def build_aggregrate():    
    connection = MySqlHook(mysql_conn_id='mysql_default')
    sql = '''
        INSERT INTO `swapi_data`.`swapi_people_aggregate` (film, birth_year, name, film_name)
        SELECT 
            film, 
            max(birth_year_number) as birth_year, 
            (
                SELECT name 
                FROM 
                    swapi_data.swapi_people 
                WHERE 
                    film = t.film 
                ORDER BY 
                    birth_year_number DESC LIMIT 0,1
            ) as name,
            film_name
        FROM 
            swapi_data.swapi_people t 
        GROUP BY 
            film,
            film_name;
    '''
    connection.run(sql, autocommit=True, parameters=())

    return True

def send_aggregate_to_requestbin():
    target = 'http://requestbin.net/r/zorarbzo'
    
    connection = MySqlHook(mysql_conn_id='mysql_default')
    sql = '''
        SELECT 
            film_name, name, birth_year 
        FROM
            `swapi_data`.`swapi_people_aggregate`;
    '''
    result = connection.get_records(sql)
    data = []
    for item in result: 
        data.append({
            "film_name": item[0],
            "name": item[1],
            "birth_year": str(item[2])
        })

    result = requests.post(target, data = json.dumps(data))
    
    return result

args = {
    'owner': 'anuj.pandit',
    'depends_on_past': False,
    'start_date': datetime.utcnow() - timedelta(minutes=10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='aggregate_swapi',
          default_args=args,
          schedule_interval='*/5 * * * *',
          dagrun_timeout=timedelta(seconds=300))

sensor = ExternalTaskSensor(task_id='dag_sensor', 
    external_dag_id = 'process_swapi_people', 
    external_task_id = 'print_date', 
    dag=dag, 
    mode = 'reschedule')

t1 = PythonOperator(task_id='check_database',
    provide_context=False,
    python_callable=check_database,
    dag=dag)

t2 = PythonOperator(task_id='create_table',
    provide_context=False,
    python_callable=create_table,
    dag=dag)

t3 = PythonOperator(task_id='build_aggregrate',
    provide_context=False,
    python_callable=build_aggregrate,
    dag=dag)

t4 = PythonOperator(task_id='send_aggregate_to_requestbin',
    provide_context=False,
    python_callable=send_aggregate_to_requestbin,
    dag=dag)

t5 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

sensor >> t1 >> t2 >> t3 >> t4 >> t5 