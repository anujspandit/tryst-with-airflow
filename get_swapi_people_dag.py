from datetime import datetime, timedelta
import json
import time

from airflow.hooks import HttpHook
from airflow.hooks.mysql_hook import MySqlHook

from airflow.operators import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow.models import DAG

# Create database if it does not exist
def check_database():
    # Create database if not exists 
    connection = MySqlHook(mysql_conn_id='mysql_default')    
    sql = 'CREATE DATABASE IF NOT EXISTS `swapi_data`;'
    connection.run(sql, autocommit=True, parameters=())
    return True

# Create table if does not exist and clear existing records
def create_table():
    # Drop and Re-create table  
    connection = MySqlHook(mysql_conn_id='mysql_default') 

    sql = '''CREATE TABLE IF NOT EXISTS `swapi_data`.`swapi_people` (
        `id` int(11) NOT NULL auto_increment,    
        `name` varchar(100)  NOT NULL default '',     
        `birth_year` varchar(100) NOT NULL default '',
        `film`  varchar(100) NOT NULL default '',
        `film_name`  varchar(100) NOT NULL default '',
        `url` varchar(100) NOT NULL default '', 
        `birth_year_number` DECIMAL(4,1)  NOT NULL default 0,
        PRIMARY KEY  (`id`)
    );'''
    connection.run(sql, autocommit=True, parameters=())

    sql = '''DELETE FROM `swapi_data`.`swapi_people`;'''
    connection.run(sql, autocommit=True, parameters=())

    return True

# Get people from swapi.dev. 9 pages in a loop. 
# Can split this task in the future and make the range dynamic by using count of records in /people/
def get_people(**kwargs):    
    api_hook = HttpHook(http_conn_id='swapi_people', method='GET')

    for i in range(9): 
        response = api_hook.run('', data={'page':i+1})
        response_json = json.loads(response.content)
        print(response_json)
        store_people(response_json['results'])
        time.sleep(3)

    return True

# Store the people in swapi_data.swapi_people table
# For each person, film data is in an array
# The array is flattened and records are created for a person-and-film
def store_people(records):
    connection = MySqlHook(mysql_conn_id='mysql_default')
    for person in records:
        name = person['name']
        birth_year = person['birth_year']
        url = person['url']
        films = person['films']
        film_names_master = {            
            "http://swapi.dev/api/films/1/": "A New Hope",
            "http://swapi.dev/api/films/2/": "The Empire Strikes Back",
            "http://swapi.dev/api/films/3/": "Return of the Jedi",
            "http://swapi.dev/api/films/4/": "The Phantom Menace",
            "http://swapi.dev/api/films/5/": "Attack of the Clones",
            "http://swapi.dev/api/films/6/": "Revenge of the Sith",
            "http://swapi.dev/api/films/7/": "The Force Awakens",
        }
        for film in films: 
            film_name = film_names_master[film]
            sql = 'INSERT INTO `swapi_data`.`swapi_people`(name, birth_year, film, url, film_name) VALUES (%s, %s, %s, %s, %s)'
            connection.run(sql, autocommit=True, parameters=(name, birth_year, film, url, film_name))

    return True

# Replace 'unknown' with '0BBY'
# Could not find any records with birth year in ABY!!!
def clean_people_data(**kwargs):    
    connection = MySqlHook(mysql_conn_id='mysql_default')
    sql = 'UPDATE `swapi_data`.`swapi_people` SET birth_year = %s WHERE birth_year = %s'
    connection.run(sql, autocommit=True, parameters=('0BBY', 'unknown'))

    return True

# Convert birth year to a decimal value after replacing 'BBY' 
def prepare_people_data(**kwargs):    
    connection = MySqlHook(mysql_conn_id='mysql_default')
    sql = 'UPDATE `swapi_data`.`swapi_people` SET birth_year_number = REPLACE(birth_year, %s, %s);'
    connection.run(sql, autocommit=True, parameters=('BBY', ''))

    return True

args = {
    'owner': 'anuj.pandit',
    'depends_on_past': False,
    'start_date': datetime.utcnow() - timedelta(minutes=10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='process_swapi_people',
          default_args=args,
          schedule_interval='*/5 * * * *',
          dagrun_timeout=timedelta(seconds=300))

t1 = PythonOperator(task_id='check_database',
    provide_context=False,
    python_callable=check_database,
    dag=dag)

t2 = PythonOperator(task_id='create_table',
    provide_context=False,
    python_callable=create_table,
    dag=dag)

t3 = PythonOperator(task_id='get_people',
    provide_context=True,
    python_callable=get_people,
    dag=dag)

t4 = PythonOperator(task_id='clean_people_data',
    provide_context=True,
    python_callable=clean_people_data,
    dag=dag)

t5 = PythonOperator(task_id='prepare_people_data',
    provide_context=True,
    python_callable=prepare_people_data,
    dag=dag)

t6 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6