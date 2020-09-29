
## Table of Contents 

- [About](#about)
- [Prequisites](#prerequisites)
- [Improvements](#improvements)
- [Contact](#contact)
- [License](#license)

---
## About

- There are two dags (1: process_swapi_people| 2: aggregate_swapi)
- The first one loads people from swapi.dev into a mysql table swapi_data.swapi.people
- The second dag 'senses' the completion of the last task in the first dag 
- The tasks in the second dag aggregate data to get max age by film and character 
- Aggregate data is posted to http://requestbin.net/r/zorarbzo and also stored in a mysql table swapi_data.swapi.people_aggregate
- The API does not have data of FILM 7 - so the oldest character in 5 films is Master Yoda 

### For both DAGs:

#### In the DAG definition:
```python
schedule_interval='*/5 * * * *'
```
#### In the args:
```python
'start_date': datetime.utcnow() - timedelta(minutes=10),
```
#### Sensor specifics:
```python
sensor = ExternalTaskSensor(task_id='dag_sensor', 
    external_dag_id = 'process_swapi_people', 
    external_task_id = 'print_date', 
    dag=dag, 
    mode = 'reschedule')
```

[![DAGS IN ACTION](https://user-images.githubusercontent.com/12543322/94524422-36811300-0250-11eb-97bb-0c6afe89cf42.PNG)]()

## Prerequisites

- Setup Aiflow, Celery, Rabbitmq, Mysql
- mysql_default connection should be set to your Mysql db. User must have create db privileges 
- http connection called swapi_people	pointing to	https://swapi.dev/api/people/

### Improvements

- Calculate number of pages returned by the API - make range dynamic
- Crawl and process 1 page at a time 
- Create aggregate in timestamped tables - do not overwrite 

### Contact

- anuj.pandit@gmail.com

## License

- **[MIT license](http://opensource.org/licenses/mit-license.php)**
