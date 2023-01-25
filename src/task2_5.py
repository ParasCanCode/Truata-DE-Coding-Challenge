# Import Modules

import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Default Arguments

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' : days_ago(1),
    'retries' : 0
}

# Instantiate DAG

dag = DAG(dag_id = 'AirflowTask',  default_args= default_args, schedule_interval= None)


#Assigning Tasks

t1 = DummyOperator(task_id ='Task1', dag = dag)
t2 = DummyOperator(task_id ='Task2', dag = dag)
t3 = DummyOperator(task_id ='Task3', dag = dag)
t4 = DummyOperator(task_id ='Task4', dag = dag)
t5 = DummyOperator(task_id ='Task5', dag = dag)
t6 = DummyOperator(task_id ='Task6', dag = dag)

# Schedule Task
t1.set_downstream([t2, t3])
t4.set_upstream([t2, t3])
t5.set_upstream([t2, t3])
t6.set_upstream([t2, t3])