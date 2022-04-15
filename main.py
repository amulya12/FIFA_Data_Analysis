from numpy import append
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import storage as gcs



def get_records():
    
    client=gcs.Client()
    bucket=client.get_bucket("dataset-fifa")
    blob=bucket.get_blob("FIFA/FIFA17_official_data.csv")
    data=blob.download_as_text()
    raw_data=pd.read_csv(StringIO(data))

    # bq_client=bigquery.Client(project="amazing-office-347302")

    # bq_dataset= "{}.{}".format("amazing-office-347302","test")
    # dataset=bigquery.Dataset(bq_dataset)
    
    # bq_table= "{}.{}.{}".format("amazing-office-347302","test","schema")
    # table=bigquery.Table(bq_table)
    schema=[
  {
    "mode": "NULLABLE",
    "name": "ID",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Age",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Photo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Nationality",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Flag",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Overall",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Potential",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "Club",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ClubLogo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Value",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Wage",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Special",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "PreferredFoot",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "InternationalReputation",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "WeakFoot",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "SkillMoves",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "WorkRate",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "BodyType",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "RealFace",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Position",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "JerseyNumber",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Joined",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "LoanedFrom",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ContractValidUntil",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Height",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Weight",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Crossing",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Finishing",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "HeadingAccuracy",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "ShortPassing",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Volleys",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Dribbling",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Curve",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "FKAccuracy",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "LongPassing",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "BallControl",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Acceleration",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "SprintSpeed",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Agility",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Reactions",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Balance",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "ShotPower",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Jumping",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Stamina",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Strength",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "LongShots",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Aggression",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Interceptions",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Positioning",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Vision",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Penalties",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Composure",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "Marking",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "StandingTackle",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "SlidingTackle",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "GKDiving",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "GKHandling",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "GKKicking",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "GKPositioning",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "GKReflexes",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "BestPosition",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "BestOverallRating",
    "type": "FLOAT"
  }
]

    raw_data.to_gbq(destination_table="test.schema",project_id="amazing-office-347302",if_exists="append",table_schema=schema)
    
# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly'
}

with DAG('new1_dag',
         start_date=datetime(2022, 3, 14),
         max_active_runs=2,
         schedule_interval=timedelta(minutes=30),
         default_args=default_args,
         catchup=False
         ) as dag:

    get_combined_records= PythonOperator(
        task_id = 'task1',
        python_callable=get_records
    )

    

    get_combined_records 
    # >> clean_process_records >> upload_big_query