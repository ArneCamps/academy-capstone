import datetime as dt

from airflow import DAG
from conveyor.operators import ConveyorSparkSubmitOperatorV2


dag = DAG(
    dag_id="arne_camps_capstone_exercise",
    description="create an airflow dag",
    default_args={"owner": "Airflow"},
    schedule_interval="0 0 * * *",
    start_date=dt.datetime(2022,9,1,0,0,0),
    catchup=True
)

role = "arne_project-{{ macros.conveyor.env() }}"

ConveyorSparkSubmitOperatorV2(
    dag=dag,
    spark_main_version=3,
    task_id="the-task-id",
    num_executors=1,
    driver_instance_type="mx.small",
    executor_instance_type="mx.small",
    aws_role=role,
    application="local:///workspace/academy-capstone/c_writeSnowflake.py"

)