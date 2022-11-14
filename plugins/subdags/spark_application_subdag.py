import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.local_to_s3 import \
    LocalFilesystemToS3Operator
from constants import S3_BUCKET_NAME, S3_BUCKET_URI


def spark_application_subdag(parent_dag_name, child_dag_name, args, spark_application_path) -> DAG:
    """
    SubDAG that automates uploading a Spark application to S3, executing it in EMR, and waiting for it to complete.
    """

    dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval=None,
        catchup=False,
        is_paused_upon_creation=False,
        start_date=datetime(2022, 1, 1)
    )

    file_name = os.path.basename(spark_application_path)

    upload_spark_application_task = LocalFilesystemToS3Operator(
        task_id=f'{child_dag_name}-upload_spark_application',
        dag=dag,
        filename=spark_application_path,
        dest_key=f'applications/{file_name}',
        dest_bucket=S3_BUCKET_NAME,
        replace=True,
        trigger_rule='none_failed'
    )

    # Add steps adds the Spark application to the job flows queue to execute.
    # This operator completes immediately after adding to the queue, before execution starts.
    emr_add_steps_task = EmrAddStepsOperator(
        task_id=f'{child_dag_name}-emr_add_steps',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull(dag_id='" + parent_dag_name +
        "', task_ids='job_flow_id', include_prior_dates=True) }}",
        aws_conn_id='aws_default',
        steps=[
            {
                "Name": f"{file_name}",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "client",
                        f"{S3_BUCKET_URI}/applications/{file_name}",
                    ],
                },
            },
        ],
    )

    # Waits for the step added in the previous step to fail or complete execution.
    emr_step_sensor_task = EmrStepSensor(
        task_id=f'{child_dag_name}-emr_step_sensor',
        dag=dag,
        job_flow_id="{{ task_instance.xcom_pull(dag_id='" + parent_dag_name +
        "', task_ids='job_flow_id', include_prior_dates=True) }}",
        step_id="{{ task_instance.xcom_pull(task_ids='" + child_dag_name + "-emr_add_steps')[0] }}",
        aws_conn_id='aws_default',
    )

    upload_spark_application_task >> emr_add_steps_task >> emr_step_sensor_task

    return dag
