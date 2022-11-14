import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (BranchPythonOperator, PythonOperator,
                                      ShortCircuitOperator)
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator)
from constants import S3_BUCKET_URI, SPARK_APPLICATIONS_PATH
from subdags.spark_application_subdag import spark_application_subdag

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 11, 15),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

DAG_NAME = 'main_dag'

dag = DAG(DAG_NAME,
          default_args=default_args,
          description='Load medical data from S3 and load it into S3',
          schedule_interval='@daily',
          catchup=False,
          params={
              "job_flow_id": Param('', type="string"),
              "terminate_job_flow": Param(False, type='boolean')
          }
          )

start_task = EmptyOperator(task_id='begin_execution',  dag=dag)

JOB_FLOW_OVERRIDES = {
    "Name": "udacity_data_engineering_nanodegree_capstone_project",
    "ReleaseLabel": "emr-5.29.0",
    # Note: You can remove Hadoop and Livy if you do not need to use the EMR Notebook UI
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}, {"Name": "Livy"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            # Note: Included this config to create more worker nodes for Spark to parallelize across.
            # {
            #     "Name": "Task Nodes",
            #     "Market": "ON_DEMAND",
            #     "InstanceRole": "TASK",
            #     "InstanceType": "m5.xlarge",
            #     "InstanceCount": 2,
            # },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "LogUri": f"{S3_BUCKET_URI}/emr_logs",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True
}


def should_create_job_flow(**kwargs):
    """
    Accesses to DAG execution params to determine if a new job flow needs to be created
    or if the user passed in the ID of an already existing one.
    """
    return 'skip_create_job_flow' if kwargs['params']['job_flow_id'] else 'emr_create_job_flow'

# Either create job flow or skip and move on
should_create_job_flow_task = BranchPythonOperator(
    task_id='should_create_job_flow',
    dag=dag,
    python_callable=should_create_job_flow,
    provide_context=True
)

def push_job_flow_id(**kwargs):
    """
    Determine the job flow id that should be used by all downstream nodes. Either use
    the one passed in through params or the job flow that was created by the previous operator.
    """
    param_value = kwargs['params']['job_flow_id']
    xcom_value = kwargs['ti'].xcom_pull(task_ids='emr_create_job_flow', key="return_value")

    job_flow_id = ''
    if param_value:
        job_flow_id = param_value
    elif xcom_value:
        job_flow_id = xcom_value
    else:
        raise ValueError("No valid job flow ID available")

    return job_flow_id

job_flow_id = PythonOperator(
    task_id='job_flow_id',
    dag=dag,
    python_callable=push_job_flow_id,
    provide_context=True,
    trigger_rule='none_failed'
)

skipping_create_job_flow_task = EmptyOperator(task_id='skip_create_job_flow',  dag=dag)

emr_create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='emr_create_job_flow',
    dag=dag,
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    region_name='us-west-2'
)

# ETL Operators that transform raw datasets to the final data model as Parquet files

medical_facility_etl_task = SubDagOperator(
    task_id="medical_facility_etl",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "medical_facility_etl", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/etls/medical_facility_etl.py"),
    trigger_rule='none_failed'
)
apr_drg_code_etl_task = SubDagOperator(
    task_id="apr_drg_code_etl",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "apr_drg_code_etl", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/etls/apr_drg_code_etl.py"),
    trigger_rule='none_failed'
)
apr_mdc_code_etl_task = SubDagOperator(
    task_id="apr_mdc_code_etl",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "apr_mdc_code_etl", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/etls/apr_mdc_code_etl.py"),
    trigger_rule='none_failed'
)

discharge_etl_task = SubDagOperator(
    task_id="discharge_etl",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "discharge_etl", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/etls/discharge_etl.py"),
    trigger_rule='none_failed'
)

# Data quality operators that verify the correctness of the data model produced by the ETL Operators

data_quality_task = EmptyOperator(task_id='begin_data_quality_checks', dag=dag)

apr_data_quality_task = SubDagOperator(
    task_id="apr_data_quality",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "apr_data_quality", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/data_quality/apr_data_quality.py"),
    trigger_rule='none_failed'
)

discharge_data_quality_task = SubDagOperator(
    task_id="discharge_data_quality",
    dag=dag,
    subdag=spark_application_subdag(dag.dag_id, "discharge_data_quality", dag.default_args,\
                                    f"{SPARK_APPLICATIONS_PATH}/data_quality/discharge_data_quality.py"),
    trigger_rule='none_failed'
)

# Terminate job flow if user requested for it to happen through params

def should_terminate_job_flow_test(**kwargs):
    return kwargs['params']['terminate_job_flow']


should_terminate_job_flow_task = ShortCircuitOperator(
    task_id='should_terminate_job_flow',
    dag=dag,
    python_callable=should_terminate_job_flow_test,
    provide_context=True
)
terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    dag=dag,
    job_flow_id="{{ params.job_flow_id if params.job_flow_id != '' else task_instance.xcom_pull(task_ids='emr_create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
)

end_task = EmptyOperator(task_id='stop_execution',  dag=dag)

start_task >> should_create_job_flow_task
should_create_job_flow_task >> emr_create_job_flow_task
should_create_job_flow_task >> skipping_create_job_flow_task

skipping_create_job_flow_task >> job_flow_id
emr_create_job_flow_task >> job_flow_id

job_flow_id >> discharge_etl_task
job_flow_id >> apr_drg_code_etl_task
job_flow_id >> apr_mdc_code_etl_task
job_flow_id >> medical_facility_etl_task

discharge_etl_task >> data_quality_task
apr_drg_code_etl_task >> data_quality_task
apr_mdc_code_etl_task >> data_quality_task
medical_facility_etl_task >> data_quality_task

data_quality_task >> apr_data_quality_task
data_quality_task >> discharge_data_quality_task

apr_data_quality_task >> should_terminate_job_flow_task
discharge_data_quality_task >> should_terminate_job_flow_task

should_terminate_job_flow_task >> terminate_job_flow_task
terminate_job_flow_task >> end_task
