# Udacity Data Engineering Nanodegree Capstone Project

This repo contains Eric Dudley's capstone project submission for the Data Engineering Nanodegree.

## Overview

The code in this repository ingests medical facility discharge data and related metadata into a data lake for purposes of data analysis.

The following datasets are being used:

| Data Set                                                                                                                                                                       | Format | Size           | Description                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Health_Facility_General_Information.csv ([source](https://health.data.ny.gov/Health/Health-Facility-General-Information/vn5v-hh5r/data))                                       | csv    | 6,125 rows     | Each row contains metadata about a medical facility in the state of New York (Name, address, description, etc.)                                                                                                                             |
| Hospital_Inpatient_Discharges**SPARCS*De-Identified***2010.csv ([source](https://health.data.ny.gov/Health/Hospital-Inpatient-Discharges-SPARCS-De-Identified/mtfm-rxf4/data)) | csv    | 2,622,133 rows | Each row contains information about a patient discharge from a hospital in New York state in the year 2010 (diagnosis, treatment, cost, etc.)                                                                                               |
| Hospital-Acquired_Infections\_\_Beginning_2008.csv ([source](https://health.data.ny.gov/Health/Hospital-Acquired-Infections-Beginning-2008/utrt-zdsi))                         | csv    | 24,476 rows    | Each row contains information about the total hospital acquired infections for a given medical facility in New York in a given year.                                                                                                        |
| apr_drg_codes.json ([source](https://apps.3mhis.com/docs/Groupers/All_Patient_Refined_DRG/apr400_DRG_descriptions.txt))                                                        | json   | 334 rows       | Each row contains a mapping of [APR DRG code](https://www.3m.com/3M/en_US/health-information-systems-us/drive-value-based-care/patient-classification-methodologies/apr-drgs/) to descriptions and MDC (major diagnostic categories) codes. |
| All datasets                                                                                                                                                                   |        | 2,653,068 rows | Together, the datasets contain 2.6 million rows.                                                                                                                                                                                            |

The following is an overview of the workflow that will be used to work with these datasets:

1. Storage | Upload CSV, JSON datasets to S3
2. ETL | Download and transform datasets using a Spark cluster hosted with AWS EMR, upload transformed dataframes S3 as Parquet files.
3. Data Quality | Submit Spark jobs to make data quality checks.
4. Analysis | Utilize AWS EMR Notebooks to perform adhoc analysis on the transformed datasets.

This process will be automated by an Airflow DAG to submit Spark jobs.

## Data Dictionary

Below is the data model produced in this project. It is a star schema with a single fact table and three dimension tables.

### discharge

Fact table, each row represents a single patient discharge.

| Column                       | Description                                                                                                                                                                     | Example Value | Type    |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------- |
| facility_id                  | The unique identifier of the facility the patient was discharged from.                                                                                                          | '1186'        | string  |
| age_group                    | The age group the patient was in at the time of departure.                                                                                                                      | '18 to 29'    | string  |
| gender                       | The sex, assigned at birth, of the patient, either 'M' or 'F'.                                                                                                                  | 'M'           | string  |
| race                         | A textual representation of the race of the patient.                                                                                                                            | 'White'       | string  |
| days_of_stay                 | The number of contiguous days the patient was at the medical facility before being discharged.                                                                                  | 8             | integer |
| discharge_year               | The year in which the patient was discharged.                                                                                                                                   | 2010          | integer |
| apr_drg_code                 | A number (left padded with zeroes to be of length three) that represents a [Diagnostic-related group](https://en.wikipedia.org/wiki/Diagnosis-related_group).                   | '776'         | string  |
| apr_mdc_code                 | A number between 0 and 25 (left padded with zeroes to be of length two) that represents a [Major Diagnostic Category](https://en.wikipedia.org/wiki/Major_Diagnostic_Category). | '20'          | string  |
| apr_severity_of_illness_code | A number representing the severity of illness, smaller numbers are less severe.                                                                                                 | 2             | integer |
| apr_risk_of_mortality        | A textual represntation of the risk of mortality of the patient.                                                                                                                | 'Minor'       | string  |
| total_charges                | The total USD amount that was charged to the patient.                                                                                                                           | 12553.24      | double  |
| total_costs                  | The total USD amount that was the cost of the medical facility for treating the patient.                                                                                        | 9838.02       | double  |

### medical_facility

Dimension table, each row represents a single medical facility.

| Column      | Description                                                                            | Example Value                  | Type   |
| ----------- | -------------------------------------------------------------------------------------- | ------------------------------ | ------ |
| id          | Unique identifier for a medical facility.                                              | '1186'                         | string |
| name        | Public name of the medical facility.                                                   | 'North Central Bronx Hospital' | string |
| description | General description of the facility, can be used to group multiple similar facilities. | 'Hospital'                     | string |
| latitude    | Latitude of the location of the facility.                                              | 40.880512                      | double |
| longitude   | Longitude of the location of the facility.                                             | -73.881538                     | double |
| zip_code    | Postal Zip code of the facility.                                                       | '10467'                        | string |

### apr_drg_code

Dimension table, each row represents a single APR DRG diagnosis code.

| Column      | Description                                                                                                                                                                     | Example Value                     | Type   |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | ------ |
| code        | A number (left padded with zeroes to be of length three) that represents a [Diagnostic-related group](https://en.wikipedia.org/wiki/Diagnosis-related_group).                   | '038'                             | string |
| description | A human readable description of the given Diagnostic-related group.                                                                                                             | 'EXTRACRANIAL PROCEDURES WITH CC' | string |
| mdc_code    | A number between 0 and 25 (left padded with zeroes to be of length two) that represents a [Major Diagnostic Category](https://en.wikipedia.org/wiki/Major_Diagnostic_Category). | '001'                             | string |
| type        | A value that is either 'P' or 'M'. This represents whether the DRG is a procedure or a principal diagnosis respectively.                                                        | 'P'                               | string |

### apr_mdc_code

Dimension table, each row represents a single APR MDC code.

| Column      | Description                                                                                                                                                                     | Example Value | Type   |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------ |
| code        | A number between 0 and 25 (left padded with zeroes to be of length two) that represents a [Major Diagnostic Category](https://en.wikipedia.org/wiki/Major_Diagnostic_Category). | '02'          | string |
| description | A human readable description of the given Major Diagnostic Category.                                                                                                            |

## Implementation

### Data Investigation

The `data_investigation.ipynb` notebook leverages Pandas to load the various datasets on the local machine. This notebook was used to quickly investiate the quality and shape of the datasets before solidfying the desired final data models.

### Storage

![ndg_s3_data](./images/ndg_s3_data.png)

First, the datasets were acquired from their respective sources, including downloading files directly from a web server (DRG codes JSON file), and exporting datasets using an online tool (all other datasets).

An S3 bucket `udacity-data-engineering-nanodegree-capstone-project` was created to house all of the data for this project.

The datasets were manually uploaded using the AWS S3 UI, each with a `data/` prefix.

### ETL / Data Quality

![ndg_airflow_etl](./images/ndg_airflow_etl.png)

An airflow DAG was created to automate the ETL that processes these datasets into the data schema defined above.

1. The DAG takes in `job_flow_id` and `terminate_job_flow` parameters. (In this case, the term "job flow" is equivalent to "EMR Cluster".)

- If a `job_flow_id` is provided it will be used in all downstream EMR operators; otherwise, a new job flow will be created.
- If `terminate_job_flow` is set to `true`, the active job flow (passed in or created during execution) will be terminated at the end of the DAG.

2. Four ETL Spark applications are then submitted to EMR, each application is responsible for producing one table in the final data model.

- The applications are submitted using a Subdag.
- The subdag first uploads the local Spark application file (example: `apr_drg_code_etl.py`) to S3.
  ![ndg_s3_applications](./images/ndg_s3_applications.png)
- Next, it submits the Spark application using the `EmrAddSteps` operator and waits for the Spark application to complete using a sensor operator. The progress of the Spark application can be monitored in the AWS EMR UI.
  ![ndg_emr_steps](./images/ndg_emr_steps.png)
- Each Spark application downloads a dataset from S3, transforms the dataset into a dataframe that satisfies the expected data model, and then uploads the new dataframe in the `Parquet` format to the S3 bucket under the `tables/` prefix.
  ![ndg_s3_tables](./images/ndg_s3_tables.png)

3. Using the same Subdag used to submit the ETL applications, two Spark applications are submitted to EMR, each responsible for performing data quality checks on different datasets.

- Each data quality application downloads a Parquet dataset from S3 produced in the previous step and performs assertions on the dataset. These assertions check for qualities such as:
  - Size of dataframes
  - Length of DRG and MDC codes
  - Range of USD patient costs/charges.
- If any of the assertions fail, the whole DAG fails and the error is reported to the user.

4. If `terminate_job_flow` is set to `true`, the active job flow (passed in or created during execution) is terminated. Finally, the DAG completes successfully.

### Analysis

The queries in `data_analysis.ipynb` were run in an EMR Notebook to answer the following questions.

- What are the top three ZIP codes in medical costs?
- What DRG + MDC results in the most charges to patients?
- What is the most prevalent DRG at each medical facility?


## Additional Considerations

### The data was increased by 100x.

Since the implementation is already leveraging a distributed data processing system, Spark, and an infinitely scalable object storage system, S3, there would be no major changes. The following minor changes would have to be made:

- Update dataset S3 URL to work with multiple S3 files with the same prefix.
- Scale AWS EMR Cluster horizontally by adding more workers to allow Spark to more effectively distribute tasks.
- Update partition key in final data model Parquet files to ensure that the data is partitioned reasonably (eg. No single partition will be too large to be downloaded individually.)

### The pipelines would be run on a daily basis by 7 am every day.

Since the implementation is already leveraging a workflow automation system, Airflow, no major changes would have to be made. The following minor changes would have to be made:

- Update DAG schedule to run daily at 7am
- Potentially setup Airflow email notification with the on-call system to ensure that maintainers of the pipeline will be notified if the daily DAG run failed.
- Potentially setup an Airflow SLA for the DAG if it is required that the DAG completes by a certain time after 7am.

### The database needed to be accessed by 100+ people.

The final datasets are stored as Parquet files in S3 bucket, so the following changes would have to be made to support this use case.

- Communicate with users of the data to determine what type of access is required (direct access to datasets? aggregated view? dashboard tool?)
- Determine who needs to access what dataset and use IAM + S3 Bucket permissions to control who can access the datasets in S3.
- If many users need to be able to access this data using a Spark interface, it could make sense to set up a script / pipeline that automatically creates an EMR Notebook + Cluster for the user to explore the datasets.
- If many users need to be able to access this data using a SQL interface, it could make sense to use AWS Glue + Athena to create a standard SQL interface to these datasets.


# Useful Information

## OSX Airflow Setup

### Install dependencies

`brew install postgresql`

`brew install redis`

`pip3 install -r requirements.txt`

### Initialize database

`createuser airflow`

`createdb airflow`

`airflow db init`

### Start services

`redis-server`

`postgres -D /usr/local/var/postgresql@14`

`airflow celery worker`

`airflow webserver`

`airflow scheduler`

### Create airflow user

`airflow users create --username admin123 --firstname Admin --lastname User --role Admin --password Admin123 --email admin@exmaple.org`

### Update airflow.cfg

#### [core]

`executor = CeleryExecutor`

#### [database]

`sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow`

#### [celery]

`broker_url = redis://localhost:6379/0`

`result_backend = db+postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow`

### Update airflow connection

Set aws_default connection `Extra` field:
`{"region_name": "us-west-2"}`