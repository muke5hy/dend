from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import DataQualityOperator
# from airflow.models import GetDefaultExecutor
from airflow.executors import LocalExecutor
from subdags.copy_to_redshift import get_s3_to_redshift
import yaml

start_date = datetime.utcnow()

default_args = {
    'owner': 'muke5hy',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False,
    'start_date': start_date,
    'template_searchpath': './dags'
}

dag = DAG('udacity-dend-capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1)

start_operator = DummyOperator(
    dag=dag,
    task_id='start_operator')

# Read table definitions from YAML file
with open('dags/configuration/copy_from_s3_to_redshift.yml', 'r') as file:
    copy_definitions = yaml.safe_load(file)

with dag:
    subdag_id = 'copy_data_to_redshift'
    copy_data_to_redshift = SubDagOperator(
        subdag=get_s3_to_redshift(
            parent_dag_name='udacity-dend-capstone',
            task_id=subdag_id,
            tables_definition=copy_definitions,
            redshift_conn_id='redshift',
            redshift_schema='public',
            s3_conn_id='aws_credentials',
            s3_bucket='udac-dend-capstone-dz',
            load_type='truncate',
            schema_location='Local',
            start_date=start_date),
        task_id=subdag_id,
        dag=dag,
        executor=LocalExecutor())
    copy_data_to_redshift.set_upstream(start_operator)


process_dim_category = PostgresOperator(
    dag=dag,
    task_id='process_dim_category',
    sql='/sql/categories.sql',
    postgres_conn_id='redshift'
)
process_dim_category.set_upstream(copy_data_to_redshift)

process_dim_cities = PostgresOperator(
    dag=dag,
    task_id='process_dim_cities',
    sql='/sql/cities.sql',
    postgres_conn_id='redshift'
)
process_dim_cities.set_upstream(copy_data_to_redshift)

process_dim_business = PostgresOperator(
    dag=dag,
    task_id='process_dim_business',
    sql='/sql/business.sql',
    postgres_conn_id='redshift'
)
process_dim_business.set_upstream([process_dim_category, process_dim_cities])

process_dim_users = PostgresOperator(
    dag=dag,
    task_id='process_dim_users',
    sql='/sql/users.sql',
    postgres_conn_id='redshift'
)
process_dim_users.set_upstream(copy_data_to_redshift)

process_dim_times = PostgresOperator(
    dag=dag,
    task_id='process_dim_times',
    sql='/sql/times.sql',
    postgres_conn_id='redshift'
)
process_dim_times.set_upstream(copy_data_to_redshift)

process_fact_tips = PostgresOperator(
    dag=dag,
    task_id='process_fact_tips',
    sql='/sql/fact_tips.sql',
    postgres_conn_id='redshift'
)
process_fact_tips.set_upstream([process_dim_times, process_dim_users, process_dim_business])

process_fact_reviews = PostgresOperator(
    dag=dag,
    task_id='process_fact_reviews',
    sql='/sql/fact_reviews.sql',
    postgres_conn_id='redshift'
)
process_fact_reviews.set_upstream([process_dim_times, process_dim_users, process_dim_business])

process_fk = PostgresOperator(
    dag=dag,
    task_id='process_foreign_keys',
    sql='/sql/dim_fk.sql',
    postgres_conn_id='redshift'
)
process_fk.set_upstream([process_fact_tips, process_fact_reviews])


run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    queries=({"table": "dim_times", "where": "day IS NULL", "result": 0},
             {"table": "fact_review", "where": "user_id IS NULL", "result": 0},
             {"table": "fact_review", "result": 6685900})

)
run_quality_checks.set_upstream(process_fk)


end_operator = DummyOperator(
    dag=dag,
    task_id='end_operator')
end_operator.set_upstream(run_quality_checks)
