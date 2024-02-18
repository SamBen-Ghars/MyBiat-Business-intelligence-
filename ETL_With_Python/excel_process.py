from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from datetime import datetime, timedelta
from utils.params import test_meta, postgres_create_query

default_args = {
        'owner': 'airflow',
        'retries': 5,
        'retry_delay': timedelta(minutes=2)
        }


def get_conn():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_obj')
    engine = pg_hook.get_sqlalchemy_engine()
    return engine


def delete_trans_tables_content():
    conn = get_conn()
    for table, meta in test_meta.items():
        conn.execute(f"DELETE FROM {meta['trans_table']};")


def create_tables():
    conn = get_conn()
    conn.execute(postgres_create_query)
    print('created tables !!')


def insert_query(upsert_query, trans_table, df):
    conn = get_conn()
    df.to_sql(
            trans_table,
            conn,
            index=False,
            if_exists='append',
            schema='public'
             )
    conn.execute(upsert_query)


def process_excel():
    for table_name, meta in test_meta.items():
        file_path = os.path.abspath(meta['file_path'])
        df = pd.read_excel(file_path)
        df_length = len(df)
        if df_length >= 100000:
            for i in range(0, df_length, 50000):
                df_chunk = df.iloc[i:i+50000]
                insert_query(meta['upsert_query'],
                             meta['trans_table'],
                             df_chunk)
        else:
            insert_query(meta['upsert_query'], meta['trans_table'], df)


with DAG(
    dag_id='excel_db_etl',
        default_args=default_args,
        description='test test',
        schedule_interval='@daily',
        start_date=datetime(2011, 7, 21, 2),
        max_active_runs=1
        ) as dag:
    create_tables_if_not_exists = PythonOperator(
            task_id='create_db_tables_if_not_exists',
            python_callable=create_tables,
            dag=dag,
            )
    etl_process = PythonOperator(
            task_id='process_excel_to_db',
            python_callable=process_excel,
            dag=dag,
            )
    delete_trans_table_content = PythonOperator(
            task_id='delete_transaction_tables_content',
            python_callable=delete_trans_tables_content,
            dag=dag,
            )

    create_tables_if_not_exists >> etl_process >> delete_trans_table_content
