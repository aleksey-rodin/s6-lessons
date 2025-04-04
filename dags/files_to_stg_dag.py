import contextlib
from typing import List

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pendulum
import vertica_python


conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv202502277',
    'password': 'uCnZdezbNG4ZkyC',
    'database': 'dwh',
    'autocommit': True
}


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str]
):
    vertica_conn = vertica_python.connect(**conn_info)
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM LOCAL '{dataset_path}' DELIMITER ',' ENCLOSED BY '"'
    """
    with contextlib.closing(vertica_conn.cursor()) as cur:
        print('start load')
        cur.execute(copy_expr)
        print('end load')
    vertica_conn.close()


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',  # путь к скачанному файлу
            'schema': 'STV202502277__STAGING',  # схема , куда загружаем данные
            'table': 'group_log',  # таблица , в которую будем загружать
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime']  # колонки для загрузки
        },
    )

    start >> load_group_log >> end


sprint6_dag_load_data_to_staging = sprint6_dag_load_data_to_staging()