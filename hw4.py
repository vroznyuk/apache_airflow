import datetime as dt
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2021, 2, 7),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


@dag(default_args=args, schedule_interval=None)
def hw4_taskflow():

    @task
    def first_task():
        print('Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}')
        return 0

    @task
    def download_titanic_dataset(*agrs) -> dict:
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        return df.to_json()

    @task
    def pivot_dataset(titanic_json: dict) -> dict:
        return pd.read_json(titanic_json).\
            pivot_table(index=['Sex'], columns=['Pclass'], values='Name', aggfunc='count').reset_index().to_json()

    @task
    def mean_fare_per_class(titanic_json: dict) -> dict:
        return pd.read_json(titanic_json).groupby('Pclass').Fare.mean().reset_index().to_json()

    @task
    def pivot_to_db(result_json: dict):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_json(result_json)
        tbl_name = Variable.get('TABLE_TITANIC_PIVOT')

        db_query = 'delete from ' + tbl_name
        pg_hook.run(db_query)

        for row in df.itertuples(index=False):
            db_query = 'insert into ' + tbl_name + ' (sex, cl1_count, cl2_count, cl3_count) values (%s, %s, %s, %s)'
            pg_hook.run(db_query, parameters=tuple(row))  # row['Sex'], row[1], row[2], row[3],))
        return 0

    @task
    def mean_fare_to_db(result_json: dict):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_json(result_json)
        tbl_name = Variable.get('TABLE_TITANIC_MEAN_FARE')

        db_query = 'delete from ' + tbl_name
        pg_hook.run(db_query)

        for row in df.itertuples(index=False):
            db_query = 'insert into ' + tbl_name + ' (pclass, mean_fare) values (%s, %s)'
            pg_hook.run(db_query, parameters=tuple(row))  # (row['Pclass'], row['Fare'],))


    @task
    def last_task(*agrs):
        print('Pipeline finished! Execution date is {{ ds }}')
        return 0

    dummy = first_task()
    titanic_ds = download_titanic_dataset(dummy)
    pivot_ds = pivot_dataset(titanic_ds)
    mean_fare_ds = mean_fare_per_class(titanic_ds)
    pivot_dummy = pivot_to_db(pivot_ds)
    mean_fare_dummy = mean_fare_to_db(mean_fare_ds)
    last_task(pivot_dummy, mean_fare_dummy)


titanic_pivot_hw4 = hw4_taskflow()
