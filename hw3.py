import os
import sys
import datetime as dt
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, path)

from modules.utils import download_titanic_dataset, pivot_dataset, mean_fare_per_class, pivot_to_db, mean_fare_to_db


# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2021, 2, 1),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


with DAG(
        dag_id='titanic_pivot_hw3',  # Имя DAG
        schedule_interval=None,  # Периодичность запуска, например, "00 15 * * *"
        default_args=args,  # Базовые аргументы
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        provide_context=True,
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_titanic_dataset',
        provide_context=True,
        python_callable=pivot_dataset,
        dag=dag,
    )

    # Расчет средней стоимости билеты по классам
    mean_fare_titanic_dataset = PythonOperator(
        task_id='mean_fare_titanic_dataset',
        provide_context=True,
        python_callable=mean_fare_per_class,
        dag=dag,
    )

    pivot_titanic_save_to_db = PythonOperator(
        task_id='pivot_titanic_save_to_db',
        provide_context=True,
        python_callable=pivot_to_db,
        dag=dag,
    )

    mean_fare_titanic_save_to_db = PythonOperator(
        task_id='mean_fare_titanic_save_to_db',
        provide_context=True,
        python_callable=mean_fare_to_db,
        dag=dag,
    )

    # Вывод информации о завершении обработки данных
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
        dag=dag,
    )

    # Порядок выполнения тасок
    first_task >> create_titanic_dataset >> [pivot_titanic_dataset, mean_fare_titanic_dataset]
    pivot_titanic_dataset >> pivot_titanic_save_to_db >> last_task
    mean_fare_titanic_dataset >> mean_fare_titanic_save_to_db >> last_task
