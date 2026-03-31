from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner':             'gustavo.alvarado',
    'retries':           3,
    'retry_delay':       timedelta(minutes=5),
    'email_on_failure':  False,
    'depends_on_past':   False,
}

def run_bronze_task(**context):
    from src.ingesta.bronze_loader import run_bronze
    result = run_bronze()
    context['ti'].xcom_push(key='bronze_counts', value=result['counts'])
    return result['counts']

def run_silver_task(**context):
    from src.transformacion.silver_transformer import run_silver
    result = run_silver()
    context['ti'].xcom_push(key='silver_counts', value=result['counts'])
    return result['counts']

def run_gold_task(**context):
    from src.modelado.gold_builder import run_gold
    result = run_gold()
    return result['paths']

def run_quality_task(**context):
    from src.calidad.quality_checks import run_quality_checks
    report = run_quality_checks()
    summary = report.summary()
    if summary['failed'] > 0:
        raise ValueError(
            f"Quality checks fallaron: {summary['failed']}/{summary['total_checks']}. "
            f"Pass rate: {summary['pass_rate']}%"
        )
    return summary

with DAG(
    dag_id='pipeline_puntored_medallion',
    default_args=default_args,
    description='Pipeline Medallion Bronze→Silver→Gold para Punto Red',
    schedule_interval='0 6 * * *',  # Todos los dias a las 6AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['puntored', 'medallion', 'transacciones', 'fintech'],
) as dag:

    inicio = EmptyOperator(task_id='inicio_pipeline')

    bronze = PythonOperator(
        task_id='ingesta_bronze',
        python_callable=run_bronze_task,
    )

    silver = PythonOperator(
        task_id='transformacion_silver',
        python_callable=run_silver_task,
    )

    gold = PythonOperator(
        task_id='modelado_gold',
        python_callable=run_gold_task,
    )

    quality = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_task,
    )

    fin = EmptyOperator(task_id='pipeline_completado')

    # Dependencias — Bronze → Silver → Gold → Quality → Fin
    inicio >> bronze >> silver >> gold >> quality >> fin