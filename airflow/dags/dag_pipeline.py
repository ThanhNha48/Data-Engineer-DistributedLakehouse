# ./dags/dag_pipeline.py
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DBT_ROOT_DIR = "/opt/airflow/ecommerce_dbt"
SPARK_JOBS_DIR = "/opt/spark_jobs"


@dag(
    dag_id="ecommerce_dag_pipeline",
    default_args={
        "owner": "data_engineering_team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    schedule=timedelta(hours=6),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "medallion", "ecommerce", "analytics", "iceberg", "spark"],
    max_active_runs=1,
)
def ecommerce_pipeline():
    from operators.dbt_operator import DbtOperator

    logger = logging.getLogger("airflow.task")

    @task
    def start_pipeline():
        pipeline_id = f"dag_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Bắt đầu pipeline: {pipeline_id}")
        return {"pipeline_id": pipeline_id}

    start = start_pipeline()

    prepare_bronze = SparkSubmitOperator(
        task_id="0_prepare_bronze_namespace",
        application=f"{SPARK_JOBS_DIR}/0_create_bronze_namespace.py",
        conn_id="spark_default",
        name="prepare-bronze-namespace",
        verbose=True,
    )
    load_bronze = SparkSubmitOperator(
        task_id="1_load_bronze_seeds",
        application=f"{SPARK_JOBS_DIR}/1_load_bronze_seeds.py",
        conn_id="spark_default",
        name="load-bronze-seeds",
        verbose=True,
    )
    validate_bronze = SparkSubmitOperator(
        task_id="2_validate_bronze",
        application=f"{SPARK_JOBS_DIR}/2_validate_bronze.py",
        conn_id="spark_default",
        name="validate-bronze",
        verbose=True,
    )

    bronze_to_silver = DbtOperator(
        task_id="3_dbt_bronze_to_silver",
        dbt_root_dir=DBT_ROOT_DIR,
        dbt_command="run",
        select=["tag:bronze"],
        target="silver",
    )

    silver_to_gold = DbtOperator(
        task_id="4_dbt_silver_to_gold",
        dbt_root_dir=DBT_ROOT_DIR,
        dbt_command="run",
        select=["tag:silver"],
        target="gold",
    )

    gold_marts = DbtOperator(
        task_id="5_dbt_gold_marts",
        dbt_root_dir=DBT_ROOT_DIR,
        dbt_command="run",
        select=["tag:gold"],
        target="gold",
    )

    generate_docs = DbtOperator(
        task_id="6_dbt_docs_generate",
        dbt_root_dir=DBT_ROOT_DIR,
        dbt_command="docs generate",
        target="dev",
    )

    @task
    def end_pipeline(meta: dict):
        logger.info(f"Pipeline {meta['pipeline_id']} hoàn thành!")
        return {"status": "completed"}

    end = end_pipeline(start)

    (
        start
        >> prepare_bronze
        >> load_bronze
        >> validate_bronze
        >> bronze_to_silver
        >> silver_to_gold
        >> gold_marts
        >> generate_docs
        >> end
    )


dag = ecommerce_pipeline()
