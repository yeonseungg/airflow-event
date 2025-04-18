from __future__ import annotations

import json
import pathlib
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging


def build_url() -> str:
    """
    현재 시각의 분(minute)에 +1을 더하여 1~60 범위로 변환해
    JSONPlaceholder의 TODO 항목 URL을 생성
    """
    minute_plus = (datetime.now().minute + 1) % 60 or 60
    return f"https://jsonplaceholder.typicode.com/todos/{minute_plus}"


def fetch_and_save(**kwargs) -> None:
    """
    지정된 API로부터 데이터를 수집하고
    /opt/airflow/data/test.json에 저장
    """
    logger = logging.getLogger("airflow.task")

    url = build_url()
    logger.info(f"[todo1_dag] Fetching URL: {url}")

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        json_data = resp.json()
    except requests.RequestException as e:
        logger.error(f"[todo1_dag] API 요청 실패: {e}")
        raise RuntimeError(f"API 요청 실패: {e}") from e

    # 저장 디렉터리 및 파일 경로
    data_dir = pathlib.Path("/opt/airflow/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    target_file = data_dir / "test.json"

    # 저장
    with target_file.open("a", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    logger.info(f"[todo1_dag] Data saved to {target_file}")


# DAG 정의
default_args = {
    "owner": "exam",
    "retries": 1,
}

with DAG(
    dag_id="todo1_dag",
    description="5분마다 JSONPlaceholder TODO 수집 후 test.json 저장",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",  # 5분마다
    catchup=False,
    tags=["exam", "github-trigger"],
) as dag:

    fetch_todo_task = PythonOperator(
        task_id="todo1_dag_task",
        python_callable=fetch_and_save,
    )

    fetch_todo_task
