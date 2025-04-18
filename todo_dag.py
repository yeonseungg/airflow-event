# from __future__ import annotations

# import json
# import pathlib
# from datetime import datetime

# import requests
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

# # 결과 저장 경로 (컨테이너 내부 기준) ───────────────────────────────
# DATA_DIR = pathlib.Path("/opt/airflow/data")
# DATA_DIR.mkdir(parents=True, exist_ok=True)
# TARGET_FILE = DATA_DIR / "test.json"


# def build_url() -> str:

#     minute_plus = (datetime.now().minute + 1) % 60 or 60
#     return f"https://jsonplaceholder.typicode.com/todos/{minute_plus}"


# def fetch_and_save(**context):
#     url = build_url()
#     try:
#         resp = requests.get(url, timeout=15)
#         resp.raise_for_status()
#     except requests.RequestException as e:
#         # Airflow 로그 확인을 위해 예외 그대로 올린다
#         raise RuntimeError(f"API 요청 실패: {e}") from e

#     # Pretty‑print JSON을 파일에 기록
#     TARGET_FILE.write_text(
#         json.dumps(resp.json(), ensure_ascii=False, indent=2),
#         encoding="utf‑8",
#     )


# default_args = {
#     "owner": "exam",
#     "retries": 1,
# }

# with DAG(
#     dag_id="fetch_todo",
#     description="5분마다 JSONPlaceholder TODO 수집 후 test.json 저장",
#     default_args=default_args,
#     start_date=days_ago(1),
#     schedule_interval="*/5 * * * *", 
#     catchup=False,                    
#     tags=["exam", "github-trigger"],
# ) as dag:

#     fetch_task = PythonOperator(
#         task_id="fetch_todo_task",
#         python_callable=fetch_and_save,
#         provide_context=True,
#     )

#     fetch_task
