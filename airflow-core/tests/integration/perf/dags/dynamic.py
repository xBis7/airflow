from airflow import DAG
from airflow.sdk import task, Param
from datetime import datetime
import time

default_args = {
    'owner': 'test',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'configurable_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=50,
    params={
        "num_tasks": Param(
            default=10,
            type="integer",
            minimum=1,
            maximum=5000,
            description="Number of test tasks to create"
        ),
        "sleep_duration": Param(
            default=5,
            type="integer",
            minimum=1,
            maximum=60,
            description="Sleep duration for each task in seconds"
        )
    }
)

@task
def generate_task_numbers(**context):
    """Generate task numbers based on dag params"""
    num_tasks = context['params']['num_tasks']
    print(f"Generating {num_tasks} tasks")
    return list(range(num_tasks))

@task
def test_task(task_num: int, **context):
    """Configurable test task"""
    sleep_duration = context['params']['sleep_duration']
    print(f"task_{task_num}")
    time.sleep(sleep_duration)
    return f"Completed task_{task_num}"

@task
def summary_task(results: list, **context):
    """Summary of all completed tasks"""
    num_tasks = context['params']['num_tasks']
    print(f"Completed {len(results)}/{num_tasks} tasks")
    return {"requested": num_tasks, "completed": len(results)}

# Build the pipeline
task_numbers = generate_task_numbers()
mapped_tasks = test_task.expand(task_num=task_numbers)
summary = summary_task(mapped_tasks)

task_numbers >> mapped_tasks >> summary
