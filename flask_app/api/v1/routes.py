from flask import Blueprint, request
from flask_app.tasks import start_etl_task as start_etl_celery_task

api = Blueprint('api', __name__)


@api.route('/etl_task', methods=['POST'])
def start_etl_task():
    task_request = request.get_json()
    return start_etl_celery_task.delay(kwargs=task_request)


@api.route('/etl_task/<task_id>', methods=['DELETE'])
def abort_etl_task(task_id):
    task = start_etl_celery_task.AsyncResult(task_id)
    task.abort()
    return