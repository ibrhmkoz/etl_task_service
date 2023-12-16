from celery.result import AsyncResult
from flask import Blueprint, request, url_for, jsonify
from flask_app.tasks import start_etl_task as start_etl_celery_task

api = Blueprint('api', __name__)


@api.route('/etl_task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task_result = AsyncResult(task_id)
    response = {
        "task_id": task_id,
        "status": task_result.status
    }
    return jsonify(response)


@api.route('/etl_task', methods=['POST'])
def start_etl_task():
    task_request = request.get_json()
    task = start_etl_celery_task.delay(task_request)

    response = {
        "content": "ETL task started successfully",
        "_links": [
            {
                "href": url_for('api.get_task_status', task_id=task.id, _external=False),
                "rel": "status",
                "type": "GET"
            },
            {
                "href": url_for('api.abort_etl_task', task_id=task.id, _external=False),
                "rel": "abort",
                "type": "DELETE"
            },
            {
                "href": url_for('api.start_etl_task', _external=False),
                "rel": "self",
                "type": "POST"
            }
        ]
    }
    return jsonify(response), 202


@api.route('/etl_task/<task_id>', methods=['DELETE'])
def abort_etl_task(task_id):
    task = start_etl_celery_task.AsyncResult(task_id)
    task.abort()
    response = {"content": "ETL task aborted successfully"}
    return jsonify(response), 200
