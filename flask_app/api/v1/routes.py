from flask import Blueprint

api = Blueprint('api', __name__)


@api.route('/etl_task', methods=['GET'])
def start_etl_task():
    return "Starting ETL task"
