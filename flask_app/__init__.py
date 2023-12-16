from flask import Flask

from flask_app.celery_init_app import celery_init_app
from .api.v1.routes import api as api_v1


def create_app():
    app = Flask(__name__)
    app.config.from_mapping(
        CELERY=dict(
            broker_url="redis://redis",
            result_backend="redis://redis",
            task_ignore_result=True,
        ),
    )
    celery_app = celery_init_app(app)
    app.register_blueprint(api_v1, url_prefix="/api/v1")
    return app, celery_app
