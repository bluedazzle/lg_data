# coding: utf-8

from datetime import timedelta
from celery.schedules import crontab

CELERYBEAT_SCHEDULE = {
    'top-column-task': {
        'task': 'tasks.top_column_task',
        'schedule': crontab(minute=0, hour=3),
    },
    'top-column-spider-task': {
        'task': 'tasks.top_column_spider_task',
        'schedule': crontab(minute=0, hour='6,17')
    },
}

CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

CELERY_TIMEZONE = 'Asia/Shanghai'