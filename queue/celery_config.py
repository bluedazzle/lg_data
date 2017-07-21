# coding: utf-8

from datetime import timedelta
from celery.schedules import crontab

CELERYBEAT_SCHEDULE = {
    'top-column-task': {
        'task': 'tasks.top_column_task',
        'schedule': crontab(minute=0, hour='3, 16'),
    },
    'top-column-spider-task': {
        'task': 'tasks.top_column_spider_task',
        'schedule': crontab(minute=0, hour='6,17')
    },
}

CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

CELERY_TIMEZONE = 'Asia/Shanghai'

CELERYD_CONCURRENCY = 1

CELERYD_MAX_TASKS_PER_CHILD = 50

CELERYD_FORCE_EXECV = True  # 非常重要,有些情况下可以防止死锁

CELERYD_PREFETCH_MULTIPLIER = 1
