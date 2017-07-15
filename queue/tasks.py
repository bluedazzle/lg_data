# coding: utf-8
from __future__ import unicode_literals

from celery import Celery
from sqlalchemy import func

import sys
import os
import redis

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

from db.models import DBSession

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

app.config_from_object('celery_config')


@app.task(bind=True)
def period_task(self):
    session = DBSession()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    sr = session.execute("""SELECT slug FROM
                        (
                            SELECT id,slug FROM core_zhcolumn
                        ) as l1
                        JOIN
                        (
                            SELECT count(core_zharticle.id) AS c1, belong_id
                            FROM core_zharticle
                            GROUP BY belong_id
                        ) as l2
                        ON l2.belong_id = l1.id
                        ORDER BY l2.c1 DESC
                        LIMIT 1000;""")
    for itm in sr:
        r.sadd('top_column_slug', itm[0])
    r.expire('top_column_slug', 60 * 60 * 23)