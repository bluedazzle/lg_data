# coding: utf-8
from __future__ import unicode_literals

from celery import Celery
from sqlalchemy import func

import sys
import os
import redis
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

from db.models import DBSession, ZHArticle

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

app.config_from_object('lg_data.queue.celery_config')


@app.task(bind=True)
def top_column_task(self):
    logging.info('Start collect top 1000 column to redis...')
    session = DBSession()
    try:
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
        logging.info('Success collect top column to redis!')
    except Exception as e:
        logging.exception('ERROR in collect top column reason {0}'.format(e))
    finally:
        session.close()


@app.task(bind=True)
def top_column_spider_task(self):
    logging.info('Start crawling top column...')
    # todo
    logging.info('Success crawl top column!')


@app.task
def generate_keywords_task(token):
    from lg_data.queue.utils import generate_keywords

    session = DBSession()
    article = session.query(ZHArticle).filter(ZHArticle.md5 == token).first()
    if not article:
        return False
    generate_keywords(article)
    session.commit()
