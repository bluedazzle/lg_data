# coding: utf-8
from __future__ import unicode_literals

import json
import os
import redis
import logging
import requests

from celery import Celery

from lg_data.db.models import ZHColumn, DBSession, ZHArticle
from lg_data.db.pagination import query_by_pagination

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

app.config_from_object('lg_data.queue.celery_config')


@app.task()
def top_column_task():
    logging.info('Start collect top 1000 column to redis...')
    session = DBSession()
    try:
        r = redis.StrictRedis(host='localhost', port=6379, db=1)
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
        return True
    except Exception as e:
        logging.exception('ERROR in collect top column reason {0}'.format(e))
        return False
    finally:
        session.close()


@app.task
def total_column_task():
    logging.info('Start collect all column to redis...')
    session = DBSession()
    r = redis.StrictRedis(host='localhost', port=6379, db=1)
    for columns in query_by_pagination(session, ZHColumn):
        slugs = [column.slug for column in columns]
        r.sadd('top_column_slug', *slugs)
    r.expire('top_column_slug', 60 * 60 * 23)
    logging.info('Success collect all  colmun to redis!')


@app.task
def top_column_spider_task():
    logging.info('Start crawling top column...')
    os.system('cd /var/www/site/run/ && ./topzl.sh')
    logging.info('Success crawl top column!')
    return True


@app.task
def generate_keywords_task(token):
    from lg_data.queue.utils import generate_keywords

    session = DBSession()
    article = session.query(ZHArticle).filter(ZHArticle.md5 == token).first()
    if not article:
        return False
    generate_keywords(article)
    session.commit()
    return True


@app.task
def sync_column_to_redis():
    logging.info('Start sync column to redis...')
    session = DBSession()
    try:
        cache = redis.StrictRedis(host='localhost', port=6379, db=1)
        for queryset in query_by_pagination(session, ZHColumn):
            for column in queryset:
                cache.sadd('total_column', column.slug)
        logging.info('Success sync column to redis')
        return True
    except Exception as e:
        logging.exception('ERROR in sync column reason {0}'.format(e))
        return False
    finally:
        session.close()


@app.task
def notify_baidu_new_url(urls):
    logging.info('Start notify url to baidu')
    url = 'http://data.zz.baidu.com/urls?site=https://www.wznav.com&token=cSZ81G7f4tUX78WO'
    try:
        if not urls:
            return True
        resp = requests.post(url, data=urls)
        json_data = json.loads(resp.content)
        if json_data.get('error', None):
            return False
        logging.info('Success notify url to baidu')
        return True
    except Exception as e:
        logging.exception('ERROR in notify baidu new url reason {0}'.format(e))
        return False
