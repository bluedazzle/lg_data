# coding: utf-8
from __future__ import unicode_literals

import math

import logging
from sqlalchemy import func


def query_by_pagination(session, obj, order_by='id', start_offset=0, limit=1000):
    total = session.query(func.count(obj.id)).scalar()
    total_page = int(math.ceil(total / float(limit)))
    start = 0
    if start_offset:
        start = start_offset / limit

    for i in xrange(start, total_page):
        offset = limit * i
        result = session.query(obj).order_by(order_by).limit(limit).offset(offset).all()
        logging.info('Current {0}->{1}/{2} {3}%'.format(offset, offset + limit, total, (offset + limit) / total * 100))
        yield result
