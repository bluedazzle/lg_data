# coding: utf-8
from __future__ import unicode_literals

import re

import logging
from bs4 import BeautifulSoup

from lg_data.db.pagination import query_by_pagination
from lg_data.db.models import DBSession, ZHArticle


def fix_href(article):
    soup = BeautifulSoup(article.content, "lxml")
    finds = soup.find_all('a')
    for itm in finds:
        href = itm.get('href', '')
        res = re.findall(r'/p/([0-9]+)', href)
        if res:
            logging.info(article.title)
            itm['href'] = 'https://www.wznav.com/article/{0}/'.format(res[0])
    article.content = soup.prettify()


def main():
    session = DBSession()
    for queryset in query_by_pagination(session, ZHArticle):
        for article in queryset:
            fix_href(article)
        try:
            session.commit()
        except Exception as e:
            session.rollback()


if __name__ == '__main__':
    main()
