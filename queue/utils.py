# coding: utf-8
from __future__ import unicode_literals

from bs4 import BeautifulSoup
import jieba.analyse

def generate_keywords(obj):
    soup = BeautifulSoup(obj.content, "lxml")
    raw_text = soup.get_text()
    obj.summary = "".join(raw_text[:200].split())
    title_list = [itm for itm in jieba.cut(obj.title) if len(itm) > 1]
    seg_list = jieba.analyse.extract_tags(raw_text, topK=100, withWeight=False)
    seg_list = seg_list[:20]
    seg_list.extend(title_list)
    seg_list = set(seg_list)
    obj.keywords = ','.join(seg_list)
    print obj.keywords