#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib3
import re
import time
from db import add_papers

def get_paper_name(string):
    result = re.findall('papel=(.*)">', string)
    return result

def get_paper_value(table):
    result = re.findall('<td>(.*)</td>', table)
    return result

def parse_in_dict(header, values):
    return dict(zip(header, values))
    
http = urllib3.PoolManager()
r = http.request('GET', 'http://www.fundamentus.com.br/resultado.php')
pattern = re.compile('<table id="resultado".*</table>', re.DOTALL)
content = re.findall(pattern, r.data)[0]

index = 0
all_papers = list()
rows = content.split('<tr class="')
table_headers = [
    'cotacao',
    'pl',
    'pvp',
    'psr',
    'yield',
    'pativo',
    'pcapgiro',
    'pebit',
    'pativcircliq',
    'evebit',
    'evebitda',
    'mrgebit',
    'mrgliq',
    'liqcorr',
    'roic',
    'roe',
    'liq2meses',
    'patrimliq',
    'divbrutapatrim',
    'crescrec',
    'paper',
    'timestamp'
]

rows.pop(0)
for row in rows:
    paper_name = get_paper_name(row)
    paper_values = get_paper_value(row)
    paper_values.append(paper_name[0])
    paper_values.append(time.time())
    paper_dict = parse_in_dict(table_headers, paper_values)
    all_papers.append(paper_dict)
    
# print(all_papers['PEFX5'])
add_papers(all_papers)

raw_input()
exit()