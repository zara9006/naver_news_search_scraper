import json
import os
import requests
import re
import urllib
from bs4 import BeautifulSoup
from datetime import datetime

def load_comments(fname):
    with open(fname, encoding='utf-8') as f:
        docs = [doc.strip() for doc in f]
    if docs[0][-15:] != 'antipathy_count':
        first = docs[0].split('antipathy_count')[0] + 'antipathy_count'
        second = docs[0].split('antipathy_count')[1]
        docs = [first, second] + docs[1:]
    return docs

def check_dir(fname):
    folder = os.path.dirname(os.path.abspath(fname))
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
        print('make directory: %s' % folder)

def load_docs(fname):
    try:
        with open(fname, encoding='utf-8') as f:
            return [doc.replace('\n', '') for doc in f]
    except:
        return []

def write_docs(docs, fname):
    check_dir(fname)
    with open(fname, 'w', encoding='utf-8') as f:
        for doc in docs:
            f.write('%s\n' % doc)

def write_json(json_object, fname):
    check_dir(fname)
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(json_object, f, ensure_ascii=False, indent=2)

def load_json(fname):
    try:
        with open(fname, encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}    

def get_path(*dir_path_list):
    dir_path = '/'.join(dir_path_list)
    # TODO: Do not Hard Code 'File Seperator' Symbol
    return dir_path

def current_timestamp():
    return str(datetime.now())

# user-agent를 지정해서 기계가 아닌 일반 유저가 브라우저로 접근한 것처럼 한거 같다. ( 추측 )
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

# BeautifulSoup를 사용하여 html을 파싱해서 반환
def get_soup(url):
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'lxml')
        return soup
    except Exception as e:
        raise ValueError('failed in util.getDocument(), %s' % str(e))
        return None

# 문자열 날짜를 날짜타입으로
def convert_str_date_to_datetime(string_date):
    try:
        return datetime.strptime(string_date, '%Y-%m-%d')
    except Exception as e:
        raise ValueError('Failed to Convert String to Datetime %s' % str(e))

# 날짜타입을 문자열로
def convert_datetime_to_str(date):
    try:
        return date.isoformat().split('T')[0]
    except Exception as e:
        raise ValueError('Failed to Convert Datetime to String %s' % str(e))

def url_encode(query, encoding='utf-8'):
    def encode_a_term(term):
        try:
            return urllib.parse.quote(term, encoding=encoding)
        except Exception as e:
            raise ValueError('Failed to encode query %s' % str(e))
    return '+'.join([encode_a_term(term) for term in query.split()])

def str_date_extraction(string_date):
    date = re.search(r'\d{4}.\d{2}.\d{2}', string_date).group()
    time = re.search(r'\d{2}:\d{2}', string_date)
    if (time is None):
        time = re.search(r'\d{1}:\d{2}', string_date)

    time = time.group()

    if ('오후' in string_date):
        time = str((int(time.split(':')[0]) + 12)) + ":" + time.split(':')[1]

    return date+" "+time