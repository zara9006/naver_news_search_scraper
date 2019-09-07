import json
import re
import requests
from utils import check_dir
from utils import current_timestamp
from utils import get_path
from utils import get_soup
from utils import str_date_extraction
from bs4 import BeautifulSoup

from config import root
from config import debug
from config import verbose
from config import version
from config import SLEEP

# oid:언론사ID, aid:뉴스기사ID,

_info_to_crawl = ('sid1', 'sid2', 'oid', 'aid', 'url',
                  'office', 'title', 'contentHtml', 'content',
                  'crawledTime', 'writtenTime', 'crawlerVersion')


# 스크래핑 함수
def scrap(url):
    try:
        json_dict = _extract_content_as_dict(url)
        return json_dict
    except Exception as e:
        print(url, e)
        return {}
        # write log


# 스크래핑 시작
def _extract_content_as_dict(url):
    def remove_unnecessary_info_from_json_dict(json_dict):
        trimmed_dict = dict()
        for key, value in json_dict.items():
            if value and key in _info_to_crawl:
                trimmed_dict[key] = value
        return trimmed_dict

    # 최종 url과 파라미터리스트
    url, attributes = _parse_and_redirect_url(url)
    # 기사 HTML
    soup = get_soup(url)

    # 제목, 작성일, 내용,
    if 'sports' in url:
        json_dict = _parse_sport(soup)
    elif 'entertain' in url:
        json_dict = _parse_entertain(soup)
    else:
        json_dict = _parse_basic(soup)

    json_dict.update(attributes)
    json_dict.update({
        'url': url,
        'crawlerVersion': version,
        'crawledTime': current_timestamp()
    })
    json_dict = remove_unnecessary_info_from_json_dict(json_dict)
    return json_dict


# 최종 url 주소와 파라미터리스트 반환
def _parse_and_redirect_url(url):
    def redirect(url):
        try:
            response = requests.get(url)
            # response.history는 최종 리다이렏트 URL로 연겨로디는 응답 주소 목록이라고 한다.
            # history가 있다면 해당 주소를 반환하고 없다면 원래 url이 최종 주소가 된다.
            redirected_url = response.url if response.history else url
            return redirected_url
        except Exception as e:
            raise ValueError('redirection error %s' % str(e))

    # url에서 파라미터들을 딕셔너리로 추출해서 반환
    def parse_attribute_of_url(url):
        # 오래 된 링크에서 office_id나 article_id로 되어있는 링크가 있던 경우가 있었나 보다.. ( 추측 )
        url = url.replace('office_id', 'oid')
        url = url.replace('article_id', 'aid')
        # get방식에서 ?가 없으면 파라미터도 없는 것
        if ('?' in url) == False:
            return {}
        # 파라미터들을 딕셔너리로 저장
        parts = url.split('?')[1].split('&')
        parts = {part.split('=')[0]: part.split('=')[1] for part in parts}
        return parts

    # sid가 없거나 스포츠나 연예기사가 아니라면 그냥 sid를 반환 ( 숫자형태 )
    # 스포츠나 연예기사라면 ( sport나 entertain이라는 문자열로 보기 좋게 변환 )
    def masking_sid1(url, sid1):
        if not sid1: return sid1
        if 'sports.news' in url: return 'sport'
        if 'entertain' in url: return 'entertain'
        return sid1

    # url 요청 파라미터
    attributes = parse_attribute_of_url(url)
    # 해당 키값이 없다면 None으로 기본값 설정
    for key in ['sid1', 'sid2', 'oid', 'aid']:
        if (key in attributes) == False:
            attributes[key] = None

    redirected_url = redirect(url)
    attributes['sid1'] = masking_sid1(redirected_url, attributes['sid1'])

    return redirected_url, attributes


def _parse_content(html):
    content = []
    html = re.sub('<\\!--[^>]*-->', '', html.decode())  # Remove Comments
    html = re.sub('\n', '<br/>', html)  # Preseve Line Change
    html = re.sub('<a.*/a>', '', html)  # Remove Ads
    html = re.sub('<em.*/em>', '', html)
    html = re.sub('<script.*/script>', '', html)  # Remove Java Script
    html = re.sub('</?b>', '<br/>', html)
    html = re.sub('</?p>', '<br/>', html)
    for line in html.split('<br/>'):
        line = re.sub('<[^>]*>', '', line).strip()
        if not line:
            continue
        if line[0] not in ['\\', '/'] and line[-1] != ';':
            content.append(line)
    content = '\n'.join(content) if content else ''
    return content


def _parse_sport(soup):
    title = soup.select('div[class=news_headline] h4')
    title = title[0].text if title else None

    written_time = soup.select('div[class=news_headline] div[class=info] span')
    written_time = written_time[0].text if written_time else None
    written_time = str_date_extraction(written_time)

    content_html = soup.select('div[id=newsEndContents]')
    content_html = content_html[0] if content_html else None

    content = _parse_content(content_html) if content_html else None

    return {
        'title': title,
        'writtenTime': written_time,
        'contentHtml': content_html.decode(),
        'content': content
    }


def _parse_entertain(soup):
    title = soup.select('h2[class=end_tit]')
    title = title[0].text if title else None
    title = title.strip()

    written_time = soup.select('div[class=article_info] span')
    written_time = written_time[0].text if written_time else None
    written_time = str_date_extraction(written_time)

    content_html = soup.select('div[id=articeBody]')
    content_html = content_html[0] if content_html else None
    content = _parse_content(content_html) if content_html else None

    return {
        'title': title,
        'writtenTime': written_time,
        'contentHtml': content_html.decode(),
        'content': content
    }


def _parse_basic(soup):
    title = soup.select('h3[id=articleTitle]')
    title = title[0].text if title else None

    written_time = soup.select('span[class=t11]')
    written_time = written_time[0].text if written_time else None
    written_time = str_date_extraction(written_time)

    content_html = soup.select('div[id=articleBodyContents]')
    content_html = content_html[0] if content_html else None

    content = _parse_content(content_html) if content_html else None

    return {'title': title,
            'writtenTime': written_time,
            'contentHtml': content_html.decode(),
            'content': content
            }


class BatchArticleCrawler:
    sid1_list = ['1{}'.format('%02d' % i) for i in range(0, 11)]

    def __init__(self, *, year, month, date, root=root,
                 debug=False, verbose=False, version=None, name=''):
        self.root = root
        self.year = str(year)
        self.month = '%02d' % month if type(month) == int else month
        self.date = '%02d' % date if type(date) == int else date
        self.debug = debug
        self.verbose = verbose
        self.version = '0.0' if version == None else version
        self._name = name

    def scrap_a_day_as_corpus(self):
        urls = self._get_urls_from_breaking_news()
        n_successes = 0

        docs = []
        indexs = []
        oid_aids = []

        for i, url in enumerate(urls):
            try:
                json_dict = scrap(url)
                content = json_dict.get('content', '')
                if not content:
                    continue
                index = '{}\t{}\t{}\t{}'.format(
                    get_path(json_dict['oid'], self.year, self.month, self.date, json_dict['aid']),
                    json_dict.get('sid1', ''),
                    json_dict.get('writtenTime', ''),
                    json_dict.get('title', '')
                )
                docs.append(content.replace('\n', '  ').replace('\r\n', '  ').strip())
                indexs.append(index)
                oid_aids.append((json_dict['oid'], json_dict['aid']))
                n_successes += 1
            except Exception as e:
                print('Exception: {}\n{}'.format(url, str(e)))
                continue
            finally:
                if i % 1000 == 999:
                    print('\r  - {}scraping {} in {} ({} success) ...'.format(self._name + (': ' if self._name else ''),
                                                                              i + 1, len(urls), n_successes),
                          flush=True, end='')
        print('\rScrapped news')
        return docs, indexs, oid_aids

    def _get_urls_from_breaking_news(self):
        import time

        base_url = 'http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1={}&date={}&page={}'
        yymmdd = self.year + self.month + self.date
        links_in_all_sections = set()

        for sid1 in self.sid1_list:
            links_in_a_section = set()
            last_links = set()
            page = 1

            while page < 1000:
                url = base_url.format(sid1, yymmdd, page)
                soup = get_soup(url)
                links = soup.select('div[class^=list] a[href^=http]')
                links = [link.attrs.get('href', '') for link in links]
                links = {link for link in links if 'naver.com' in link and 'read.nhn?' in link}

                if last_links == links:
                    break

                links_in_a_section.update(links)
                last_links = {link for link in links}

                if self.verbose:
                    print('\rpage = {}, links = {}'.format(page, len(links_in_a_section)), flush=True, end='')

                page += 1
                if self.debug and page >= 3:
                    break
                time.sleep(SLEEP)

            links_in_all_sections.update(links_in_a_section)
            if self.verbose:
                print('\rsection = {}, links = {}'.format(sid1, len(links_in_a_section)))

        print('date={} has {} news'.format(yymmdd, len(links_in_all_sections)))
        return links_in_all_sections