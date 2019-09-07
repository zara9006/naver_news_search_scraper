import re
import os
import time
from comment_crawler import get_comments
from news_crawler import scrap
from datetime import datetime, timedelta
from utils import check_dir
from utils import convert_datetime_to_str
from utils import convert_str_date_to_datetime
from utils import get_soup
from utils import get_path
from utils import url_encode
from db_setting import host,username,password
from pymongo import MongoClient

# 검색결과 서칭한 기사 url들 반환
def get_article_urls(query, search_date, verbose=True, debug=True):

    """
        qeury : 검색어
        search_date : 검색 날짜 ( 하루씩 크롤링 )
        verbose : 크롤링 진행과정 로그 옵션
        debug : 디버깅 모드 옵션 ( 검색어마다 3페이지 씩만 크롤링하고 종료 )
    """

    search_result_url = _get_search_result_url(query, search_date) # 검색 url
    num_articles = _parse_article_num(search_result_url) # 검색된 기사 개수

    # 로그
    if verbose:
        print('검색 URL : %s' % search_result_url)
        print('{} -> 검색어 : {} , 총 기사 수 : {}'.format(search_date, query, num_articles))

    # url 리스트
    urls = _extract_urls_from_search_result(search_result_url, num_articles, verbose, debug)

    return urls

def get_article_num(query, start_date, end_date=None):
    if end_date == None:
        end_date = start_date
    search_result_url = _get_search_result_url(query, start_date, end_date)
    return _parse_article_num(search_result_url)

# 현재 검색어로 검색된 총 기사 개수
def _parse_article_num(search_result_url):
    try:
        soup = get_soup(search_result_url)
        if not soup:
            return 0

        result_header_text = soup.select('div[class=section_head] div[class^=title_desc] span')
        if not result_header_text:
            return 0

        result_header_text = result_header_text[0].text
        result_header_text = re.findall('[,\\d]+건', result_header_text)[0]
        result_header_text = re.sub(',', '', result_header_text)
        num_articles = int(result_header_text[:-1])
        return num_articles

    except Exception as e:
        raise ValueError('Failed to get total number of articles %s' % str(e))

# 검색 URL 완성 함수 ( 네이버 뉴스기사 검색 )
def _get_search_result_url(query, search_date):
        url_prefix = 'https://search.naver.com/search.naver?where=news&query={0}&sm=tab_opt&sort=0&photo=0&field=0&reporter_article=&pd=3&ds={1}&de={2}'
        search_date_ = search_date.replace('-', '.')
        search_result_url = url_prefix.format(url_encode(query), search_date_, search_date_)
        return search_result_url

def _article_num_to_page_num(num_articles):
    num_pages = num_articles // 10
    if num_articles % 10 != 0:
        num_pages += 1
    return num_pages

# url set 반환 ( 중복 url 방지를 위해 set 사용 )
def _extract_urls_from_search_result(search_result_url, num_articles, verbose=True, debug=True):
    urls = set()
    num_pages = _article_num_to_page_num(num_articles)
    page = 0
    for page in range(num_pages):
        urls_in_page = _parse_urls_from_page(search_result_url, page)
        urls.update(urls_in_page)
        if verbose and page % 5 == 0:
            print('  .. extract urls: page= {}, #urls= {}'.format(page, len(urls)))
        if debug and page >= 3:
            break
    if verbose:
        print('  .. extract urls: page= {}, #urls= {}'.format(page, len(urls)))
    return urls

def _parse_urls_from_page(base_url, page):

    url_patterns = ('a[href^="https://news.naver.com/main/read.nhn?"]',
            'a[href^="https://entertain.naver.com/main/read.nhn?"]',
            'a[href^="https://sports.news.naver.com/sports/index.nhn?"]',
            'a[href^="https://news.naver.com/sports/index.nhn?"]')

    urls_in_page = set()
    page_url = '{}&start={}&refresh_start=0'.format(base_url, 1 + 10*(page-1))
    soup = get_soup(page_url)
    if not soup:
        return urls_in_page
    try:
        article_blocks = soup.select('ul[class=type01]')[0]
        for pattern in url_patterns:
            article_urls = [link['href'] for link in article_blocks.select(pattern)]
            urls_in_page.update(article_urls)
    except Exception as e:
        raise ValueError('Failed to extract urls from page %s' % str(e))

    return urls_in_page

# 크롤링 객체
class SearchCrawler:
    # 생성자 옵션값 초기화
    def __init__(self, root, verbose, debug, dbsave, comments, header=None, sleep=0.03):
        self.root = root
        self.verbose = verbose
        self.debug = debug
        self.comments = comments
        if header is None:
            header = ''
        self.header = header
        self.header_strf = '' if not header else '_' + header
        self.sleep = sleep
        self.dbsave = dbsave

    # 검색 함수
    def search(self, query, start_date, end_date=None):
        """
            start_date: str
               ex) 2017-05-01
        """
        if end_date == None:
            end_date = start_date

        start_date = convert_str_date_to_datetime(start_date)
        end_date = convert_str_date_to_datetime(end_date)

        """ 날짜별로 크롤링 """

        for i in range((end_date - start_date).days + 1):
            scrap_date = start_date + timedelta(days=i)
            scrap_date = convert_datetime_to_str(scrap_date)
            year, month, date = scrap_date.split('-')
            urls = get_article_urls(query, scrap_date, verbose=self.verbose, debug=self.debug)

            docs = []
            indexs = []
            comments = []

            for i in range((end_date - start_date).days + 1):
                scrap_date = start_date + timedelta(days=i)  # 시작날짜부터 하루씩 증가
                scrap_date = convert_datetime_to_str(scrap_date)  # 문자열로 다시 변경
                year, month, date = scrap_date.split('-')  # 년, 월, 일 분리
                # 해당 날짜에 해당하는 검색 기사 url 리스트를 구해옴
                urls = get_article_urls(query, scrap_date, self.verbose, self.debug)

                docs = []
                indexs = []
                comments = []

                for i, url in enumerate(urls):
                    # verbose 옵션이 있으면 50개마다 진행상황 출력
                    if self.verbose and i % 50 == 0:
                        print('\r  - scrapping {} / {} news'.format(i + 1, len(urls)), end='')

                    try:
                        json_dict = scrap(url)
                        content = json_dict.get('content', '')
                        if not content:
                            continue
                        # { 언론사ID/년/월/일/뉴스기사ID } \t { sid1 카테고리 이름 or 번호 } \t { 뉴스기사 작성시간 } \t { 기사제목 }
                        index = '{}\t{}\t{}\t{}'.format(
                            get_path(json_dict['oid'], year, month, date, json_dict['aid']),
                            json_dict.get('sid1', ''),
                            json_dict.get('writtenTime', ''),
                            json_dict.get('title', '')
                        )

                        # docs에 기사 내용 추가
                        docs.append(content.replace('\n', '  ').replace('\r\n', '  ').strip())
                        # indexs에 index 추가
                        indexs.append(index)

                        # 댓글 수집 옵션이 있다면 get_comments 함수 실행
                        if self.comments:
                            comments.append(get_comments(url))

                        time.sleep(self.sleep)
                    except Exception as e:
                        print('Exception: {}\n{}'.format(url, str(e)))
                        continue

                if self.verbose:
                    print('\r  .. search crawler saved {} articles in {} on {}'.format(len(urls), len(urls),
                                                                                       year + month + date))

                if not docs:
                    continue

                if (self.dbsave):
                    self._save_mongodb(scrap_date, docs, indexs, comments, query)
                else:
                    self._save_news_as_corpus(scrap_date, docs, indexs)
                    if self.comments:
                        self._save_comments(scrap_date, indexs, comments)

            if self.verbose:
                print('Search Crawling For Query [{}] Time Between [{}] ~ [{}] Finished'.format(query, start_date,
                                                                                                end_date))

            return True

    def _save_mongodb(self, scrap_date, docs, indexs, comments, query):
        connection = MongoClient('mongodb://%s:%s@%s' % (username, password, host))
        db = connection['crawling']
        collection = db['naver_news']

        if (collection is None):
            print('database error')
            return

        print("Database insert start...")

        for i, (doc, index, comment) in enumerate(zip(docs, indexs, comments)):
            idx, category, news_date, title = index.split('\t')
            oid, year, month, day, aid = idx.split('/')

            insertData = {
                "query": query.split(' ')[0],
                "category": category,
                "title": title,
                "writeDate": news_date,
                "oid": oid,
                "aid": aid,
                "doc": doc,
                "comments": comment,
                "uploadDate": scrap_date
            }
            collection.insert(insertData)

            if self.verbose and i % 10 == 0:
                print("  .. MongoDB saved {} articles in {}".format(i, len(list(zip(docs, indexs, comments)))))

    def _save_news_as_corpus(self, scrap_date, docs, indexs):
        corpus_path = '{}/news/{}{}.txt'.format(self.root, scrap_date, self.header_strf)
        index_path = '{}/news/{}{}.index'.format(self.root, scrap_date, self.header_strf)

        # 디렉토리 생성
        check_dir(corpus_path)

        # 파일쓰기
        with open(corpus_path, 'w', encoding='utf-8') as f:
            for doc in docs:
                f.write('{}\n'.format(doc.strip()))
        with open(index_path, 'w', encoding='utf-8') as f:
            for index in indexs:
                f.write('{}\n'.format(index))

    def _save_comments(self, scrap_date, indexs, comments):
        def comment_filename(index):
            oid, _, _, _, aid = index.split('\t')[0].split('/')
            return '{}-{}'.format(oid, aid)

        columns = 'comment_no userName contents reg_time sympathy_count antipathy_count'.replace(' ', '\t')

        dirname = '{}/comments/{}/'.format(self.root, scrap_date)
        dirname = os.path.abspath(dirname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        for comments_, index in zip(comments, indexs):
            if not comments_:
                continue
            filename = comment_filename(index)
            path = '{}/{}.txt'.format(dirname, filename)

            with open(path, 'w', encoding='utf-8') as f:
                f.write(columns + '\n')
                for comment in comments_:
                    comment_strf = '\t'.join(str(v) for v in comment)
                    f.write('{}\n'.format(comment_strf))
