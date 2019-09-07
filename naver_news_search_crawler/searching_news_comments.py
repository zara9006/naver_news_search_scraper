import argparse
import os
from datetime import datetime,timedelta,date
from search_crawler import SearchCrawler

# 오늘 날짜
TODAY = datetime.now().strftime("%Y-%m-%d")

# 질의어 파일 read 함수
def parse_query_file(query_file, begin_date, end_date):
    """
    query_file : str
        검색어 파일 경로 ( Default : queries.txt )
    begin_date : str
        크롤링 시작 날짜 ( Default : 30일 전 )
    end_date : str
        크롤링 종료 날짜 ( Default : 오늘 )
    """
    with open(query_file, encoding='utf-8') as f:
        docs = [doc.strip().split('\t') for doc in f]
    if not docs:
        raise ValueError('Query file must be inserted')
    args = []
    for i, cols in enumerate(docs):
        query = cols[0]
        outname = '%d'%i
        bd = begin_date
        ed = end_date

        if len(cols) >= 2:
            outname = cols[1]
        if len(cols) == 4:
            bd = cols[2]
            ed = cols[3]
        args.append((query, outname, bd, ed))
    return args

# 메인 함수
def main():

    """ 시작 옵션 파싱  """

    parser = argparse.ArgumentParser()
    parser.add_argument('--root_directory', type=str, default='../output/', help='수집한 뉴스와 댓글의 저장 위치')
    parser.add_argument('--begin_date', type=str, default=(date.today() - timedelta(30)).isoformat(),help='시작 날짜 : datetime yyyy-mm-dd ( default : 30일 전 )')
    parser.add_argument('--end_date', type=str, default=TODAY, help='종료 날짜 : datetime yyyy-mm-dd ( default : 오늘 )')
    parser.add_argument('--sleep', type=float, default=0.1, help='네이버 서버에 부하를 주지 않기 위한 여유시간 설정 ( 단위 : 초 )')
    parser.add_argument('--header', type=str, default=None, help='저장 폴더 이름')
    parser.add_argument('--query_file', type=str, default='queries.txt', help='질의어 텍스트 파일 지정')
    parser.add_argument('--debug', dest='DEBUG', action='store_true')
    parser.add_argument('--verbose', dest='VERBOSE', action='store_true')
    parser.add_argument('--comments', dest='GET_COMMENTS', action='store_true')
    parser.add_argument('--dbsave', dest='DBSAVE', action='store_true',help='몽고db 저장 설정파일 = db_settings.py')

    args = parser.parse_args()
    root_directory = args.root_directory
    begin_date = args.begin_date
    end_date = args.end_date
    sleep = args.sleep
    header = args.header
    DEBUG = args.DEBUG
    VERBOSE = args.VERBOSE
    DBSAVE = args.DBSAVE
    GET_COMMENTS = args.GET_COMMENTS
    now_strf = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    root_header = now_strf if header is None else header
    root_directory += '/%s/' % root_header
    query_file = args.query_file

    if not os.path.exists(query_file):
        raise ValueError('Query file are not found: {}'.format(query_file))


    """ 검색어 파일 탐색  """

    scraping_args = parse_query_file(query_file, begin_date, end_date)


    """ 검색어 별 크롤링 시작 """

    for query, outname, bd, ed in scraping_args:
        directory = '{}/{}/'.format(root_directory, outname)
        if bd > ed:
            continue
        crawler = SearchCrawler(directory, VERBOSE, DEBUG, DBSAVE, GET_COMMENTS, header, sleep)
        crawler.search(query, bd, ed)


if __name__ == '__main__':
    main()