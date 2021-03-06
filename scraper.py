'''
Kei Imada
20181227

Scraper for Swarthmore Prereq Visualizer
'''

import re
import bs4
import sys
import json
import requests
import argparse
from html import unescape
from itertools import chain
import multiprocessing as multi


BASEURL = 'http://catalog.swarthmore.edu/content.php?filter%5B27%5D=\
-1&filter%5B29%5D=&filter%5Bcourse_type%5D=-1&filter%5Bkeyword%5D=\
&filter%5B32%5D=1&filter%5Bcpage%5D={page_number}&cur_cat_oid=7&expand=\
1&navoid=191&print=1&filter%5Bexact_match%5D=\
1#acalog_template_course_filter'  # the url where we get the data from


VERBOSE = False  # verbose flag
# compiled regex to match courses
creg = re.compile('[A-Z]{4} \d{3}[A-Z]*(?: [A-Z]+)?')


def vprint(*args, **kwargs):
    ''' verbose printing '''
    if VERBOSE:
        print(*args, **kwargs)


def get_table(soup):
    ''' returns relevant table element from given soup '''
    return soup.find_all('table', attrs={'class': 'table_default'})[-1]


def get_course_rows(soup):
    ''' returns course rows from given soup '''
    table = get_table(soup)
    for br in table.find_all('br'):
        br.replace_with('\n'+br.text)
    return table.findChildren('tr', recursive=False)[2:-2]


def get_num_pages():
    ''' returns number of pages to parse '''
    req = requests.get(BASEURL.format(page_number=1))
    soup = bs4.BeautifulSoup(req.text, 'lxml')
    table = get_table(soup)
    rows = table.findChildren('tr', recursive=False)[-1]
    return int(rows.find_all('a')[-1].text)


def parse_course_page(url):
    ''' given url to request, requests, parses, and returns list of courses '''
    req = requests.get(url.format(page_number=1))
    soup = bs4.BeautifulSoup(req.text, 'lxml')
    rows = get_course_rows(soup)
    course_list = []
    for i, r in enumerate(rows):
        # get rid of extra newlines and spaces
        # -2 to get rid of annoying college bulletin urls
        row = list(map(lambda r: r.strip(),
                       re.split('\n', r.text.strip())))[:-2]
        row = unescape('\n'.join(filter(None, row)))
        row = ' '.join(filter(None, re.split(' ', row)))
        if len(row.strip()) > 0:
            # if there is anything in the row
            course = row[:row.index('.')]
            course_list.append({'course': course, 'text': row})
    return course_list


def parse_course_dict(cdict):
    '''
    given course dictionary, fills in other elements

    cdict: dictionary {
        'course': string the course name e.g. 'ANTH 093'
        'text': string the course description
    }
    '''
    text = cdict['text'].split('\n')
    cdict['department'] = cdict['course'].split()[0]
    cdict['prereq'] = ''
    for line in text:
        if line[:13] == 'Prerequisite:':
            cdict['prereq'] += ' ' + line[14:].strip()
    cdict['prereq'] = cdict['prereq'].strip()
    return cdict


def parse_courses(num_threads=None):
    ''' parses all the courses and writes to file '''
    pool = multi.Pool(num_threads)
    num_pages = get_num_pages()
    vprint(num_pages, 'pages to parse')
    url_list = [BASEURL.format(page_number=i+1) for i in range(num_pages)]
    courses = list(chain.from_iterable(pool.map(parse_course_page, url_list)))
    vprint(num_pages, 'pages parsed')
    courses.append({
        'course': 'MATH 026',
        'text': 'For students who place out of the first half of MATH 025. '
        'This course goes into more depth on sequences, series, and '
        'differential equations than does MATH 025. Students may not take '
        'MATH 026 for credit after MATH 025 without special permission.\n'
        'Prerequisite: Placement by examination (see "Advanced Placement and '
        'Credit Policy" section).\nNatural sciences and engineering.\n'
        '1 credit.\nFall 2018. Goldwyn.\nFall 2019. Staff.\nCatalog chapter: '
        'Mathematics and Statistics\nDepartment website: '
        'http://www.swarthmore.edu/mathematics-statistics'})
    courses.append({
        'course': 'MATH 028S',
        'text': 'Prerequisite: Placement by examination'})
    # ^ those darn exceptions
    parsed_courses = pool.map(parse_course_dict, courses)
    vprint(len(parsed_courses), 'course texts parsed')
    pool.close()
    return parsed_courses


def main():
    global VERBOSE
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--out', type=argparse.FileType('w'),
                        default=sys.stdout, help='Write scraped data to FILE',
                        metavar='FILE')
    parser.add_argument('-t', '--threads', type=int, metavar='N',
                        help='Use N threads (default: number of cores)',
                        default=None)
    parser.add_argument('-v', '--verbose', help='Be verbose',
                        action='store_true', default=False)

    args = parser.parse_args()
    VERBOSE = args.verbose
    parsed_courses = parse_courses(num_threads=args.threads)
    args.out.write(json.dumps(parsed_courses, separators=(',', ':')))
    vprint(len(parsed_courses), 'courses written to', args.out.name)
    args.out.close()


if __name__ == '__main__':
    main()
