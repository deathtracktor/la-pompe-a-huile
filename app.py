from contextlib import contextmanager
from datetime import date, timedelta
from functools import partial
from itertools import count
from urllib.parse import urlparse, parse_qs
import time

from bs4 import BeautifulSoup
import requests
from requests.cookies import create_cookie
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

HOME_URL = 'https://e-disclosure.ru/'
SEARCH_URL = 'https://e-disclosure.ru/poisk-po-soobshheniyam'

POST_ARGS_TEMPL = {
    'lastPageSize': 10,
    'query': '',
    'queryEvent': '',
    'eventTypeTerm': '',
    'radView': '0',
    'eventTypeCheckboxGroup': [
        97, 81, 100, 101, 102, 103, 105, 106, 107, 150, 205, 206, 232,
    ],
    'textfieldEvent': '',
    'radReg': 'FederalDistricts',
    'districtsCheckboxGroup': -1,
    'regionsCheckboxGroup': -1,
    'branchesCheckboxGroup': -1,
    'textfieldCompany': '',
}

def make_payload(page):
    """Format POST request payload."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    extra = {
        'lastPageNumber': page,
        'dateStart': yesterday.strftime('%d.%m.%Y'),
        'dateFinish': today.strftime('%d.%m.%Y'),
    }
    return {**POST_ARGS_TEMPL, **extra}


def get_session_cookies():
    """Open search page, grab the relevant session cookies."""
    opts = Options()
    opts.headless = True
    driver = webdriver.Firefox(options=opts)
    print('Fetching session cookies...')
    try:
        driver.get(HOME_URL)
        cookies = driver.get_cookies()
    finally:
        driver.close()
    return [
        create_cookie(domain=c['domain'], name=c['name'], value=c['value'])
        for c in cookies
    ]


@contextmanager
def DisclosureClient():
    """The client context manager."""
    with requests.Session() as session:
        for c in get_session_cookies():
            session.cookies.set_cookie(c)
        session.post = partial(session.post, SEARCH_URL)
        yield session


def parse_page(html):
    """Parse HTML page code, scrape the relevant stuff."""
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find('table').findAll('tr')
    if not rows:
        return
    for row in rows:
        cols = row.findAll('td')
        anchors = cols[1].findAll('a')
        org, title = anchors[0].text, anchors[1].text
        addr = urlparse(anchors[0].attrs['href'])
        item_id = parse_qs(addr.query)['id'][0]
        yield {
            'id': item_id,
            'org': org,
            'title': title,
        }

def scrap_site():
    """Load paginated results, extract relevant information."""
    with DisclosureClient() as client:
        for page in count(start=1):
            resp = client.post(data=make_payload(page))
            assert resp.status_code == 200
            items = list(parse_page(resp.text))
            if not items:
                break
            yield from items
            time.sleep(1)
    

def run_main():
    """Run the application."""
    for n, item in enumerate(scrap_site()):
        print(n, item)

if __name__ == '__main__':
    run_main()
