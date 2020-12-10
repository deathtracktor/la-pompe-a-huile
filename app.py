from contextlib import contextmanager
from datetime import date, timedelta
from functools import partial
import json
from itertools import count
from hashlib import sha1
import time

import click
from bs4 import BeautifulSoup
import requests
from requests.cookies import create_cookie
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from sqlitedict import SqliteDict

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

def make_payload(page, days):
    """Format the POST request payload."""
    today = date.today()
    start_date = today - timedelta(days=days)
    extra = {
        'lastPageNumber': page,
        'dateStart': start_date.strftime('%d.%m.%Y'),
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
        ts = cols[0].text
        anchors = cols[1].findAll('a')
        org, title = anchors[0].text, anchors[1].text
        url = anchors[1].attrs['href']
        yield {
            'ts': ts,
            'url': url,
            'org': org,
            'title': title,
        }

def deduplicate(items):
    """Remove duplicated items."""
    return dict(zip(map(make_hash, items), items)).values()


def add_summary(items):
    """Fetch the item's summary page HTML."""
    for item in items:
        print('Fetching page "{url}"...'.format(**item))
        resp = requests.get(item['url'])
        assert resp.status_code == 200
        soup = BeautifulSoup(resp.content, 'html.parser')
        summary = soup.find('div', {'id': 'cont_wrap'}).text
        yield {**item, 'summary': summary}


def scrap_site(days):
    """Load paginated results, extract relevant information."""
    with DisclosureClient() as client:
        for page in count(start=1):
            resp = client.post(data=make_payload(page, days))
            assert resp.status_code == 200
            items = list(add_summary(deduplicate(parse_page(resp.text))))
            if not items:
                break
            yield from items
            time.sleep(1)

def make_hash(obj):
    """Make an object hash."""
    return sha1(json.dumps(obj, sort_keys=True).encode()).hexdigest()

def cache_object(obj):
    """Cache the object persistently, return True if already cached."""
    key = make_hash(obj)
    with SqliteDict('.cache') as cache:
        if key in cache:
            return True
        cache[key] = obj
        cache.commit()


@click.group()
def cli():
    """CLI command group."""
    pass


@cli.command(name='fetch')
@click.option('-d', '--days', type=int, default=1,
              help='Fetch event posted in the last N days')
def fetch_new_events(days):
    """Fetch and cache events posted during the last N days."""
    n = 0
    for n, item in enumerate(scrap_site(days)):
        if cache_object(item):
            break
    print('Cached {} new records.'.format(n))


@cli.command(name='report')
def make_report():
    """Analyze cached records, make a report."""
    pass


if __name__ == '__main__':
    cli()
