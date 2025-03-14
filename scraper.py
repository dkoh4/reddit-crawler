import requests
import sys
import sqlite3
from bs4 import BeautifulSoup
import os
import time


SEED_URL = 'https://www.reddit.com/best/communities'
DB_PATH = './db/database.db'
MIN_SUBSCRIBER_COUNT = 20000
MAX_RETRIES = 3


def main():
    if not os.path.exists(DB_PATH):
        initialize_database()
    
    result = scrape_community_index()
    update_database_status(result)
    print(result['message'])


def scrape_community_index():
    status = {'status': 'error', 'message': ''}
    page = get_page_index()
    if page == 0:
        status['status'] = 'success'
        status['message'] = 'Scraping completed successfully'
        print('Nothing to scrape. Set Retry to True to scrape again.')
        return status
    url = f'{SEED_URL}/{page}'
    retries = 0

    while True:
        print(f'Scraping {url}')
        result = process_url(url)

        if result['status'] == 'error':
            retries += 1
            if retries > MAX_RETRIES:
                status['message'] = f'Failed to scrape page {page} after {MAX_RETRIES} retries. Error: {result["message"]}'
                break
            time.sleep(1)
        else:
            if result['message'] == 'COMPLETED':
                status['message'] = 'Scraping completed successfully'
                break
            page = get_page_index()
            url = f'{SEED_URL}/{page}'
            retries = 0
    return status


def process_url(url):
    status = {'status': 'error', 'message': ''}

    try:
        response = requests.get(url)

        if response.status_code != 200:
            status['message'] = f'Failed to fetch page {url}. Status code: {response.status_code}'
            return status
    
        soup = BeautifulSoup(response.text, 'html.parser')
        communities = soup.find_all('div', {'data-prefixed-name': True}, {'data-subscribers-count': True})
        
        result = update_database_communities(communities)
        if result['message'] == 'COMPLETED':
            return result

        rate_limit_remaining = int(float(response.headers.get('x-ratelimit-remaining')))
        if rate_limit_remaining < 5:
            delay_time = int(response.headers.get('x-ratelimit-reset'))
            print(f'Rate limit reached. Waiting {delay_time} seconds')
            time.sleep(delay_time)
    except Exception as e:
        status['message'] = f'Error scraping page {url}: {str(e)}'
        with open('output/error.log', 'a') as f:
            f.write(response + '\n')
            f.write(response.headers + '\n')
            f.write(response.text + '\n')
    if result['status'] == 'error':
        return result

    return result


def update_database_status(status):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''update state set value = ? where name = 'Last Status' ''', (status['message'],))
    conn.commit()
    conn.close()


def update_database_communities(communities):
    status = {'status': 'error', 'message': ''}

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for community in communities:
            name = community['data-prefixed-name'].removeprefix('r/')
            subscriber_count = int(community['data-subscribers-count'])

            if subscriber_count < MIN_SUBSCRIBER_COUNT:
                status['message'] = 'COMPLETED'
                break
            cursor.execute('''insert into communities (name, subscriber_count) values (?, ?)''', (name, subscriber_count))
        cursor.execute('''update state set value = value + 1 where name = 'Page' ''')
        conn.commit()

        conn.close()
    except Exception as e:
        status['message'] = f'Error updating database: {str(e)}'
        return status

    status['status'] = 'success'
    return status


def get_page_index():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''select value from state where name = 'Retry' ''')
    retry = cursor.fetchone()[0]
    if retry == 'True':
        cursor.execute('''insert into value (name, state) values ('Page', 1)''')
        return 1

    cursor.execute('''select value from state where name = 'Last Status' ''')
    if cursor.fetchone()[0] == 'Scraping completed successfully':
        return 0
    
    cursor.execute('''select value from state where name = 'Page' ''')
    return cursor.fetchone()[0]


def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS state (
            name TEXT PRIMARY KEY,
            value NOT NULL
        )
    ''')
    cursor.execute('''insert into state (name, value) values ('Page', 1)''')
    cursor.execute('''insert into state (name, value) values ('Retry', 'False')''')
    cursor.execute('''insert into state (name, value) values ('Last Status', 'Initialized Database')''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS communities (
            name TEXT PRIMARY KEY,
            subscriber_count INTEGER NOT NULL
        )
    ''')

    conn.commit()
    conn.close()


if __name__ == '__main__':
    sys.exit(main())