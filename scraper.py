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
            print(f'Retry {retries} of {MAX_RETRIES} for {url} with error: {result["message"]}')
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

    cursor.execute('''update state set value = ? where name = 'last_status' ''', (status['message'],))
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
            cursor.execute('''
                insert into communities (name, subscriber_count, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(name) DO UPDATE 
                SET subscriber_count = excluded.subscriber_count, last_updated = CURRENT_TIMESTAMP
            ''', (name, subscriber_count))
        cursor.execute('''update state set value = value + 1 where name = 'page' ''')
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

    cursor.execute('''select value from state where name = 'retry' ''')
    retry = cursor.fetchone()[0]
    if retry == 'True':
        cursor.execute('''insert into value (name, state) values ('page', 1)''')
        cursor.execute('''insert into value (name, state) values ('retry', False)''')
        conn.commit()
        page_index = 1
    else:
        cursor.execute('''select value from state where name = 'last_status' ''')
        if cursor.fetchone()[0] == 'Scraping completed successfully':
            page_index = 0
        else:
            cursor.execute('''select value from state where name = 'page' ''')
            page_index = cursor.fetchone()[0]
    conn.close()
    return page_index


def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS state (
            name TEXT PRIMARY KEY,
            value NOT NULL
        )
    ''')
    data = [
        ('page', 1),
        ('retry', 'False'),
        ('last_status', 'Initialized Database')
    ]

    cursor.executemany('''INSERT INTO state (name, value) VALUES (?, ?)''', data)
    cursor.execute('''insert into state (name, value) values ('last_run', CURRENT_TIMESTAMP)''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS communities (
            name TEXT PRIMARY KEY,
            subscriber_count INTEGER NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()


if __name__ == '__main__':
    sys.exit(main())