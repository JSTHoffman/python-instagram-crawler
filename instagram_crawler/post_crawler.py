from __future__ import print_function

from multiprocessing import Process
from multiprocessing import Manager
import datetime as dt
import traceback
import time
import json
import re

from bs4 import BeautifulSoup
import requests


def crawl(driver, username, start_date, end_date, column_map, procs):

    # COLLECT POST URLs
    post_urls = get_post_urls(
        driver=driver,
        username=username,
        start_date=start_date
    )

    # CRAWL POST PAGES AND TRANSFORM DATA
    transformed_posts = list(
        chunk_transform(
            post_urls,
            start_date,
            end_date,
            column_map,
            procs
        )
    )
    return transformed_posts


def get_post_urls(driver, username, start_date):
    post_urls = []
    found_last_post = False
    print('retrieving post URLs...')
    driver.get('https://www.instagram.com/{0}'.format(username))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    sharedData = soup.find('script', text=re.compile('window._sharedData')).text
    json_obj = json.loads(sharedData[sharedData.find('{'):sharedData.rfind('}') + 1])

    # CHECK FOR PRIVATE PROFILE
    check_private_profile(json_obj)

    post_count = json_obj['entry_data']['ProfilePage'][0]['user']['media']['count']
    driver, found_last_post, post_rows = check_post_date(driver, found_last_post, start_date, post_count)
    if post_count > 12:
        driver.implicitly_wait(1)
        load_more = driver.find_element_by_xpath('//a[contains(text(), "Load more")]')
        load_more.click()
        while not found_last_post:
            scroll(driver, 1)
            driver, found_last_post, post_rows = check_post_date(driver, found_last_post, start_date, post_count)
    for row in post_rows:
        posts = row.find_elements_by_tag_name('a')
        for post in posts:
            url = post.get_attribute('href')
            print('collecting url: {0}...'.format(url), end='\r')
            post_urls.append(url)
    return post_urls


def check_post_date(driver, flag, start_date, post_count):
    soup = BeautifulSoup(driver.page_source.encode('utf-8'), 'html.parser')
    post_rows = driver.find_elements_by_class_name('_myci9')
    if len(post_rows) * 3 >= post_count:
        flag = True
    last_row = post_rows[-1].find_elements_by_tag_name('a')
    last_post = last_row[-1].get_attribute('href')
    response = requests.get(last_post)
    soup = BeautifulSoup(response.content, 'html.parser')
    sharedData = soup.find('script', text=re.compile('window._sharedData')).text
    raw_post = json.loads(sharedData[sharedData.find('{'):sharedData.rfind('}') + 1])
    post_date = dt.datetime.fromtimestamp(raw_post['entry_data']['PostPage'][0]['media']['date'])
    if post_date.date() < start_date.date():
        flag = True
    return driver, flag, post_rows


def chunk_transform(post_urls, start_date, end_date, column_map, num_processes):
    jobs = []
    today = dt.datetime.now()
    transformed_posts = Manager().list()
    chunk_size = get_chunk_size(len(post_urls), num_processes)
    chunks = [post_urls[i:i + chunk_size] for i in xrange(0, len(post_urls), chunk_size)]
    print('\ncollecting post data ({0} concurrent processes)...'.format(num_processes))
    for chunk in chunks:
        process = Process(target=transform_posts, args=(chunk, transformed_posts, start_date, end_date, today, column_map))
        jobs.append(process)
        process.start()
    for job in jobs: job.join()
    return transformed_posts


def transform_posts(post_urls, array, start_date, end_date, today, column_map):
    for url in post_urls:
        try:
            print('scraping {0}...'.format(url), end='\r')
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            sharedData = soup.find('script', text=re.compile('window._sharedData')).text
            raw_post = json.loads(sharedData[sharedData.find('{'):sharedData.rfind('}') + 1])
            raw_post = raw_post['entry_data']['PostPage'][0]['media']
            post_date = dt.datetime.fromtimestamp(raw_post['date'])

            if post_date.date() >= start_date.date() and post_date.date() <= end_date.date():
                transformed_post = dict((key, None) for key in column_map)
                transformed_post['channel'] = 'instagram'
                transformed_post['post_id'] = raw_post['code']
                transformed_post['likes'] = raw_post['likes']['count']
                transformed_post['comments'] = raw_post['comments']['count']
                transformed_post['username'] = raw_post['owner']['username']
                transformed_post['image'] = raw_post['display_src']
                transformed_post['url'] = url
                transformed_post['publish_date'] = post_date.strftime('%Y-%m-%d %H:%M:%S')
                transformed_post['is_ad'] = raw_post['is_ad']
                transformed_post['is_video'] = raw_post['is_video']
                transformed_post['post_lifetime'] = (today.date() - post_date.date()).days

                if 'usertags' in raw_post:
                    tags = [item['user']['username'] for item in raw_post['usertags']['nodes']]
                    transformed_post['user_tags'] = ', '.join(tags)

                if 'caption' in raw_post:
                    transformed_post['caption'] = raw_post['caption'].encode('ascii', 'ignore').strip(' ')

                if raw_post['location'] != None:
                    transformed_post['location'] = raw_post['location']['name'].encode('utf-8')

                if transformed_post['is_video'] == True:
                    if 'video_views' in raw_post:
                        transformed_post['video_views'] = raw_post['video_views']
                array.append(fill_none(transformed_post))

        except Exception:
            print(
                'Error retrieving post data for post: {0}\n{1}'
                .format(url, traceback.format_exc())
            )


def fill_none(transformed_post):
    for field in transformed_post:
        if transformed_post[field] == '':
            transformed_post[field] = None
    return transformed_post


def scroll(driver, count):
    # SCROLL PAGE TO LOAD MORE PHOTOS
    for i in range(count):
        driver.execute_script(
            'window.scrollTo(0, document.body.scrollHeight);'
        )
        time.sleep(0.2)
        driver.execute_script(
            'window.scrollTo(0, 0);'
        )
        time.sleep(0.2)
    return driver


def get_chunk_size(post_num, num_processes):
    chunk_size = int(round(float(post_num) / num_processes))
    if chunk_size < 1:
        return chunk_size + 1
    return chunk_size


def check_private_profile(data):
    if data['entry_data']['ProfilePage'][0]['user']['is_private'] == True:
        raise Exception('PrivateProfileError')
