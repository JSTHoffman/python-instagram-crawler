from __future__ import print_function

from multiprocessing import Process
from multiprocessing import Manager
import datetime as dt
import traceback
import signal
import time
import json
import re
import os

from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
from bs4 import BeautifulSoup
import requests


class CheckRowCount(object):
    '''defines the webdriver wait condition: that the new
    row count must be greater than the old row count'''

    def __init__(self, locator, row_count):
        self.locator = locator
        self.row_count = row_count

    def __call__(self, driver):
        new_rows = driver.find_elements(*self.locator)
        new_row_count = len(new_rows)
        return bool(new_row_count > self.row_count)


def crawl(driver, username, start_date, end_date, column_map, procs):
    print('\ncrawling {0}\'s profile'.format(username))

    # CHECK PROFILE INFO
    profile_info = check_profile(username, driver)

    # COLLECT POST URLs
    post_urls = get_post_urls(
        driver=driver,
        start_date=start_date,
        shared_data=profile_info
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

    print('\npulled {0} posts for {1}!'
          .format(len(transformed_posts), username))
    return transformed_posts


def get_post_urls(driver, start_date, shared_data):
    print('retrieving post URLs...')

    # GET POST COUNT FROM PROFILE INFO
    post_count = shared_data['entry_data']['ProfilePage'][0]['user']['media']['count']

    # CLICK LOAD MORE BUTTON IF > 12 POSTS
    if post_count > 12:
        driver.implicitly_wait(1)

        # TRY TO FIND LOAD MORE BUTTON
        # (WILL NOT BE PRESENT FOR NEWER ACCOUNTS)
        try:
            load_more = driver.find_element_by_xpath(
                '//a[contains(text(), "Load more")]'
            )
            load_more.click()

        # CONTINUE IF LOAD MORE BUTTON IS NOT FOUND
        except NoSuchElementException:
            pass

    # GET POST URLS
    post_urls = []
    found_last_post = False
    while not found_last_post:
        post_rows = driver.find_elements_by_class_name('_70iju')
        row_count = len(post_rows)

        # IF NUMBER OF POSTS (ROWS x 3) IS >= POST COUNT
        # THEN ALL POSTS ARE DISPLAYED EVEN IF THE START
        # DATE HASN'T BEEN REACHED
        if row_count * 3 >= post_count:
            found_last_post = True
            break

        # GET SHARED DATA OBJECT WITH POST INFO
        # FOR THE LAST POST ON THE PAGE
        last_row = post_rows[-1].find_elements_by_tag_name('a')
        last_url = last_row[-1].get_attribute('href').encode('utf-8')
        last_post = check_post_date(last_url)

        # GRAB POST DATE AND CHECK TO SEE IF MORE IMAGES NEED TO BE LOADED
        post_date = dt.datetime.fromtimestamp(
            last_post['entry_data']['PostPage'][0]['graphql']['shortcode_media']['taken_at_timestamp']
        )
        print('last post date: {0}'.format(post_date.date()))
        if post_date.date() < start_date.date():
            found_last_post = True
            break

        # SCROLL TO LOAD MORE PHOTOS
        print('loading more posts...')
        scroll(driver, 1)
        try:
            WebDriverWait(driver, 10).until(CheckRowCount((By.CLASS_NAME, '_70iju'), row_count))
        except TimeoutException:
            raise TimeoutException('hung loading more posts')

    # COLLECT ALL POST URLs ON THE PAGE
    for row in post_rows:
        posts = row.find_elements_by_tag_name('a')
        for post in posts:
            url = post.get_attribute('href').strip()
            print('collecting url: {0}...'.format(url), end='\r')
            post_urls.append(url)
    return post_urls


def check_post_date(post_url):
    # CREATE NEW DRIVER TO LOAD POST PAGE
    new_driver = get_driver()
    new_driver.get(post_url)

    # GET THE SHARED DATA OBJECT WITH POST INFO
    shared_data = new_driver.execute_script(
        'return window._sharedData;'
    )

    # END PHANTOMJS PROCESS AND CLOSE NEW DRIVER
    new_driver.service.process.send_signal(signal.SIGTERM)
    time.sleep(2)
    new_driver.quit()
    return shared_data


def chunk_transform(post_urls, start_date, end_date, column_map, num_processes):
    # MULTIPROCESSING LIST OBJECT FOR COLLECTING
    # OUTPUT FROM MULTIPLE CONCURRENT PROCESSES
    transformed_posts = Manager().list()

    # CALCULATE THE NUMBER OF URLs EACH PROCESS WILL CRAWL
    chunk_size = get_chunk_size(len(post_urls), num_processes)

    # CREATE GROUPS OF URLs FOR EACH CRAWLER PROCESS
    chunks = []
    for i in xrange(0, len(post_urls), chunk_size):
        chunks.append(post_urls[i:i + chunk_size])

    print('\ncollecting post data ({0} concurrent processes)...'
          .format(num_processes))

    # RUN TRANSFORM FUNCTION IN SEPARATE
    # PROCESS FOR EACH GOUP OF POST URLs
    jobs = []
    for chunk in chunks:
        process = Process(
            target=transform_posts,
            args=(
                chunk,
                transformed_posts,
                start_date,
                end_date,
                column_map
            )
        )
        # ADD PROCESS TO JOBS LIST
        # AND START PROCESS
        jobs.append(process)
        process.start()

    # WAIT UNTIL ALL PROCESSES IN JOBS
    # LIST HAVE FINISHED TO CONTINUE
    for job in jobs: job.join()
    return transformed_posts


def transform_posts(post_urls, array, start_date, end_date, column_map):
    # GET TODAY'S DATE TO CALCULATE POST LIFETIME
    today = dt.datetime.now()
    for url in post_urls:
        try:
            print('scraping {0}...'.format(url), end='\r')

            # PARSE HTML TO GET SHARED DATA OBJECT
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            shared_data = soup.find('script', text=re.compile('window._sharedData')).text
            raw_post = json.loads(shared_data[shared_data.find('{'):shared_data.rfind('}') + 1])

            # POST INFO LOCATED IN THE MEDIA OBJECT IN SHARED DATA
            raw_post = raw_post['entry_data']['PostPage'][0]['graphql']['shortcode_media']
            post_date = dt.datetime.fromtimestamp(raw_post['taken_at_timestamp'])

            # TRANSFORM DATA IF POST DATE WITHIN RANGE
            if post_date.date() >= start_date.date() and post_date.date() <= end_date.date():
                # CREATE EMPTY POST OBJECT AND PARSE VALUES FROM RAW DATA
                transformed_post = dict((key, None) for key in column_map)
                transformed_post['channel'] = 'instagram'
                transformed_post['post_id'] = raw_post['shortcode']
                transformed_post['likes'] = raw_post['edge_media_preview_like']['count']
                transformed_post['comments'] = raw_post['edge_media_to_comment']['count']
                transformed_post['username'] = raw_post['owner']['username']
                transformed_post['image'] = raw_post['display_url']
                transformed_post['url'] = url
                transformed_post['publish_date'] = post_date.strftime('%Y-%m-%d %H:%M:%S')
                transformed_post['is_ad'] = raw_post['is_ad']
                transformed_post['is_video'] = raw_post['is_video']
                transformed_post['post_lifetime'] = (today.date() - post_date.date()).days

                # USERTAGS, CAPTION, LOCATION, AND VIDEO VIEWS
                # ARE NOT ALWAYS PRESENT IN THE RAW DATA
                if 'edge_media_to_tagged_user' in raw_post:
                    tags = []
                    for item in raw_post['edge_media_to_tagged_user']['edges']:
                        tags.append(item['node']['user']['username'])
                    transformed_post['user_tags'] = ', '.join(tags)

                if len(raw_post['edge_media_to_caption']['edges']):
                    caption = raw_post['edge_media_to_caption']['edges'][0]['node']['text']
                    caption = caption.encode('ascii', 'ignore')
                    transformed_post['caption'] = caption.strip()

                if raw_post['location']:
                    location = raw_post['location']['name']
                    transformed_post['location'] = location.encode('utf-8')

                if transformed_post['is_video']:
                    if 'video_view_count' in raw_post:
                        transformed_post['video_views'] = raw_post['video_view_count']
                array.append(fill_none(transformed_post))

        except Exception:
            print('Error retrieving post data for post: {0}\n{1}'
                  .format(url, traceback.format_exc()))


def fill_none(transformed_post):
    # IN CASE FIELD IN RAW DATA CONTAINS EMPTY STRING
    for field in transformed_post:
        if transformed_post[field] == '':
            transformed_post[field] = None
    return transformed_post


def scroll(driver, count):
    # SCROLL PAGE TO LOAD MORE PHOTOS
    for i in range(count):
        # SCROLL TO BOTTOM OF PAGE
        driver.execute_script(
            'window.scrollTo(0, document.body.scrollHeight);'
        )
    return driver


def get_chunk_size(post_num, num_processes):
    # DIVIDE NUMBER OF POSTS BY NUMBER OF PROCESSES
    chunk_size = int(round(float(post_num) / num_processes))
    if chunk_size < 1:
        return 1
    return chunk_size


def get_driver():
    # CREATE NEW DRIVER TO CHECK POST DATES
    driver = webdriver.PhantomJS(
        service_log_path=os.path.devnull,
        service_args=[
            '--ignore-ssl-errors=true',
            '--ssl-protocol=any',
            '--cookies-file=/cookies.txt'
        ]
    )
    return driver


def check_profile(username, driver):
    # PARSE HTML TO GET SHARED DATA OBJECT (CONTAINS PROFILE INFO)
    driver.get('https://www.instagram.com/{0}'.format(username))
    shared_data = driver.execute_script(
        'return window._sharedData;'
    )
    # CHECK FOR PRIVATE PROFILE
    check_private_profile(shared_data)
    return shared_data


def check_private_profile(data):
    # CHECK PRIVATE PROFILE FLAG IN SHARED DATA OBJECT
    if data['entry_data']['ProfilePage'][0]['user']['is_private'] == True:
        raise Exception('PrivateProfileError')
