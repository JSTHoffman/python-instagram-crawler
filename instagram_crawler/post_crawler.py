from __future__ import print_function

from multiprocessing import Process
from multiprocessing import Manager
import datetime as dt
import traceback
import random
import json
import time
import sys
import re
import os

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import requests

# PATTERN TO STRIP IMAGE URL SIGNATURES
URL_PATTERN = re.compile(r'vp.*\/.{32}\/.{8}\/')

# EPOCH DATE FOR CREATING UNIX TIMESTAMPS
EPOCH = dt.datetime.utcfromtimestamp(0)

# USER AGENT INSTANCE
# FOR GENERATING RANDOM
# USER AGENT STRINGS
UA = UserAgent()


class CheckLastPost(object):
    '''defines the webdriver wait condition:
    the last post on the page must have a new url'''

    def __init__(self, last_url):
        self.last_url = last_url

    def __call__(self, driver):
        post_divs = driver.find_elements_by_css_selector('div._mck9w._gvoze._tn0ps')
        new_last_post = post_divs[-1].find_element_by_tag_name('a')
        new_last_url = new_last_post.get_attribute('href').encode('utf-8')
        return bool(new_last_url != self.last_url)


def crawl(driver, username, start_date, end_date, column_map, procs):
    '''handler function for crawling an instagram profile'''
    print('\ncrawling {0}\'s profile'.format(username))

    # SEED RANDOM NUMBER GENERATOR
    # BEFORE CRAWLING EACH ACCOUNT
    random.seed(unix_timestamp())
    time.sleep(random.uniform(0.5, 3))

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
    '''collects URLs for posts on the profile page
    with post dates later than start_date'''
    print('retrieving post URLs...')

    # GET POST COUNT FROM PROFILE INFO
    post_count = shared_data['entry_data']['ProfilePage'][0]['user']['media']['count']

    # GET POST URLS
    post_urls = list()
    found_last_post = False
    while not found_last_post:
        post_divs = driver.find_elements_by_css_selector(
            'div._mck9w._gvoze._tn0ps'
        )

        # ADD POST URLs TO LIST
        for div in post_divs:
            post = div.find_element_by_tag_name('a')
            url = post.get_attribute('href').encode('utf-8')
            if url not in post_urls:
                post_urls.append(url)

        # IF NUMBER OF POSTS IS >= POST COUNT THEN ALL POSTS ARE
        # DISPLAYED EVEN IF THE START DATE HASN'T BEEN REACHED
        if len(post_urls) >= post_count:
            found_last_post = True
            break

        # GET SHARED DATA OBJECT WITH POST INFO
        # FOR THE LAST POST ON THE PAGE
        last_url = post_urls[-1]
        last_post = get_post(last_url)

        # GRAB POST DATE AND CHECK TO SEE IF MORE IMAGES NEED TO BE LOADED
        post_info = last_post['entry_data']['PostPage'][0]['graphql']
        post_date = dt.datetime.fromtimestamp(
            post_info['shortcode_media']['taken_at_timestamp']
        )
        print('last post date: {0}'.format(post_date.date()), end='\r')
        sys.stdout.flush()
        if post_date.date() < start_date.date():
            found_last_post = True
            break

        # SCROLL TO LOAD MORE PHOTOS
        sys.stdout.flush()
        scroll(driver, 1)
        try:
            WebDriverWait(driver, 30).until(CheckLastPost(last_url))
        except TimeoutException:
            raise TimeoutException('hung loading more posts')
    return post_urls


def chunk_transform(post_urls, start_date, end_date, column_map, num_processes):
    '''splits post URLs into chunks to be processed in parallel'''

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
    '''gets the sharedData object from a post page using get_post()
    and transforms the raw data, appending it to the
    multiprocessing manager list'''

    # GET TODAY'S DATE TO CALCULATE POST LIFETIME
    today = dt.datetime.now()
    for url in post_urls:
        try:
            print('scraping {0}...'.format(url), end='\r')
            sys.stdout.flush()

            # GET SHARED DATA OBJECT FOR POST
            shared_data = get_post(url)

            # POST INFO LOCATED IN THE MEDIA OBJECT IN SHARED DATA
            raw_post = shared_data['entry_data']['PostPage'][0]['graphql']['shortcode_media']
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
                transformed_post['image'] = URL_PATTERN.sub('', raw_post['display_url'])
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


def get_post(post_url):
    '''loads a post page and gets the
    sharedData object with post info'''
    retries = 0
    while retries < 5:
        try:
            # RANDOM WAIT UP TO 1 SECOND
            time.sleep(random.uniform(0.2, 1))

            # SET RANDOM USER AGENT HEADER
            headers = {'User-Agent': UA.random}

            # SEND REQUEST
            response = requests.get(post_url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            script = soup.find('script', text=re.compile('window._sharedData')).text
            shared_data = json.loads(re.search(r'{.*}', script).group(0))
            return shared_data

        except Exception as e:
            retries += 1
            wait = random.randint(10, 30)
            print('error loading post: {0}: {1}'.format(post_url, e))
            print('retrying in {0} seconds...'.format(wait))
            time.sleep(wait)
    raise Exception('retires exceeded loading post')


def fill_none(transformed_post):
    '''in case fields in the raw data contain empty strings'''
    for field in transformed_post:
        if transformed_post[field] == '':
            transformed_post[field] = None
    return transformed_post


def scroll(driver, count):
    '''scrolls to bottom of page to
    trigger ajax request for more photos'''
    for i in range(count):
        # RANDOM WAIT UP TO 1 SECOND
        time.sleep(random.uniform(0.2, 1))

        # SCROLL TO BOTTOM OF PAGE
        driver.execute_script(
            'window.scrollTo(0, document.body.scrollHeight);'
        )
        # RANDOM WAIT UP TO 1/2 SECOND
        time.sleep(random.uniform(0.2, 0.5))

        # SCROLL UP A BIT
        driver.execute_script(
            'window.scrollTo(0, document.body.scrollHeight - 1000);'
        )
        # RANDOM WAIT UP TO 1 SECOND
        time.sleep(random.uniform(0.2, 1))
    return driver


def get_chunk_size(post_num, num_processes):
    '''determines the size of a chunk based on the number
    of posts retrieved and the number of process used'''
    chunk_size = int(round(float(post_num) / num_processes))
    if chunk_size < 1:
        return 1
    return chunk_size


def check_profile(username, driver):
    '''gets the sharedData object from a
    profile page and checks the is_private flag'''

    # LOAD PROFILE PAGE AND GET SHARED DATA OBJECT
    driver.get('https://www.instagram.com/{0}'.format(username))
    shared_data = driver.execute_script(
        'return window._sharedData;'
    )
    # RANDOM WAIT UP TO 1 SECOND
    time.sleep(random.uniform(0.2, 1))

    # CHECK FOR PRIVATE PROFILE
    if shared_data['entry_data']['ProfilePage'][0]['user']['is_private'] == True:
        raise Exception('PrivateProfileError')
    return shared_data


def unix_timestamp():
    '''get current time as unix timestamp'''
    return (dt.datetime.now() - EPOCH).total_seconds() * 1000.0
