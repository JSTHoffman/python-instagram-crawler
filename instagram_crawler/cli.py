from __future__ import print_function

import datetime as dt
import traceback
import random
import signal
import json
import time
import os

import dateutil.parser as parser
from selenium import webdriver
import pandas as pd
import click

import post_crawler


# OPTIONS
@click.command()
@click.option(
    '--usernames',
    '-u',
    multiple=True,
    help='Username of the account to crawl.'
)
@click.option(
    '--procs',
    '-p',
    nargs=1,
    default=5,
    help='Number of processes to run while crawling a profile.'
)


def main(usernames, procs):
    '''Crawl public Instagram profiles to collect post data.'''

    # GET USER INPUT FOR ARGUMENTS
    args = user_input(usernames=usernames)

    # CREATE DATAFRAME TO HOLD OUTPUT
    final = pd.DataFrame()

    # LOAD COLUMN MAP
    home_directory = os.path.expanduser('~')
    map_path = '{0}/apps/cli_tools/python-instagram-crawler/' \
               'instagram_crawler/column_map.json'
    map_path = map_path.format(home_directory)

    with open(map_path) as json_file:
        column_map = json.load(json_file)
    json_file.close()

    # LOAD USERNAMES
    if 'input_file' in args:
        usernames = get_accounts(
            path=args['input_file'],
            column=args['column_name']
        )
        input_dir = args['input_file'].rfind('/')
        out_path = args['input_file'][:input_dir]
    else:
        usernames = args['accounts']
        out_path = '{0}/apps/cli_tools/python-instagram-crawler' \
                   '/output'.format(home_directory)

    # ADD PHANTOMJS EXECUTABLE TO PATH
    if 'phantomjs' not in os.environ['PATH']:
        print('adding phantomjs executable to PATH...')
        os.environ['PATH'] += ':{0}/bin'.format(os.getcwd())

    for username in usernames:
        try:
            # CREATE PHANTOMJS WEBDRIVER
            driver = get_driver()

            # RANDOM WAIT UP TO 1 SECOND
            random.seed(post_crawler.unix_timestamp())
            time.sleep(random.uniform(0.2, 1))

            # START CRAWLER FOR PROFILE
            posts = post_crawler.crawl(
                driver=driver,
                username=username,
                start_date=args['start_date'],
                end_date=args['end_date'],
                column_map=column_map,
                procs=procs
            )

            # CLOSE DRIVER
            driver.service.process.send_signal(signal.SIGTERM)
            driver.quit()

            # CONVERT DICT TO PANDAS DF AND APPEND TO FINAL
            transformed_df = pd.DataFrame.from_dict(posts)
            final = final.append(transformed_df)

        except (Exception, KeyboardInterrupt) as e:
            # HANDLE EXCEPTION
            handle_exception(
                error=e,
                username=username,
                data=final,
                path=out_path,
                args=args,
                driver=driver,
                home_dir=home_directory
            )

    # SAVE OUTPUT FILE
    save_results(
        path=out_path,
        data=final,
        args=args
    )


def user_input(usernames):
    inputs = {}

    # IF USERNAME ARGUMENT IS GIVEN SET ACCOUNT INPUT
    if usernames:
        inputs['accounts'] = usernames
    else:
        # GET FILE WITH ACCOUNT NAMES
        found_file = False
        while not found_file:
            # GET ACCOUNTS FILE PATH
            inputs['input_file'] = click.prompt(
                'Where is your accounts file located? >> ',
                type=str
            )

            # CHECK THAT FILE EXISTS
            if os.path.exists(inputs['input_file']):
                found_file = True
            else:
                print('Sorry, that file couldn\'t be found.\n')

        # GET COLUMN NAME WITH USERNAMES
        inputs['column_name'] = click.prompt(
            'Which column in your file holds the usernames? >> ',
            type=str
        )

    # GET STARTING DATE
    start_date = click.prompt(
        'When start date would you like to use? >> ',
        type=str
    )
    inputs['start_date'] = parser.parse(start_date)

    # GET ENDING DATE
    end_date = click.prompt(
        'What end date would you like to use? >> ',
        type=str
    )
    inputs['end_date'] = parser.parse(end_date)

    # GET OUPUT FILE NAME
    inputs['out_file'] = click.prompt(
        'What would you like to call the output file? >> ',
        type=str
    )
    return inputs


def get_driver():
    '''creates a new webdriver instance to check post dates'''
    # GET RANDOM USER AGENT STRING
    user_agent = post_crawler.UA.random

    # SET PHANTOMJS USER AGENT
    webdriver.DesiredCapabilities \
        .PHANTOMJS['phantomjs.page.customHeaders.User-Agent'] = user_agent
    webdriver.DesiredCapabilities \
        .PHANTOMJS['phantomjs.page.settings.userAgent'] = user_agent

    # CREATE WEBDRIVER
    # print('creating webdriver...')
    cookie_path = '{0}/apps/cli_tools/python-instagram-crawler/cookies.txt'
    driver = webdriver.PhantomJS(
        service_log_path=os.path.devnull,
        service_args=[
            '--ignore-ssl-errors=true',
            '--ssl-protocol=any',
            '--cookies-file={0}'.format(cookie_path)
        ]
    )
    return driver


def save_results(path, data, args):
    # GENERATE FILE PATH AND WRITE OUTPUT
    # FILE TO SAME DIRECTORY AS INPUT FILE
    # (USE CRAWLER OUTPUT DIRECTORY IF NO INPUT FILE IS USED)
    file_name = (args['out_file'][:args['out_file'].rfind('.csv')]
                 if '.csv' in args['out_file']
                 else args['out_file'])

    out_file = '{0}/{1}{2}'.format(
        path,
        file_name,
        '.csv'
    )
    data.to_csv(out_file, index=False)
    print('\ndone!\noutput file: {0}'.format(out_file))


def get_accounts(path, column):
    print('\nloading accounts...')
    accounts = pd.read_csv(path)
    accounts = accounts[column].tolist()
    return accounts


def handle_exception(error, username, data, path, args, driver, home_dir):
    error_name = type(error).__name__
    print('error crawling {0}\'s profile: {1}: {2}'
          .format(username, error_name, error.message))

    # SAVE SCREENSHOT
    # save_screenshot(
    #     error_name=error_name,
    #     username=username,
    #     driver=driver,
    #     home_dir=home_dir
    # )

    # SHOW TRACEBACK IF USER CHOOSES
    if click.confirm('would you like to see the stack trace?'):
        print(traceback.format_exc(error))

    # CONTINUE IF USER CHOOSES, OTHERWISE CLOSE DRIVER
    if click.confirm('do you want to continue?'):
        pass
    else:
        # HANDLE SAVING DATA
        handle_save(path=path, data=data, args=args)
        # CLOSE DRIVER BEFORE EXITING
        driver.quit()
        exit()


def save_screenshot(error_name, username, driver, home_dir):
    timestamp = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    file_name = '{0}_error_{1}_{2}.png'.format(username, error_name, timestamp)
    error_dir = '{0}/apps/cli_tools/python-instagram-crawler/errors/'
    error_path = error_dir.format(home_dir) + file_name
    driver.save_screenshot(error_path)
    print('screenshot saved to {0}'.format(error_path))


def handle_save(path, data, args):
    if click.confirm('would you like to save your results?'):
        save_results(
            path=path,
            data=data,
            args=args
        )
