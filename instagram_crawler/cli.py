from __future__ import print_function

import traceback
import signal
import json
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
    args = user_input(usernames)

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
            args['input_file'],
            args['column_name']
        )
        input_dir = args['input_file'].rfind('/')
        out_path = args['input_file'][:input_dir]
    else:
        usernames = args['accounts']
        out_path = '{0}/apps/cli_tools/python-instagram-crawler' \
                   '/output'.format(home_directory)

    # ADD PHANTOMJS EXECUTABLE TO PATH
    if 'phantomjs' not in os.environ['PATH']:
        print('\nadding phantomjs executable to PATH...')
        os.environ['PATH'] += ':{0}/bin'.format(os.getcwd())

    # CREATE PHANTOMJS WEBDRIVER
    driver = get_driver()

    for username in usernames:
        try:
            # START CRAWLER FOR PROFILE
            posts = post_crawler.crawl(
                driver=driver,
                username=username,
                start_date=args['start_date'],
                end_date=args['end_date'],
                column_map=column_map,
                procs=procs
            )

            # CONVERT DICT TO PANDAS DF AND APPEND TO FINAL
            transformed_df = pd.DataFrame.from_dict(posts)
            final = final.append(transformed_df)

        except Exception as e:
            # HANDLE EXCEPTION BY GIVING USER OPTION TO
            # SEE STACK TRACE AND CONTINUE OR NOT
            handle_exception(e, username)

    # GENERATE FILE PATH AND WRITE OUTPUT
    # FILE TO SAME DIRECTORY AS INPUT FILE
    out_file = '{0}/{1}{2}'.format(
        out_path,
        args['out_file'].strip('.csv'),
        '.csv'
    )
    final.to_csv(out_file, index=False)

    print('\ndone!\noutput file: {0}'.format(out_file))

    # END PHANTOMJS PROCESS AND CLOSE DRIVER
    driver.service.process.send_signal(signal.SIGTERM)
    driver.quit()


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
    print('creating webdriver...')
    driver = webdriver.PhantomJS(
        service_log_path=os.path.devnull,
        service_args=[
            '--ignore-ssl-errors=true',
            '--ssl-protocol=any',
            '--cookies-file=/cookies.txt'
        ]
    )
    return driver


def get_accounts(path, column):
    print('\nloading accounts...')
    accounts = pd.read_csv(path)
    accounts = accounts[column].tolist()
    return accounts


def handle_exception(error, username):
    print('Error crawling {0}\'s profile: {1}'
          .format(username, error.message))
    if click.confirm('would you like to see the stack trace?'):
        print(traceback.format_exc(error))
    click.confirm('Do you want to continue?', abort=True)
