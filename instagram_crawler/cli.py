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
    '--procs',
    '-p',
    nargs=1,
    default=5,
    help='Number of processes to run while crawling a profile.'
)


def main(procs):
    """Crawl public Instagram profiles to collect post data."""

    # GET USER INPUT FOR ARGUMENTS
    args = user_input()

    # CREATE DATAFRAME TO HOLD OUTPUT
    final = pd.DataFrame()

    # LOAD USERNAMES
    usernames = get_accounts(
        args['input_file'],
        args['column_name']
    )

    # LOAD COLUMN MAP
    with open('instagram_crawler/column_map.json') as json_file:
        column_map = json.load(json_file)
    json_file.close()

    # ADD PHANTOMJS EXECUTABLE TO PATH
    if 'phantomjs' not in os.environ['PATH']:
        print('adding phantomjs executable to PATH...')
        os.environ['PATH'] += ':{0}/bin'.format(os.getcwd())

    # CREATE PHANTOMJS WEBDRIVER
    driver = get_driver()

    for username in usernames:
        print(
            '\ncrawling {0}\'s account'
            .format(username)
        )

        try:
            posts = post_crawler.crawl(
                driver=driver,
                username=username,
                start_date=args['start_date'],
                end_date=args['end_date'],
                column_map=column_map,
                procs=procs
            )

            print(
                '\npulled {0} posts for {1}!'
                .format(len(posts), username)
            )

            transformed_df = pd.DataFrame.from_dict(posts)
            final = final.append(transformed_df)

        except Exception:
            print(traceback.format_exc())

    # WRITE OUT FINAL CSV FILE
    final.to_csv('output/{0}.csv'.format(args['out_file']), index=False)

    driver.service.process.send_signal(signal.SIGTERM)
    driver.quit()


def user_input():
    inputs = {}

    # GET ACCOUNTS FILE PATH
    inputs['input_file'] = click.prompt(
        'Where is your accounts file located? >> ',
        type=str
    )

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
            '--ssl-protocol=any'
        ]
    )
    return driver


def get_accounts(path, column):
    print('loading accounts...')
    accounts = pd.read_csv(path)
    accounts = accounts[column].tolist()
    return accounts
