#!/usr/bin/env python -u

import time
import logging
import logging.config
from pprint import PrettyPrinter

from constant_data import VERSION
from selenium_scraper import SwgohGgScraper
from api_scraper import SwgohGgApi
import db_objects

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

def main():
    # print('method main')
    # print('got logger')
    # print(logger)
    # print('testing logger')
    # print('logging crit')
    # logger.critical('msg')
    # print('logging error')
    # logger.error('msg')
    # print('logging warning')
    # logger.warning('msg')
    # print('logging info')
    # logger.info('msg')
    # print('logging debug')
    # logger.debug('msg')
    # print('done testing logger')


    logger.debug('main proc started')
    gac_num = 119

    logger.debug('connecting to db')
    my_db = db_objects.MyDb()
    my_db.connect()
    logger.debug('fetching info from db')

    print(f'Defined now: Gac number {gac_num}')

    pprint = PrettyPrinter()
    info = my_db.get_info()
    pprint.pprint(info)

    print('1 prepare jobs: scan leaderboards')
    print('2 execute')
    print()
    print('3 prepare jobs: scan units')
    print('4 execute')
    print()
    print('5 prepare jobs: scan player battles')
    print('6 execute')

    print('0 : update gac events table')

    task_number = input('Task number?')

    if task_number == '1':
        selenium_scraper = SwgohGgScraper(my_db, gac_num)
        selenium_scraper.prepare_jobs_scan_leaderboards()

    if task_number == '2':
        selenium_scraper = SwgohGgScraper(my_db, gac_num)
        selenium_scraper.scrape_leaderboards()

    if task_number == '3':
        api_scraper = SwgohGgApi(my_db, gac_num)
        api_scraper.prepare_jobs_scan_units()

    if task_number == '4':
        api_scraper = SwgohGgApi(my_db, gac_num)
        api_scraper.scrape_players()

    if task_number == '5':
        selenium_scraper = SwgohGgScraper(my_db, gac_num)
        selenium_scraper.prepare_jobs_scan_battles()

    if task_number == '6':
        selenium_scraper = SwgohGgScraper(my_db, gac_num)
        selenium_scraper.scrape_battles()

    if task_number == '0':
        print('foo')
        api_scraper = SwgohGgApi(my_db, gac_num)
        api_scraper.scrape_gac_events()


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='Unknown/incomplete info hash resolver')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    start = time.time()
    main()
    end = time.time()
    total_time = end - start
    print("\nExecution time: " + str(total_time))
