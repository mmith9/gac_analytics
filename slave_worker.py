#!/usr/bin/env python

import time
import logging
import logging.config
#from pprint import PrettyPrinter

#from pyvirtualdisplay import Display
#display = Display(visible=0, size=(800, 600))
#display.start()

from constant_data import VERSION
from selenium_scraper import SwgohGgScraper
#from api_scraper import SwgohGgApi
import db_objects

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


def main():

    logger.debug('main proc started')
    gac_num = 118

    logger.debug('connecting to sqlite')
    local_db = db_objects.LocalDb()
    local_db.connect('gac_temp_db.sqlite')

    print(f'Defined now: Gac number {gac_num}')
    print('0 init // clear local db')
    print('1 copy jobs from main db to local')
    print('2 execute copied')

    task_number = input('Task number?')

    if task_number == '0':
        local_db.initialize_db()

    if task_number == '1':
        my_db = db_objects.MyDb()
        my_db.connect()
        local_db.copy_scrape_jobs(my_db)

    if task_number == '2':
        selenium_scraper = SwgohGgScraper(local_db, gac_num)
        selenium_scraper.scrape_battles_slave()


if __name__ == "__main__":

    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='slave worker for selenium on raspberry')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    start = time.time()
    main()
    end = time.time()
    total_time = end - start
    print("\nExecution time: " + str(total_time))
