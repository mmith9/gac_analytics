#!/usr/bin/env python

import time
import logging
import logging.config
import platform
#from pprint import PrettyPrinter

from constant_data import VERSION
from selenium_scraper import SwgohGgScraper
#from api_scraper import SwgohGgApi
import db_objects


if platform.system() == 'Linux':
    from pyvirtualdisplay import Display
    display = Display(visible=0, size=(800, 600))
    display.start()



logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


def main():

    logger.debug('main proc started')

    logger.debug('connecting to sqlite')
    local_db = db_objects.LocalDb()
    local_db.connect('gac_temp_db.sqlite')

    try:
        query = 'select min(gac_num), max(gac_num) from local_job_scan_battles'
        local_db.cursor.execute(query)
        rows = local_db.cursor.fetchall()
        gac_num = rows[0][0]
        assert gac_num==rows[0][1]
        query = 'select count(*) from local_job_scan_battles'
        local_db.cursor.execute(query)
        rows = local_db.cursor.fetchall()
        jobs_number = rows[0][0]
    except local_db.connection.DatabaseError as err:
        gac_num = 'undefined'
        jobs_number = 'undefined'
        logger.error('%s', err)


    print(f'Defined now: Gac number {gac_num}')
    print('0 init // clear local db')
    print('1 copy jobs from main db to local')
    print(f'2 execute copied jobs, {jobs_number} on queue')
    print('3 pump local data to main db')

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
    if task_number == '3':
        my_db = db_objects.MyDb()
        my_db.connect()
        local_db.pump_data_to_main_db(my_db)

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
