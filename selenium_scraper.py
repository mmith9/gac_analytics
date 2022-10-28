import logging
import traceback
import sys
import re
import time
import sqlite3
import mysql.connector

from bs4 import BeautifulSoup

from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.remote.webdriver import WebDriver

import throttling
from constant_data import CHROME_USER
#from constant_data import DRIVER_PATH
from constant_data import DIVISIONS
from constant_data import LEAGUES

from gac_dictionaries import Dictionary
from gac_objects import GacRound, GacTeam

from db_objects import MyDb

logger = logging.getLogger(__name__)


class SwgohGgScraper:
    def __init__(self, my_db: MyDb, gac_num) -> None:
        self.initialize_chrome()
        self.cursor = my_db.cursor
        self.db_connection = my_db.connection
        self.jobs_to_scrape: list
        self.snapped_allycodes: list
        self.unit_dict = Dictionary(
            'unit_dict', self.cursor, self.db_connection)
        self.dc_dict = Dictionary(
            'dc_mechanics_dict',self.cursor, self.db_connection)
        self.gac_generate_num = gac_num
        self.rate_limiter: throttling.RateLimiter
        self.rate_counter: throttling.RateCounter
        self.current_gac_num=-1

    def initialize_chrome(self):
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--user-data-dir=c:\\users\\' + CHROME_USER)
        options.add_argument("--start-minimized")
        # options.add_argument("--headless")
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-gl-drawing-for-tests")
        options.add_argument("--disable-renderer-accessibility")
        

        self.driver = webdriver.Chrome(options=options)

        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )

    def remove_used_allycode(self, allycode) -> bool:
        query = 'delete from _job_scan_battles'
        query += ' where allycode = (%s) '
        try:
            self.cursor.execute(query, (int(allycode),))  # one element tuple
        except mysql.connector.Error:
            logger.error('failed to remove used allycode from job table')
            traceback.print_exc()
            return False
        return True

    def remove_used_allycode_slave(self, allycode) -> bool:
        query = 'delete from local_job_scan_battles'
        query += ' where allycode = ? '
        try:
            self.cursor.execute(query, (int(allycode),))  # one element tuple
        except sqlite3.DatabaseError:
            logger.error('failed to remove used allycode from job table')
            traceback.print_exc()
            return False

        query = 'insert into local_jobs_completed (allycode, gac_num) '
        query+= 'values (?, ?)'
        self.cursor.execute(query, (allycode, self.current_gac_num))
        return True

    def upload_round_to_db(self, a_round: GacRound) -> bool:
        attacker = int(a_round.attacker)
        defender = int(a_round.defender)
        for battle in a_round.battles:
            if battle.type == 'squad' and battle.attempt == 1:
                query = 'insert into battles '\
                        '(attacker, defender, banners, bt_date, duration, bt_gac_num) '\
                        'values (%s, %s, %s, %s, %s, %s)'
                try:
                    self.cursor.execute(query,
                                        (attacker, defender, battle.banners,
                                         battle.datetime, battle.duration, a_round.gac_num))
                    battle_id = self.cursor.lastrowid
                except mysql.connector.Error:
                    logger.error('failed to upload battle to db')
                    traceback.print_exc()
                    return False

                if not self.upload_team_to_db(battle.attacker_team, battle_id, 'attacker'):
                    logger.error('failed to upload attacker team to db')
                    return False

                if not self.upload_team_to_db(battle.defender_team, battle_id, 'defender'):
                    logger.error('failed to upload defender team to db')
                    return False
        return True

    def upload_round_to_db_slave(self, a_round: GacRound) -> bool:
        attacker = int(a_round.attacker)
        defender = int(a_round.defender)
        battles_uploaded = 0
        for battle in a_round.battles:
            if battle.type != 'squad' or battle.attempt!=1:
                continue
        
            if battle.attacker_team.datacron:
                attacker_dc_id=battle.attacker_team.datacron.\
                    save_yourself_to_db(self.cursor, self.dc_dict, 'sqlite')
            else:
                attacker_dc_id=False

            if battle.defender_team.datacron:
                defender_dc_id=battle.defender_team.datacron.\
                    save_yourself_to_db(self.cursor, self.dc_dict, 'sqlite')
            else:
                defender_dc_id=False

            an_insert = [attacker, defender, battle.banners,battle.datetime,\
                            battle.duration, a_round.gac_num]

            query = 'insert into local_battles '\
                    '(attacker, defender, banners, bt_date, duration, bt_gac_num, '

            if attacker_dc_id:
                query+= 'attacker_dc_id, '
                an_insert.append(attacker_dc_id)
            if defender_dc_id:
                query+= 'defender_dc_id, '
                an_insert.append(defender_dc_id)
            query+='d1,d2,d3,d4,d5,a1,a2,a3,a4,a5'
            query+=' ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?'
            if attacker_dc_id:
                query+= ', ?'
            if defender_dc_id:
                query+= ', ?'
            query+=')'

            units = [0 for _i in range(0,10)]

            units[0]=self.unit_dict.to_int(battle.defender_team.members[0].base_id)
            members:GacTeam = sorted(battle.defender_team.members[1:],\
                        key=lambda unit: self.unit_dict.to_int(unit.base_id))
            i=1
            for unit in members:
                unit_id = self.unit_dict.to_int(unit.base_id)
                units[i] = unit_id
                i+=1

            units[5]=units[0]=self.unit_dict.to_int(battle.attacker_team.members[0].base_id)
            members:GacTeam = sorted(battle.attacker_team.members[1:],\
                        key=lambda unit: self.unit_dict.to_int(unit.base_id))
            i=6
            for unit in members:
                unit_id = self.unit_dict.to_int(unit.base_id)
                units[i] = unit_id
                i+=1

            an_insert+= units

            try:
                self.cursor.execute(query, an_insert)
                battles_uploaded+=1
            except sqlite3.DatabaseError:
                logger.error('failed to upload battle to db')
                traceback.print_exc()
                return False
        logger.debug('Uploaded %s battles to db', battles_uploaded)
        return True

    def upload_team_to_db(self, team, battle_id, side) -> bool:
        query = 'insert into participants '\
                '(pcp_battle_id, pcp_unit_id, pcp_side, pcp_role) '\
                'values (%s, %s, %s, %s)'

        for unit in team.members:
            unit_id = self.unit_dict.to_int(unit.base_id)
            try:
                self.cursor.execute(query,
                                    (battle_id, unit_id, side, unit.role))
            except mysql.connector.Error:
                logger.error('failed to upload participant to db ')
                traceback.print_exc()
                return False
        return True

    def scrape_round(self, round_data: dict):
        scrape_url = 'https://swgoh.gg/p/' + \
            str(round_data['attacker']) + '/gac-history/'
        scrape_url += '?gac=' + str(round_data['gac_num'])
        scrape_url += '&r=' + str(round_data['round_num'])

        page_is_loaded = False
        while not page_is_loaded:
            try:
                self.driver.get(scrape_url)
            # selenium.common.exceptions.WebDriverException: Message:
            # unknown error: net::ERR_NAME_NOT_RESOLVED
            except WebDriverException as err:
                logger.error('page loading error %s', err)
                self.rate_counter.request_failed()
                self.rate_limiter.adaptative_stall()
                continue

            try:
                round_element = self.driver.find_element(
                    By.CLASS_NAME, "list-group.media-list.media-list-stream.m-t-0")
                page_is_loaded = True

            except NoSuchElementException:
                logger.info('page didn\'t load, stalling a bit')
                self.rate_counter.request_failed()
                if not self.rate_limiter.adaptative_stall():  # no more stalling?
                    input('no more stalling?')
                    input('no more stalling?')

        try:
            round_element.find_element(
                By.CLASS_NAME, 'alert.alert-danger.text-center')
            logger.warning(
                'error element found, player %s failed to join gac', round_data['attacker'])
            gac_round = False
            self.rate_counter.request_errored()
            return False

        except NoSuchElementException:
            # no error, we are good
            self.rate_counter.request_served()

        whole_page = self.driver.page_source
        soup = BeautifulSoup(whole_page, 'lxml')
        round_element = soup.select(
            'body > div.container.p-t-md > div.content-container >'
            'div.content-container-primary > ul')

        gac_round = GacRound(round_element[0], round_data)
        if gac_round.reap_round():
            return gac_round
        else:
            return False

    def load_snapped_allycodes(self, current_gac_num):
        self.snapped_allycodes = []

        query = 'select distinct us_allycode from unit_stats '\
                'where us_gac_num = %s'
        try:
            self.cursor.execute(query, (current_gac_num,))
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.error('failed to load snapped allycodes')
            sys.exit(1)
        if not rows:
            logger.error(
                'no snapped allycodes at all for gac_num %s', current_gac_num)
            sys.exit(1)
        for row in rows:
            self.snapped_allycodes.append(row[0])
        return True

    def load_snapped_allycodes_slave(self, current_gac_num):

        self.snapped_allycodes = []
        query = 'select distinct allycode from local_snapped_allycodes '\
                'where gac_num = ?'
        try:
            self.cursor.execute(query, (current_gac_num,))
            rows = self.cursor.fetchall()
        except sqlite3.DatabaseError:
            logger.error('failed to load snapped allycodes for gac_num %s', current_gac_num)
            traceback.print_exc()
            sys.exit(1)
        if not rows:
            logger.error(
                'no snapped allycodes at all for gac_num %s', current_gac_num)
            sys.exit(1)
        for row in rows:
            self.snapped_allycodes.append(row[0])
        return True

    def load_jobs_to_scrape(self):
        query = 'select allycode, gac_num from _job_scan_battles order by id'
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.critical('failed to load jobs to scrape')
            sys.exit(1)

        if not rows:
            logger.warning('job list is empty')
            sys.exit(1)

        self.jobs_to_scrape = []
        for row in rows:
            job = {'allycode':row[0], 'gac_num':row[1]}
            self.jobs_to_scrape.append(job)

    def load_jobs_to_scrape_slave(self):
        query = 'select allycode, gac_num from local_job_scan_battles order by id'
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.critical('failed to load jobs to scrape')
            sys.exit(1)

        if not rows:
            logger.warning('job list is empty')
            sys.exit(1)

        self.jobs_to_scrape = []
        for row in rows:
            job = {'allycode':row[0], 'gac_num':row[1]}
            self.jobs_to_scrape.append(job)

    def scrape_leaderboards(self):
        self.rate_limiter = throttling.RateLimiter(0)
        self.rate_counter = throttling.RateCounter()

        # mimic human
        url = 'https://swgoh.gg/gac/leaderboard/'
        self.driver.get(url)
        time.sleep(5)

        q_del_used_page = 'delete from _job_scan_leaderboards '\
            'where page = %s'
        q_insert_ally = 'insert ignore into top_allycodes '\
                        '(ta_allycode, position, rating) '\
                        'values (%s, %s, %s)'

        q_pages = 'select league, division, page from _job_scan_leaderboards'
        self.cursor.execute(q_pages)
        rows = self.cursor.fetchall()
        logger.debug('loaded %s leaderboard pages to scan', len(rows))

        for league, division, page in rows:
            url = 'https://swgoh.gg/gac/leaderboard/'
            url += '?league=' + LEAGUES[league]
            url += '&division=' + str(division)
            url += '&page=' + str(page)

            logger.debug('fetching %s', url)

            page_loaded = False

            while not page_loaded:
                self.rate_limiter.wait_rate_limit()
                self.driver.get(url)
                soup = BeautifulSoup(self.driver.page_source, 'lxml')
                the_list = soup.select('body > div.container.p-t-md > div.content-container >'
                                       'div.content-container-primary.character-list > ul')
                if the_list:
                    page_loaded = True
                    self.rate_counter.request_served()
                else:
                    logger.warning('failed to load, stalling')
                    self.rate_counter.request_failed()
                    self.rate_limiter.adaptative_stall()
            the_list = the_list[0]
            players = the_list.find_all('li', class_='character')
            players.pop(0)  # table header
            logger.debug('got %s allies', len(players))

            position_calc = (page - 1) * 25 + 1
            scraped_players = []
            for player in players:
                href = player.find(class_='character')
                matching = re.search('/p/([0-9]{9})/', href.attrs['href'])
                if matching:
                    allycode = int(matching.group(1))

                    rating = href.find_all(class_='col-xs-3')
                    rating = rating[-1]
                    rating = rating.text
                    rating = rating.replace(',', '')
                    rating = int(rating)

                    position_real = href.find(class_='col-xs-1')
                    position_real = int(position_real.text)
                    if position_real != position_calc:
                        logger.warning('position mismatch calc %s real %s for ally %s',
                                       position_calc, position_real, allycode)

                    scraped_players.append((allycode, position_real, rating))
                else:
                    logger.warning(
                        'no allycode found for player at position %s', position_calc)
                position_calc += 1

            logger.info('scraped %s allycodes', len(scraped_players))
            for allycode, position, rating in scraped_players:
                self.cursor.execute(q_insert_ally,
                                    (allycode, position, rating))

            self.cursor.execute(q_del_used_page, (page,))
            self.db_connection.commit()
            logger.debug('scraped page %s', page)
            self.rate_limiter.wait_rate_limit()
            self.rate_counter.log_rates()

    def scrape_battles(self):
        # print('logger debug')
        # print(f'__name__ is {__name__}')
        # print('got logger', logger)
        # print('testing')
        # print('logging crit')
        # logger.critical('a crit msg')
        # print('logging error')
        # logger.error('an error msg')
        # print('logging warning')
        # logger.warning('a warning msg')
        # print('logging info')
        # logger.info('an info msg')
        # print('logging debug')
        # logger.debug('a debug msg')
        # print('done testing logger')

        self.load_jobs_to_scrape()
        assert self.jobs_to_scrape[0]['gac_num'] == self.jobs_to_scrape[-1]['gac_num']

        current_gac_num = self.jobs_to_scrape[0]['gac_num']
        self.load_snapped_allycodes(current_gac_num)
        round_data = {'snapped_allycodes': self.snapped_allycodes}
        self.rate_counter = throttling.RateCounter()
        self.rate_limiter = throttling.RateLimiter(5)

        for job in self.jobs_to_scrape:
            if job['allycode'] not in self.snapped_allycodes:
                logger.warning('%s allycode not snapped', job['allycode'])
                continue

            is_error = False
            for round_num in [1, 2, 3]:
                self.rate_limiter.wait_rate_limit()
                round_data['gac_num'] = job['gac_num']
                round_data['round_num'] = round_num
                round_data['attacker'] = job['allycode']
                round_outcome = self.scrape_round(round_data)
                if not round_outcome:
                    logger.info('%s round %s skipping', job['allycode'], round_num)
                    continue

                if not self.upload_round_to_db(round_outcome):
                    logger.error('failed to upload combat : %s vs %s in round %s to db',
                                 job['allycode'], round_outcome.defender, round_num)
                    is_error = True
                    break

                logger.info('uploaded %s vs %s round %s to db',
                            job['allycode'], round_outcome.defender, round_num)
            if is_error:
                self.db_connection.rollback()
                continue

            if not self.remove_used_allycode(job['allycode']):
                logger.error(
                    'faile to remove allycode %s from job list', job['allycode'])
                self.db_connection.rollback()

            self.db_connection.commit()
            logger.info('processed player %s', job['allycode'])
            self.rate_counter.log_rates()

    def prepare_jobs_scan_leaderboards(self):

        to_prepare = []

        for league, league_str in LEAGUES.items():
            for division in DIVISIONS:
                # no clue but that's how it's numbered
                div_number = 30 - (division*5)
                url = 'https://swgoh.gg/gac/leaderboard/'
                url += '?league=' + league_str
                url += '&division=' + str(div_number)
                url += '&page=1'

                self.driver.get(url)
                soup = BeautifulSoup(self.driver.page_source, 'lxml')

                pages_elements = soup.select('body > div.container.p-t-md >'
                                             'div.content-container >'
                                             'div.content-container-primary.character-list > ul >'
                                             'li.media.list-group-item.p-5 > ul > '
                                             'li:nth-child(1) > a')

                if pages_elements:  # has something
                    matching = re.search(
                        r'Page 1 of ([1-9][0-9]*)', pages_elements[0].text)
                    if matching:
                        pages_found = int(matching.group(1))
                        logger.debug('Found amount of pages %s', pages_found)
                    else:
                        logger.critical(
                            'cant get amount of pages from %s', pages_elements[0].text)
                        assert False
                else:
                    pages_found = 1

                choice = ''
                while choice not in ['y', 'n']:
                    print(f'Include in scan? {league_str} division {division} with {pages_found}'
                          f' pages of approx {pages_found *25} allycodes')

                    choice = input('include this to scan? y/n > ')
                    if choice == 'y':
                        to_prepare.append((league, div_number, pages_found))

                if choice == 'n':
                    break
            if choice == 'n':
                break

        query_truncate1 = 'truncate table _job_scan_leaderboards'
        query_truncate2 = 'truncate table top_allycodes'
        logger.debug('truncating table _job_scan_leaderboards')
        self.cursor.execute(query_truncate1)
        self.cursor.execute(query_truncate2)

        query_insert_job = 'insert into _job_scan_leaderboards '
        query_insert_job += '(league, division, page) '
        query_insert_job += 'values '
        query_insert_job += '(%s, %s, %s)'

        for league, div_number, pages in to_prepare:
            for page in range(1, pages+1):
                self.cursor.execute(
                    query_insert_job, (league, div_number, page))

        self.db_connection.commit()

    def prepare_jobs_scan_battles(self):
        query = 'truncate table _job_scan_battles'
        self.cursor.execute(query)

        query = 'insert into _job_scan_battles (allycode, gac_num) '
        query+= 'select distinct ta_allycode, ' + str(self.gac_generate_num) + ' '
        query+= 'from top_allycodes order by rating desc'

        self.cursor.execute(query)
        self.db_connection.commit()

    def scrape_battles_slave(self):

        self.load_jobs_to_scrape_slave()
        assert self.jobs_to_scrape[0]['gac_num'] == self.jobs_to_scrape[-1]['gac_num']
        self.current_gac_num = self.jobs_to_scrape[0]['gac_num']
        self.load_snapped_allycodes_slave(self.current_gac_num)
        round_data = {'snapped_allycodes': self.snapped_allycodes}
        self.rate_counter = throttling.RateCounter()
        self.rate_limiter = throttling.RateLimiter(0)

        self.driver.get('http://swgoh.gg')
        input('Hit enter when ready')
        for job in self.jobs_to_scrape:
            if job['allycode'] not in self.snapped_allycodes:
                logger.warning('%s allycode not snapped', job['allycode'])
                continue

            is_error = False
            for round_num in [1, 2, 3]:
                #input('Hit enter when ready')
                self.rate_limiter.wait_rate_limit()
                round_data['gac_num'] = job['gac_num']
                round_data['round_num'] = round_num
                round_data['attacker'] = job['allycode']
                round_outcome = self.scrape_round(round_data)
                if not round_outcome:
                    logger.info('%s round %s skipping', job['allycode'], round_num)
                    continue

                if not self.upload_round_to_db_slave(round_outcome):
                    logger.error('failed to upload combat : %s vs %s in round %s to db',
                                 job['allycode'], round_outcome.defender, round_num)
                    is_error = True
                    break

                logger.info('uploaded %s vs %s round %s to db',
                            job['allycode'], round_outcome.defender, round_num)
            if is_error:
                self.db_connection.rollback()
                continue

            if not self.remove_used_allycode_slave(job['allycode']):
                logger.error(
                    'failed to remove allycode %s from job list', job['allycode'])
                self.db_connection.rollback()

            self.db_connection.commit()
            logger.info('processed player %s', job['allycode'])
            self.rate_counter.log_rates()
