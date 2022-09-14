#!/usr/bin/env python -u

import time
import datetime
import re
import os
import sys
from random import random
import traceback
import mysql.connector
from bs4 import BeautifulSoup
import cchardet
#import lxml
from bs4 import Tag
from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.remote.webdriver import WebDriver


VERSION = "0.0.1"
DRIVER_PATH = 'c:\\as_is\\chromedriver.exe'
SPEED_OPTIMIZE_LEVEL = 10
CHROME_USER = 'lxmluser'
RATE_LIMIT = 4 # 3*(1+rnd) ~~  secs per request



class Dictionary:
    def __init__(self, table, cursor, db_conn) -> None:
        self.the_dict = {}
        self.table = table
        self.cursor = cursor
        self.db_conn = db_conn
        self.load_from_db()

    def load_from_db(self):
        if self.table == 'zuo_dict':
            query = 'select zuo_id, zuo_string from zuo_dict'
        elif self.table == 'unit_dict':
            query = 'select unit_id, base_id from unit_dict'
        elif self.table == 'dc_mechanics_dict':
            query = 'select dc_mc_id, mc_string from dc_mechanics_dict'
        else:
            logger.error('unknown dict table %s', self.table)
            assert False
        logger.debug('loading dictionary from %s', self.table)
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            self.the_dict[row[0]] = row[1]
            self.the_dict[row[1]] = row[0]
        logger.debug('dictionary loaded from %s', self.table)

    def to_string(self, dict_id: int) -> str:
        if dict_id in self.the_dict:
            if isinstance(dict_id, int):
                return self.the_dict[dict_id]
            elif isinstance(dict_id, str):
                logger.warning('translating str to str')
                return dict_id
            else:
                logger.critical('wrong type got into dictionary somehow')
                assert False
        else:
            logger.critical('given id not in dictionary %s', dict_id)

    def to_int(self, dict_str: str) -> int:
        if dict_str in self.the_dict:
            if isinstance(dict_str, str):
                return self.the_dict[dict_str]
            elif isinstance(dict_str, int):
                logger.warning('translating int to int')
                return dict_str
            else:
                logger.critical('wrong type got into dictionary somehow')
                assert False
        else:
            logger.critical('unit not found in dictionary %s', dict_str)
            return False


class GacUnit:
    def __init__(self, unit_element: Tag):
        self.unit_element = unit_element
        self.role: str
        self.base_id: str
        self.health: int
        self.prot: int

    def reap_unit(self):
        name = self.unit_element.find(class_='character-portrait__img')
        if not name:
            name = self.unit_element.find(class_='ship-portrait__img')

        self.base_id = name.attrs['data-base-id']

        if SPEED_OPTIMIZE_LEVEL < 1:
            statbar = self.unit_element.find(
                class_='gac-unit__bar-inner--prot')
            if statbar:
                statbar = statbar.attrs['style']
                matching = re.search('width: (\\d+)%', statbar)
                self.prot = matching.group(1)
            else:
                self.prot = -1

            statbar = self.unit_element.find_element(
                class_='gac-unit__bar-inner--hp')
            if statbar:
                statbar = statbar.atrrs['style']
                matching = re.search('width: (\\d+)%', statbar)
                self.health = matching.group(1)
            else:
                self.health = -1


class GacTeam:
    def __init__(self, team_element: Tag):
        self.team_element = team_element
        self.members = []

    def reap_team(self):
        unit_elements = self.team_element.find_all(class_='gac-unit')
        for unit_element in unit_elements:
            unit = GacUnit(unit_element)
            unit.reap_unit()
            self.members.append(unit)
        self.members[0].role = 'leader'
        for unit in self.members[1:]:
            unit.role = 'member'


class GacPlayerBattle:
    def __init__(self, battle_element: Tag) -> None:
        self.battle_element = battle_element
        self.type: str
        self.datetime: int
        self.outcome: str
        self.duration: int
        self.attempt: int
        self.attacker_team: GacTeam
        self.defender_team: GacTeam
        self.banners: int

    def reap_battle(self):

        if self.battle_element.find_all(class_='character-portrait__img'):
            # list not empty, squad battle
            self.type = 'squad'
        else:
            self.type = 'ship'
            self.attempt = 0
            return

        self.outcome = self.battle_element.find(
            class_="gac-summary__status").text
        self.datetime = self.battle_element.find(
            class_="gac-datetime").attrs['data-datetime']
        self.datetime = int(int(self.datetime)/1000)
        text = self.battle_element.find(class_='panel').text
        matching = re.search('Length: (\\d):(\\d+)', text)
        self.duration = int(matching.group(1))*60 + int(matching.group(2))
        matching = re.search('Attempt: (\\d+)', text)
        self.attempt = int(matching.group(1))
        if self.outcome.find("WIN") >= 0:
            matching = re.search('Banners: (\\d+)', text)
            self.banners = int(matching.group(1))
        elif self.outcome.find("LOSS") >= 0 or \
                self.outcome.find("TIMEOUT") >= 0 or \
                self.outcome.find("QUIT") >= 0:
            self.banners = 0
        else:
            logger.critical(
                'neither WIN nor LOSS/TIMEOUT/QUIT: %s', self.outcome)
            assert False

        teams = self.battle_element.find_all(class_='gac-squad')

        self.attacker_team = GacTeam(teams[0])
        self.attacker_team.reap_team()
        self.defender_team = GacTeam(teams[1])
        self.defender_team.reap_team()


class GacRound:
    def __init__(self, round_element: Tag, round_data: dict) -> None:
        self.round_data = round_data
        self.battles = []
        self.round_element = round_element
        self.defender: int
        self.attacker = round_data['attacker']

    def reap_round(self):
        start_time = time.time()

        winner = self.round_element.find_all(class_="winner")
        if not winner:  # empty list
            logger.warning('could not find winner element')
            return False
        winner = winner[0]
        logger.debug('got winner element')

        loser = self.round_element.find_all(class_='loser')
        if not loser:  # empty list
            logger.warning('could not find winner element')
            return False
        loser = loser[0]
        logger.debug('got loser element')

        if 'href' in winner.attrs:
            enemy = winner.attrs['href']
        elif 'href' in loser.attrs:
            enemy = loser.attrs['href']
        else:
            logger.critical('No href found, cant find defender allycode')
            assert False
        matching = re.search('/p/([0-9]{9})/', enemy)
        if matching:
            self.defender = int(matching.group(1))
        else:
            logger.critical('failed to get defender allycode from regex')
            assert False

        if self.defender not in self.round_data['snapped_allycodes']:
            logger.info('combat : %s vs %s in round %s enemy allycode not snapped',
                        self.round_data['attacker'], self.defender, self.round_data['round_num'])
            return False

        battle_elements = self.round_element.find_all(
            class_='gac-player-battles')

        battle_elements.pop(0)  # first line is headline
        for battle_element in battle_elements:
            battle = GacPlayerBattle(battle_element)
            battle.reap_battle()
            self.battles.append(battle)

        reaping_time = time.time() - start_time
        if len(self.battles) > 0:
            logger.debug('reaped %s battles in %s (%s per battle)',
                        len(self.battles), str(round(reaping_time, 3)),
                        str(round(reaping_time / len(self.battles), 3)))
        else:
            logger.info('reaped 0 battles in %s', str(round(reaping_time, 3)))

        return True


class RateLimiter:
    def __init__(self) -> None:
        self.last_call = time.time()
        self.stalls = []
        self.rate_adjust = 0
        self.rate_adjust_rnd = 0
        self.stall_adjust = 0
        self.stall_adjust_rnd = 0

    def wait_rate_limit(self):
        sleeptime = self.last_call - time.time()
        sleeptime += RATE_LIMIT + self.rate_adjust
        sleeptime += (RATE_LIMIT + self.rate_adjust_rnd) * random()

        if sleeptime > 0:
            time.sleep(sleeptime)
        self.last_call = time.time()
        if random() < 0.05: #occassionally required
            self.recalibrate()

    def recalibrate(self):
        now = time.time()
        last_5_min = 0
        last_15_min = 0
        last_h = 0
        for stall in reversed(self.stalls):
            if (now - stall) <= 300:
                last_5_min += 1
            if (now - stall) <= 900:
                last_15_min += 1
            if (now - stall) <= 3600:
                last_h += 1
            if (now - stall) > 3600:  # 1h expire time
                self.stalls.remove(stall)

        self.rate_adjust = round(
            last_h / 60 + last_15_min / 15 + last_5_min / 5, 3)
        self.rate_adjust_rnd = round(last_15_min/5 + last_5_min, 3)

        self.stall_adjust = round(
            last_h / 60 + last_15_min / 15 + (last_5_min - 1) * 5, 3)
        self.stall_adjust_rnd = round(
            last_h / 10 + last_15_min / 5 + last_5_min * 5, 3)

        logger.info('recalibrated rate %s %s stalls %s %s',
                     self.rate_adjust, self.rate_adjust_rnd,
                     self.stall_adjust, self.stall_adjust_rnd)

    def adaptative_stall(self) -> bool:
        now = time.time()
        self.stalls.append(now)
        if len(self.stalls) > 30:  # deadlock? drop last 5mins and continue
            while (now - self.stalls[-1] < 300) or (len(self.stalls) > 25):
                self.stalls.pop(-1)
            logger.warning(
                'too many stalls, pruning atleast 5 last and 5 last mins')
            return False
        else:
            self.recalibrate()
            sleeptime = self.stall_adjust + self.stall_adjust_rnd * random()
            logger.info('stalling for %s', sleeptime)
            time.sleep(sleeptime)
            return True

class RateCounter:
    def __init__(self) -> None:
        self.activity_start = time.time()
        self.requests_served = 1
        self.requests = 1
        self.requests_failed = 1
        self.requests_errored = 1

    def request_served(self):
        self.requests +=1
        self.requests_served +=1

    def request_failed(self):
        self.requests +=1
        self.requests_failed +=1

    def request_errored(self):
        self.requests +=1
        self.requests_errored +=1

    def log_rates(self):
        now = time.time()
        time_passed = now - self.activity_start
        logger.info('In %s, all %s %ss/1, served %s %ss/1, failed %s %ss/1, errored %s %ss/1 ',\
                datetime.timedelta(seconds = time_passed),\
                self.requests, round(time_passed/self.requests,3),\
                self.requests_served, round(time_passed/self.requests_served,3),\
                self.requests_failed, round(time_passed/self.requests_failed,3),\
                self.requests_errored, round(time_passed/self.requests_errored,3))

class SwgohGgScraper:
    def __init__(self, gac_num, job_table) -> None:
        self.initialize_chrome()
        self.db_connect()
        self.allycodes_to_scrape: list
        self.job_table = job_table
        self.snapped_allycodes: list
        self.unit_dict = Dictionary(
            'unit_dict', self.cursor, self.db_connection)
        self.gac_num = gac_num
        self.limiter = RateLimiter()
        self.rate_counter:RateCounter

    def db_connect(self):

        mysql_database_name = "swgoh_gac"
        mysql_user = os.environ.get("mysql_user")
        mysql_password = os.environ.get("mysql_password")

        try:
            self.db_connection = mysql.connector.connect(
                host='localhost',
                user=mysql_user,
                password=mysql_password,
                database=mysql_database_name
            )
            self.cursor = self.db_connection.cursor()
        except mysql.connector.Error:
            logger.critical('Connection to db failed')
            sys.exit(1)

    def initialize_chrome(self):
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--user-data-dir=c:\\users\\' + CHROME_USER)
        options.add_argument("--start-maximized")
        # options.add_argument("--headless")
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")

        self.driver = webdriver.Chrome(
            executable_path=DRIVER_PATH, options=options)

        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )

    def remove_used_allycode(self, allycode) -> bool:
        query = 'delete from ' + self.job_table
        query += ' where allycode = (%s) '
        try:
            self.cursor.execute(query, (int(allycode),))  # one element tuple
        except mysql.connector.Error:
            logger.error('failed to remove used allycode from job table')
            traceback.print_exc()
            return False
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
                                         battle.datetime, battle.duration, self.gac_num))
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
        scrape_url += '?gac=' + str(self.gac_num)
        scrape_url += '&r=' + str(round_data['round_num'])

        page_is_loaded = False
        while not page_is_loaded:
            self.driver.get(scrape_url)
            try:
                round_element = self.driver.find_element(
                    By.CLASS_NAME, "list-group.media-list.media-list-stream.m-t-0")
                page_is_loaded = True

            except NoSuchElementException:
                logger.info('page didn\'t load, stalling a bit')
                self.rate_counter.request_failed()
                if not self.limiter.adaptative_stall(): #no more stalling?
                    input('no more stalling?')
                    input('no more stalling?')

        try:
            round_element.find_element(
                By.CLASS_NAME, 'alert.alert-danger.text-center')
            logger.warning(
                'error element found, player %s failed to join gac', round_data['attacker'])
            gac_round = False
            self.rate_counter.request_errored()
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

    def load_snapped_allycodes(self):
        self.snapped_allycodes = []
        query = 'select distinct us_allycode from unit_stats '\
                'where us_gac_num = %s'
        try:
            self.cursor.execute(query, (self.gac_num,))
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.error('failed to load snapped allycodes')
            sys.exit(1)
        if not rows:
            logger.error(
                'no snapped allycodes at all for gac_num %s', self.gac_num)
            sys.exit(1)
        for row in rows:
            self.snapped_allycodes.append(row[0])
        return True

    def load_allycodes_to_scrape(self):
        self.allycodes_to_scrape = []
        query = 'select distinct allycode from ' + self.job_table
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.critical('failed to load jobs to scrape')
            sys.exit(1)

        if not rows:
            logger.warning('job list is empty')
            sys.exit(1)

        for row in rows:
            self.allycodes_to_scrape.append(row[0])

    def scrape_leaderboard(self):
        query = 'select page_num from leaderboard_pages_to_scan'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        pages = []
        for row in rows:
            pages.append(row[0])

        # mimic human
        url = 'https://swgoh.gg/gac/leaderboard/'
        self.driver.get(url)
        self.limiter.wait_rate_limit()

        q_del_page = 'delete from leaderboard_pages_to_scan '\
            'where page_num = %s'
        q_allycode = 'insert into leaderboard_kyber1 (allycode, position, page) '\
            'values (%s, %s, %s)'

        for page in pages:
            position = 1 + int(page-1)*25
            url = 'https://swgoh.gg/gac/leaderboard/?league=KYBER&division=25&page=' \
                + str(page)

            self.driver.get(url)

            p_list = self.driver.find_element(
                By.CLASS_NAME, "content-container-primary.character-list")
            players = p_list.find_elements(
                By.CLASS_NAME, 'media.list-group-item.p-0.character')
            players.pop(0)  # table header
            for player in players:
                href = player.find_element(
                    By.CLASS_NAME, 'media-body.character').get_attribute('href')
                matching = re.search('/p/([0-9]{9})/', href)
                if matching:
                    allycode = int(matching.group(1))
                    self.cursor.execute(q_allycode, (allycode, position, page))
                else:
                    logger.debug('no allycode found for position %s', position)
                position += 1
            self.cursor.execute(q_del_page, (page,))
            self.db_connection.commit()
            self.limiter.wait_rate_limit()

    def scrape_players(self):
        self.load_allycodes_to_scrape()
        self.load_snapped_allycodes()
        round_data = {'snapped_allycodes': self.snapped_allycodes}
        self.rate_counter = RateCounter()

        for allycode in self.allycodes_to_scrape:
            if allycode not in self.snapped_allycodes:
                logger.warning('%s allycode not snapped')
                continue

            is_error = False
            for round_num in [1, 2, 3]:
                self.limiter.wait_rate_limit()
                round_data['round_num'] = round_num
                round_data['attacker'] = allycode
                round_outcome = self.scrape_round(round_data)
                if not round_outcome:
                    logger.info('%s round %s skipping', allycode, round_num)
                    continue

                if not self.upload_round_to_db(round_outcome):
                    logger.error('failed to upload combat : %s vs %s in round %s to db',
                                 allycode, round_outcome.defender, round_num)
                    is_error = True
                    break

                logger.info('uploaded %s vs %s round %s to db',
                            allycode, round_outcome.defender, round_num)
            if is_error:
                self.db_connection.rollback()
                continue

            if not self.remove_used_allycode(allycode):
                logger.error(
                    'faile to remove allycode %s from job list', allycode)
                self.db_connection.rollback()

            self.db_connection.commit()
            logger.info('processed player %s', allycode)
            self.rate_counter.log_rates()


def main():
    gac_num = 114
    job_table = '_job_scan_player_battles'
    scraper = SwgohGgScraper(gac_num, job_table)
    driver = scraper.driver

    # url='https://www.whatismybrowser.com/detect/what-is-my-user-agent/'
    # driver.get(url)

    # dummy start
    url = 'https://swgoh.gg'
    driver.get(url)

    input('Press enter to start scraping')

    # scraper.scrape_leaderboard()
    scraper.scrape_players()

    print('work done')
    input()
    logger.debug('Saving cookies')


if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

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
