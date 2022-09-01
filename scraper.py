#!/usr/bin/env python -u


import time
import re
import os
import sys
import random

import mysql.connector
from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver

VERSION = "0.0.1"
DRIVER_PATH = 'c:\\as_is\\chromedriver.exe'
COOKIES_FILE = 'c:\\_programming\\python\\gac_scraper\\cookies.pkl'


class GacUnit:
    def __init__(self, web_element: WebDriver) -> None:
        self.role:str
        try:
            statbar = web_element.find_element(
                By.CLASS_NAME, 'gac-unit__bar-inner--prot')
            statbar = statbar.get_attribute('style')
            matching = re.search('width: (\\d+)%', statbar)
            self.prot = matching.group(1)
        except NoSuchElementException:
            self.prot = -1

        try:
            statbar = web_element.find_element(
                By.CLASS_NAME, 'gac-unit__bar-inner--hp')
            statbar = statbar.get_attribute('style')
            matching = re.search('width: (\\d+)%', statbar)
            self.health = matching.group(1)
        except NoSuchElementException:
            self.health = -1

        try:
            name = web_element.find_element(
                By.CLASS_NAME, 'character-portrait__img')
        except NoSuchElementException:
            name = web_element.find_element(
                By.CLASS_NAME, 'ship-portrait__img')
        self.data_base_id = name.get_attribute('data-base-id')


class GacTeam:
    def __init__(self, web_element: WebDriver) -> None:
        self.members = []
        units = web_element.find_elements(By.CLASS_NAME, 'gac-unit')
        for unit in units:
            self.members.append(GacUnit(unit))
        self.members[0].role = 'leader'
        for unit in self.members[1:]:
            unit.role = 'member'


class GacPlayerBattle:
    def __init__(self, web_element: WebDriver) -> None:
        try:
            _ = web_element.find_element(
                By.CLASS_NAME, 'character-portrait__img')
            self.type = 'squad'
        except NoSuchElementException:
            self.type = 'ship'

        self.outcome = web_element.find_element(By.CLASS_NAME, "gac-summary__status")\
            .text
        self.datetime = web_element.find_element(By.CLASS_NAME, "gac-datetime")\
            .get_attribute('data-datetime')
        text = web_element.find_element(By.CLASS_NAME, 'panel')\
            .text
        matching = re.search('Length: (\\d):(\\d+)', text)
        self.duration = int(matching.group(1))*60 + int(matching.group(2))
        matching = re.search('Attempt: (\\d+)', text)
        self.attempt = int(matching.group(1))
        if self.outcome == "WIN":
            matching = re.search('Banners: (\\d+)', text)
            self.banners = int(matching.group(1))
        else:
            self.banners = 0

        teams = web_element.find_elements(By.CLASS_NAME, 'gac-squad')
        self.attacker_team = GacTeam(teams[0])
        self.defender_team = GacTeam(teams[1])


class GacRound:
    def __init__(self, web_element: WebDriver, allycode) -> None:
        self.attacker = allycode

        winner = web_element.find_element(
            By.CLASS_NAME, 'winner').get_attribute('href')
        loser = web_element.find_element(
            By.CLASS_NAME, 'loser').get_attribute('href')
        if winner:
            enemy = winner
        elif loser:
            enemy = loser
        else:
            assert False
        matching = re.search('/p/([0-9]{9})/', enemy)
        if matching:
            self.defender = matching.group(1)
        else:
            assert False

        self.battles = []

        battle_elements = web_element.find_elements(
            By.CLASS_NAME, 'gac-player-battles.media.list-group-item.p-a')

        battle_elements.pop(0)  # first line is headline
        for battle_element in battle_elements:
            self.battles.append(GacPlayerBattle(battle_element))


class SwgohScraper:
    def __init__(self) -> None:
        self.initialize_chrome()
        self.db_connect()
        self.allycodes_to_scrape: list
        self.snapped_allycodes: list
        self.unit_dict: dict

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
        except:
            print('Connection to db failed')
            sys.exit()

    def initialize_chrome(self):
        options = Options()
        options.add_argument('no-sandbox')
        options.add_argument("--user-data-dir=c:\\users\\selenium")
        options.add_argument("start-maximized")
        # options.add_argument("--headless")
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")

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

    def load_unit_dictionary(self):
        self.unit_dict = {}
        query = 'select id, unit_id from unit_dict'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            self.unit_dict[row[0]] = row[1]
            self.unit_dict[row[1]] = row[0]

    def remove_used_allycode(self, allycode):
        query = 'delete from _job_scan_players '\
                'where allycode = (%s) '
        self.cursor.execute(query, (int(allycode),))  # one element tuple
        self.db_connection.commit()

    def upload_round_to_db(self, a_round: GacRound):
        attacker = int(a_round.attacker)
        defender = int(a_round.defender)
        for battle in a_round.battles:
            if battle.type == 'squad':
                query = 'insert into battles (attacker, defender, banners, datetime, duration) '\
                        'values (%s, %s, %s, %s, %s)'
                self.cursor.execute(query, \
                    (attacker, defender, battle.banners, battle.datetime, battle.duration))
                battle_id = self.cursor.lastrowid
                self.upload_team_to_db(
                    battle.attacker_team, battle_id, 'attacker'
                )
                self.upload_team_to_db(
                    battle.defender_team, battle_id, 'defender'
                )

    def upload_team_to_db(self, team, battle_id, side):
        query = 'insert into participants '\
                '(pcp_battle_id, pcp_unit_id, pcp_side, pcp_role) '\
                'values (%s, %s, %s, %s)'

        for unit in team:
            role_char = unit.role[0] #first letter
            side_char = side[0]
            unit_id = self.unit_dict[unit.base_id]
            self.cursor.execute(query, \
                (battle_id, unit_id, side_char, role_char))

    def scrape_round(self, gac_number, round_num, allycode):
        scrape_url = 'https://swgoh.gg/p/' + allycode + '/gac-history/'
        if round_num > 1:  # can do, always scanning last anyway, so mimic human
            scrape_url += '?gac=' + str(gac_number)
            scrape_url += '&r=' + str(round_num)
        self.driver.get(scrape_url)
        try:
            web_element = self.driver.find_element(
                By.CLASS_NAME, "list-group.media-list.media-list-stream.m-t-0")
        except NoSuchElementException:
            web_element = False

        if web_element:
            gac_round = GacRound(web_element, allycode)
        else:
            gac_round = False

        return gac_round

    def random_wait(self):
        wait_min = 2
        wait_max = 7
        time.sleep(wait_min+random.random()*(wait_max-wait_min))

    def load_snapped_allycodes(self):
        self.snapped_allycodes = []
        query = 'select distinct us_player from unit_stats'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            self.snapped_allycodes.append(row[0])

    def load_allycodes_to_scrape(self):
        self.allycodes_to_scrape = []
        query = 'select distinct allycode from _job_scan_players'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
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
        self.random_wait()

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
            self.random_wait()

    def scrape_players(self, gac_number):
        self.load_allycodes_to_scrape()
        self.load_snapped_allycodes()
        self.load_unit_dictionary()

        for allycode in self.allycodes_to_scrape:
            assert allycode in self.snapped_allycodes
            self.random_wait()

            round1 = self.scrape_round(gac_number, 1, allycode)
            if round1:
                self.random_wait()
                round2 = self.scrape_round(gac_number, 2, allycode)
                self.random_wait()
                round3 = self.scrape_round(gac_number, 3, allycode)

                if round1.defender in self.snapped_allycodes:
                    self.upload_round_to_db(round1)
                else:
                    logger.warning(
                        'enemy %s has no data, skipping round 1', round1.defender)
                if round2.defender in self.snapped_allycodes:
                    self.upload_round_to_db(round2)
                else:
                    logger.warning(
                        'enemy %s has no data, skipping round 2', round2.defender)
                if round3.defender in self.snapped_allycodes:
                    self.upload_round_to_db(round3)
                else:
                    logger.warning(
                        'enemy %s has no data, skipping round 3', round3.defender)
            else:
                logger.warning('player has no data, skipping %s', allycode)
            self.remove_used_allycode(allycode)


def main():

    scraper = SwgohScraper()
    driver = scraper.driver

    # url='https://www.whatismybrowser.com/detect/what-is-my-user-agent/'
    # driver.get(url)

    # dummy start
    url = 'https://swgoh.gg'
    driver.get(url)

    input('Press enter to start scraping')

    # scraper.scrape_leaderboard()
    scraper.scrape_players(gac_number=113)

    print('work done')
    input()
    logger.debug('Saving cookies')


if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    # logger.addHandler(fh)
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
