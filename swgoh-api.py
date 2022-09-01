#!/usr/bin/env python -u


import time
import re
import os
import sys
import random
import json

# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from selenium.common.exceptions import NoSuchElementException
# from selenium_stealth import stealth
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.remote.webdriver import WebDriver
import mysql.connector
import requests

VERSION = "0.0.1"
DRIVER_PATH = 'c:\\as_is\\chromedriver.exe'
COOKIES_FILE = 'c:\\_programming\\python\\gac_scraper\\cookies.pkl'


class SwgohGgApi:
    def __init__(self) -> None:
        self.db_connect()
        self.load_unit_dictionary()

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

    def load_unit_dictionary(self):
        self.unit_dict = {}
        query = 'select unit_id, base_id from unit_dict'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            self.unit_dict[row[0]] = row[1]
            self.unit_dict[row[1]] = row[0]

    def load_ship_dictionary(self):
        assert False
        self.ship_dict = {}
        query = 'select ship_id, base_id from ship_dict'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            self.ship_dict[row[0]] = row[1]
            self.ship_dict[row[1]] = row[0]

    def update_unit_dictionary(self):
        self.load_unit_dictionary()
       # self.load_ship_dictionary()

        url = 'http://api.swgoh.gg/units/'
        response = requests.get(url)
        units = json.loads(response.text)
        units = units['data']

        q_insert_unit = 'insert into unit_dict (base_id, name) '\
                        'values (%s, %s)'
        for unit in units:
            if unit['base_id'] not in self.unit_dict:
                self.cursor.execute(
                    q_insert_unit, (unit['base_id'], unit['name']))

        # ships are in unit dict anyway

        # url = 'http://api.swgoh.gg/ships/'
        # response = requests.get(url)
        # ships = json.loads(response.text)

        # q_insert_ship = 'insert into ship_dict (base_id, name) '\
        #                 'values (%s, %s)'
        # for ship in ships:
        #     if ship['base_id'] not in self.ship_dict:
        #         self.cursor.execute(
        #             q_insert_ship, (ship['base_id'], ship['name']))

        self.db_connection.commit()

    def update_stats_dictionary(self):
        url = 'http://api.swgoh.gg/stat-definitions/'
        response = requests.get(url)
        stats = json.loads(response.text)
        query = 'insert into stat_dict values (%s, %s, %s, %s)'

        for stat in stats:
            if stat['is_decimal']:
                is_decimal = 1
            elif not stat['is_decimal']:
                is_decimal = 0
            else:
                assert False

            self.cursor.execute(query,
                                (int(stat['stat_id']), stat['name'], stat['detailed_name'], is_decimal))
        self.db_connection.commit()



    def scrape_player(self, allycode):
        url = 'http://api.swgoh.gg/player/' + str(allycode) + '/'
        response = requests.get(url)
        player = json.loads(response.text)
        units = player['units']
        datacrons = player['datacrons']

class TrueUnitStats:
    def __init__(self) -> None:
        self.stats = {}
        self.stats[1] = {'name' : 'health', 'type' : 'mediumint', 'gather' : True}
        self.stats[2] = {'name' : 'strength', 'type' : 'smallint', 'gather' : False}
        self.stats[3] = {'name' : 'agility', 'type' : 'smallint', 'gather' : False}
        self.stats[4] = {'name' : 'tactics', 'type' : 'smallint', 'gather' : False}
        self.stats[5] = {'name' : 'speed', 'type' : 'smallint', 'gather' : True}
        self.stats[6] = {'name' : 'phys_dmg', 'type' : 'smallint', 'gather' : True}
        self.stats[7] = {'name' : 'special_dmg', 'type' : 'smallint', 'gather' : True}

        #convert to raw armor, store as small int, save 4
        self.stats[8] = {'name' : 'armor', 'type' : 'smallint', 'gather' : True}
        self.stats[9] = {'name' : 'resistance', 'type' : 'smallint', 'gather' : True}

        self.stats[10] = {'name' : 'arp_pen', 'type' : 'float', 'gather' : False}
        self.stats[11] = {'name' : 'resist_pen', 'type' : 'float', 'gather' : False}
        self.stats[12] = {'name' : 'dodge', 'type' : 'float', 'gather' : False}
        self.stats[13] = {'name' : 'deflection', 'type' : 'float', 'gather' : False}

        # stored as percents, 100 = 100%  converted round(crit*100) from 1.0 float
        self.stats[14] = {'name' : 'phys_crit', 'type' : 'tinyint', 'gather' : True}
        self.stats[15] = {'name' : 'special_crit', 'type' : 'tinyint', 'gather' : True}

        # stored as percent minus base 1.5, round((crit_dmg-1.5)*100). 50 = 200% from 2.0 float
        self.stats[16] = {'name' : 'crit_dmg', 'type' : 'tinyint', 'gather' : True}

        # stored as percent round(potency *100) 150 = 150% from 1.5 float
        self.stats[17] = {'name' : 'potency', 'type' : 'tinyint', 'gather' : True}
        self.stats[18] = {'name' : 'tenacity', 'type' : 'tinyint', 'gather' : True}

        self.stats[27] = {'name' : 'hp_steal', 'type' : 'float', 'gather' : False}
        self.stats[28] = {'name' : 'protection', 'type' : 'mediumint', 'gather' : True}
        self.stats[37] = {'name' : 'phys_acc', 'type' : 'float', 'gather' : False}
        self.stats[38] = {'name' : 'special_acc', 'type' : 'float', 'gather' : False}
        self.stats[39] = {'name' : 'phys_ca', 'type' : 'float', 'gather' : False}
        self.stats[40] = {'name' : 'special_ca', 'type' : 'float', 'gather' : False}

        self.misc = {}
        self.misc['us_id'] = {'type' : 'int', 'gather' : True}
        self.misc['us_allycode'] = {'type' : 'int', 'gather' : True}
        self.misc['us_gac_num'] = {'type' : 'smallint', 'gather' : True}
        self.misc['us_unit_id'] = {'type' : 'smallint', 'gather' : True}
        self.misc['us_date'] = {'type' : 'int', 'gather' : True}

        #other things to store
        #store combined gear_level + relic, 13= g13 r0, 15= r2 etc 22=r9
        #api returns 1 for g<13, rest:tier=2=relic0 3=1 4=2 5=3 6=4 7=5 8=6 9=7 10=8 11=9
        self.misc['relic_tier'] ={'type' : 'tinyint', 'gather' : True}
        self.misc['level'] ={'type' : 'tinyint', 'gather' : False}
        self.misc['gear_level'] ={'type' : 'tinyint', 'gather' : False}



    def raw_to_percent(self, raw_value):
        return (raw_value*100)/(raw_value +85*7.5)

    def percent_to_raw(self, percent_value):
        return int(round((percent_value * 85 *7.5)/(100 - percent_value),0))

    def get_store_cost(self):
        cost = 0
        tmp_dict = dict(self.stats)
        tmp_dict.update(self.misc)
        for _, value in tmp_dict.items():
            if value['gather']:
                if value['type'] == 'smallint':
                    cost += 2
                elif value['type'] == 'mediumint':
                    cost += 3
                elif value['type'] == 'float':
                    cost += 4
                elif value['type'] == 'tinyint':
                    cost += 1
                elif value['type'] == 'int':
                    cost += 4
                else:
                    pass
        return cost

    def generate_sql_create_table(self):
        for key, value in self.misc.items():
            if value['gather']:
                print('  ', key, value['type'])
        for _, value in self.stats.items():
            if value['gather']:
                print('  ', value['name'], value['type'])



def main():
    api = SwgohGgApi()
    stats = TrueUnitStats()
    cost = stats.get_store_cost()
    cost = cost 
    print(cost, 'bytes per character')
    print('approx ', round(cost *5500*230*3 /1024/1024,3), 'MiB per season')

    stats.generate_sql_create_table()

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
