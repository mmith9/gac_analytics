#!/usr/bin/env python -u

import time
import os
import sys
import random
import json
import traceback
import mysql.connector
import requests

VERSION = "0.0.1"


class LocalDictionary:
    def __init__(self) -> None:
        self.the_dict = {}

    def update_from_dict(self, a_dict: dict):
        for key, value in a_dict.items():
            self.update(key, value['name'])

    def update(self, string_id: int, string_to_insert: str):
        if string_id in self.the_dict or string_to_insert in self.the_dict:
            logger.warning('item allready in dictionary %s %s',
                           string_id, string_to_insert)
            return False
        self.the_dict[string_id] = string_to_insert
        self.the_dict[string_to_insert] = string_id
        return True

    def to_string(self, dict_id: int) -> str:
        if dict_id in self.the_dict:
            if isinstance(dict_id, int):
                return self.the_dict[dict_id]
            elif isinstance(dict_id, str):
                logger.warning('translating str to str')
                return dict_id
            else:
                logger.critical('wrong type got into dictionary somehow')
                return False
        else:
            logger.critical('given id not in dictionary %s', dict_id)
            return False

    def to_int(self, dict_str: str) -> int:
        if dict_str in self.the_dict:
            if isinstance(dict_str, str):
                return self.the_dict[dict_str]
            elif isinstance(dict_str, int):
                logger.warning('translating int to int')
                return dict_str
            else:
                logger.critical('wrong type got into dictionary somehow')
                return False
        else:
            logger.critical('given id not in dictionary %s', dict_str)
            return False


class Dictionary:
    def __init__(self, table, cursor, db_conn) -> None:
        self.the_dict = {}
        self.table = table
        self.cursor = cursor
        self.db_conn = db_conn
        self.load_from_db()
        if table == 'unit_dict':
            self.update_unit_names()

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
            self.update(dict_str)
            return self.the_dict[dict_str]

    def update(self, string_to_insert):
        if self.table == 'zuo_dict':
            query = 'insert into zuo_dict (zuo_string) values (%s) '
        elif self.table == 'unit_dict':
            query = 'insert into unit_dict (base_id) values (%s) '
        elif self.table == 'dc_mechanics_dict':
            query = 'insert into dc_mechanics_dict (mc_string) values (%s) '
        else:
            logger.error('unknown dict table %s', self.table)
            assert False

        self.cursor.execute(query, (string_to_insert,))
        self.db_conn.commit()
        dict_id = self.cursor.lastrowid
        self.the_dict[string_to_insert] = dict_id
        self.the_dict[dict_id] = string_to_insert

    def update_unit_names(self):
        if self.table != 'unit_dict':
            logger.warning('can only update unit_dict, not %s', self.table)
            return

        query = 'select unit_id, base_id from unit_dict '\
                'where name is null'
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.error('failed to check table unit_dict for null names')
            return

        if not rows:
            logger.debug('nothing to update')
            return

        url = 'http://api.swgoh.gg/units/'
        try:
            response = requests.get(url)
            definitions = json.loads(response.text)['data']
        except ConnectionError:
            logger.error('failed to fetch unit names from swgoh.gg')
            return

        query = 'update unit_dict '\
                'set name = %s '\
                'where base_id = %s '
        for row in rows:
            base_id = row[1]
            long_name = ''
            for a_dict in definitions:
                if a_dict['base_id'] == base_id:
                    long_name = a_dict['name']
                    break

            if not long_name:
                logger.error('cannot find name for %s', base_id)
                continue

            try:
                self.cursor.execute(query, (long_name, base_id))
            except mysql.connector.Error:
                logger.error('failed to update name of %s -> %s',
                             base_id, long_name)
                traceback.print_exc()
                continue
        self.db_conn.commit()


class SwgohGgApi:
    def __init__(self, gac_num) -> None:
        self.db_connect()
        self.unit_dict = Dictionary(
            'unit_dict', self.cursor, self.db_connection)
        self.dc_dict = Dictionary(
            'dc_mechanics_dict', self.cursor, self.db_connection)
        self.zuo_dict = Dictionary('zuo_dict', self.cursor, self.db_connection)
        self.stats_dict = LocalDictionary()
        self.gac_num = gac_num

        all_stats = AllUnitStats()
        self.stats_dict.update_from_dict(all_stats.stats)

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
            print('Connection to db failed')
            sys.exit()

    def scrape_player(self, allycode):
        url = 'http://api.swgoh.gg/player/' + str(allycode) + '/'
        try:
            response = requests.get(url)
        except ConnectionError:
            logger.warning('connection error while fetching %s', allycode)
            return False

        try:
            player = json.loads(response.text)
        except json.decoder.JSONDecodeError:
            logger.error('failed to decode response code: %s',
                         response.status_code)
            return False

        return player

    def scrape_players(self, a_job) -> bool:
        query = 'select allycode from ' + a_job
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except mysql.connector.Error:
            logger.critical('failed to load job list')
            traceback.print_exc()
            return False

        if not rows:
            logger.warning('no jobs found in db')
            return False

        allycodes = []
        for row in rows:
            allycodes.append(row[0])

        a_datacron = Datacron(self.dc_dict)
        unit_stats = UnitStats(self.unit_dict, self.zuo_dict)

        for allycode in allycodes:
            is_error = False
            time.sleep(random.random()*2)
            logger.debug('starting scraping player %s', allycode)
            player = self.scrape_player(allycode)
            if not player:
                logger.error('Failed to scrape player %s', allycode)
                continue

            for unit in player['units']:
                if not unit_stats.reap_from(unit):
                    logger.debug('unit reap failed - ship')
                    continue

                if unit_stats.save_yourself_to_db(allycode, self.gac_num, self.cursor):
                    logger.debug('unit saved to db')
                else:
                    logger.error('failed save to db')
                    is_error = True
                    break

            if is_error:
                self.db_connection.rollback()
                continue

            for p_datacron in player['datacrons']:
                if not a_datacron.reap_from(p_datacron):
                    logger.debug('empty datacron not reaped %s', p_datacron)
                    continue
                if a_datacron.save_yourself_to_db(allycode, self.gac_num, self.cursor):
                    logger.debug('datacron saved to db')
                else:
                    logger.error('failed to save datacron to db')
                    is_error = True
                    break

            if is_error:
                logger.error('aborting player %s', allycode)
                self.db_connection.rollback()
                continue

            logger.debug('scraped player %s', allycode)

            query = 'delete from ' + a_job + ' where allycode = %s'
            try:
                self.cursor.execute(query, (allycode,))
            except mysql.connector.Error:
                logger.error('failed to remove used allycode from db')
                is_error = True
                self.db_connection.rollback()
                continue

            self.db_connection.commit()
            logger.info('scraping of player %s complete', allycode)


class AllUnitStats:
    def __init__(self) -> None:
        self.stats = {}
        self.stats[1] = {'name': 'health', 'type': 'mediumint', 'gather': True}
        self.stats[2] = {'name': 'strength',
                         'type': 'smallint', 'gather': False}
        self.stats[3] = {'name': 'agility',
                         'type': 'smallint', 'gather': False}
        self.stats[4] = {'name': 'tactics',
                         'type': 'smallint', 'gather': False}
        self.stats[5] = {'name': 'speed', 'type': 'smallint', 'gather': True}
        self.stats[6] = {'name': 'phys_dmg',
                         'type': 'smallint', 'gather': True}
        self.stats[7] = {'name': 'special_dmg',
                         'type': 'smallint', 'gather': True}
        self.stats[8] = {'name': 'armor', 'type': 'smallint', 'gather': True}
        self.stats[9] = {'name': 'resistance',
                         'type': 'smallint', 'gather': True}
        self.stats[10] = {'name': 'arp_pen', 'type': 'float', 'gather': False}
        self.stats[11] = {'name': 'resist_pen',
                          'type': 'float', 'gather': False}
        self.stats[12] = {'name': 'dodge', 'type': 'float', 'gather': False}
        self.stats[13] = {'name': 'deflection',
                          'type': 'float', 'gather': False}
        self.stats[14] = {'name': 'phys_crit',
                          'type': 'tinyint', 'gather': True}
        self.stats[15] = {'name': 'special_crit',
                          'type': 'tinyint', 'gather': True}
        self.stats[16] = {'name': 'crit_dmg',
                          'type': 'tinyint', 'gather': True}
        self.stats[17] = {'name': 'potency', 'type': 'tinyint', 'gather': True}
        self.stats[18] = {'name': 'tenacity',
                          'type': 'tinyint', 'gather': True}
        self.stats[27] = {'name': 'hp_steal', 'type': 'float', 'gather': False}
        self.stats[28] = {'name': 'protection',
                          'type': 'mediumint', 'gather': True}
        self.stats[37] = {'name': 'phys_acc', 'type': 'float', 'gather': False}
        self.stats[38] = {'name': 'special_acc',
                          'type': 'float', 'gather': False}
        self.stats[39] = {'name': 'phys_ca', 'type': 'float', 'gather': False}
        self.stats[40] = {'name': 'special_ca',
                          'type': 'float', 'gather': False}

        self.misc = {}
        self.misc['us_id'] = {'type': 'int', 'gather': True}
        self.misc['us_allycode'] = {'type': 'int', 'gather': True}
        self.misc['us_gac_num'] = {'type': 'smallint', 'gather': True}
        self.misc['us_unit_id'] = {'type': 'smallint', 'gather': True}
        self.misc['us_date'] = {'type': 'int', 'gather': True}
        # other things to store
        # store combined gear_level + relic, 13= g13 r0, 15= r2 etc 22=r9
        # api returns 1 for g<13, rest:tier=2=relic0 3=1 4=2 5=3 6=4 7=5 8=6 9=7 10=8 11=9
        self.misc['relic_tier'] = {'type': 'tinyint', 'gather': True}
        self.misc['level'] = {'type': 'tinyint', 'gather': False}
        self.misc['gear_level'] = {'type': 'tinyint', 'gather': False}

    def raw_to_percent(self, raw_value):
        return (raw_value*100)/(raw_value + 85*7.5)

    def percent_to_raw(self, percent_value):
        return int(round((percent_value * 85 * 7.5)/(100 - percent_value), 0))

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


class Datacron:
    def __init__(self, dc_dict: Dictionary) -> None:
        self.dc_dict = dc_dict
        self.stats: dict
        self.abilities: dict
        self.level: int
        self.gac_num: int
        self.allycode: int
        self.stat_indices = (1, 2, 4, 5, 7, 8)
        self.ability_indices = (3, 6, 9)
        self.insert_datacron_query: str
        self.insert_columns_order: list
        self.prepare_queries()
        self.datacron_object: dict

    def prepare_queries(self):
        items = ['dc_allycode', 'dc_gac_num', 'dc_level']
        for level in self.ability_indices:
            items.append('dc_ability_' + str(level))
        for level in self.stat_indices:
            items.append('dc_stat_' + str(level))
        for level in self.stat_indices:
            items.append('dc_value_' + str(level))
        self.insert_columns_order = items

        header = ''
        for item in items:
            header += item + ', '
        header = header[:-2]  # chop last coma
        self.sql_columns_header = header

        query = 'insert into datacrons ('+header + ')\n'
        query += 'values (\n'
        for _ in items:
            query += '%s, '
        query = query[:-2]  # chop last coma
        query += ')'
        self.insert_datacron_query = query

    def get_sql_create_table(self):
        query = 'create table datacrons ( \n'
        query += '  dc_id int unsigned primary key auto_increment,\n'
        query += '  dc_allycode int unsigned,\n'
        query += '  dc_gac_num smallint unsigned,\n'
        query += '  dc_level tinyint unsigned,\n'
        for level in self.ability_indices:
            query += '  dc_ability_' + str(level) + ' smallint unsigned,\n'
        for level in self.stat_indices:
            query += '  dc_stat_' + str(level) + ' tinyint unsigned,\n'
        for level in self.stat_indices:
            query += '  dc_value_' + str(level) + ' tinyint unsigned ,\n'
        query = query[:-2]
        query += ' );'
        return query

    def reap_from(self, datacron_object) -> bool:
        self.datacron_object = datacron_object
        self.gac_num = ''
        self.allycode = ''
        self.stats = {}
        self.abilities = {}

        dco = self.datacron_object  # abbr
        # don't save crap
        if dco['tier'] == 0:
            return False

        self.level = dco['tier']
        level = 0
        for tier in dco['tiers']:
            level += 1
            if level in self.stat_indices:
                col_name_type = 'dc_stat_' + str(level)
                col_name_value = 'dc_value_' + str(level)
                self.stats[col_name_type] = tier['stat_type']
                stat_value = tier['stat_value']
                stat_value = int(stat_value*100)
                if stat_value > 255:  # limited to tinyint in db
                    stat_value = 255
                self.stats[col_name_value] = stat_value

            elif level in self.ability_indices:
                col_name_ability = 'dc_ability_' + str(level)
                value = self.dc_dict.to_int(tier['ability_description'])
                self.abilities[col_name_ability] = value
            else:
                logger.critical('datacron out of range')
                assert False
        return True

    def save_yourself_to_db(self, allycode, gac_num, cursor) -> bool:
        compact_stats = {}
        for item in self.insert_columns_order:
            if item in self.stats:
                compact_stats[item] = self.stats[item]
            elif item in self.abilities:
                compact_stats[item] = self.abilities[item]
            else:
                compact_stats[item] = None
        compact_stats['dc_allycode'] = allycode
        compact_stats['dc_gac_num'] = gac_num
        compact_stats['dc_level'] = self.level

        # print(compact_stats)
        # input()
        values = []
        for key in self.insert_columns_order:
            values.append(compact_stats[key])
        values = tuple(values)

        try:
            cursor.execute(self.insert_datacron_query, values)
            logger.debug('datacron inserted')
        except mysql.connector.Error:
            logger.error('insert datacron into table failed')
            traceback.print_exc()
            return False
        return True


class UnitStats:
    def __init__(self, unit_dict: Dictionary, zuo_dict: Dictionary) -> None:
        self.unit_dict = unit_dict
        self.zuo_dict = zuo_dict
        self.compact_stats: dict
        stats: dict
        self.zuo: list
        self.insert_unit_query = ''
        self.insert_zuo_query = ''

        self.template = {}
        # things to store

        self.template['us_allycode'] = {'type': 'int', 'gather': True}
        self.template['us_gac_num'] = {'type': 'smallint', 'gather': True}
        self.template['us_unit_id'] = {'type': 'smallint', 'gather': True}

        # store combined gear_level + relic, 13= g13 r0, 15= r2 etc 22=r9
        # api returns 1 for g<13, rest:tier=2=relic0 3=1 4=2 5=3 6=4 7=5 8=6 9=7 10=8 11=9
        self.template['relic_level'] = {'type': 'tinyint', 'gather': True}

        stats = {}
        self.template['stats'] = stats

        # health '1' and protection '28' divided by 10 and rounded to fit in smallint
        stats['1'] = {'name': 'health', 'type': 'smallint', 'gather': True}
        stats['5'] = {'name': 'speed', 'type': 'smallint', 'gather': True}
        stats['6'] = {'name': 'phys_dmg', 'type': 'smallint', 'gather': True}
        stats['7'] = {'name': 'special_dmg',
                      'type': 'smallint', 'gather': True}

        # convert to raw armor, store as small int, save 4
        stats['8'] = {'name': 'armor', 'type': 'smallint', 'gather': True}
        # skip resistance, it's tied to armor anyway
        #stats['9'] = {'name': 'resistance', 'type': 'smallint', 'gather': True}

        # stored as percents, rounded.  converted round(crit) from float
        stats['14'] = {'name': 'phys_crit', 'type': 'tinyint', 'gather': True}
        stats['15'] = {'name': 'special_crit',
                       'type': 'tinyint', 'gather': True}

        # stored as percent minus base 1.5, round((crit_dmg-1.5)*100). 50 = 200% from 2.0 float
        stats['16'] = {'name': 'crit_dmg', 'type': 'tinyint', 'gather': True}

        # stored as percent round(potency *100) 150 = 150% from 1.5 float
        stats['17'] = {'name': 'potency', 'type': 'tinyint', 'gather': True}
        stats['18'] = {'name': 'tenacity', 'type': 'tinyint', 'gather': True}

        stats['39'] = {'name': 'phys_ca', 'type': 'tinyint', 'gather': True}
        stats['28'] = {'name': 'protection',
                       'type': 'smallint', 'gather': True}

    def reap_from(self, unit_object) -> bool:
        data = unit_object['data']
        stats = data['stats']
        self.zuo = []
        self.compact_stats = {}

        # only units!
        if data['combat_type'] != 1:
            self.compact_stats = -1
            return False

        self.compact_stats['us_allycode'] = ''
        self.compact_stats['us_gac_num'] = ''
        self.compact_stats['us_unit_id'] = self.unit_dict.to_int(
            data['base_id'])
        if data['relic_tier'] == 1:
            self.compact_stats['relic_level'] = data['gear_level']
        else:
            self.compact_stats['relic_level'] = data['gear_level'] + \
                data['relic_tier'] - 2

        # stats in json are number-strings '1' to '40'
        # self.template['stats'] has those number-strings as keys
        # as well as their full names and type definitions
        for key, value in self.template['stats'].items():
            # armor, resistance to flat (float to smallint)
            if key in ['8', '9']:
                self.compact_stats[value['name']] = round(
                    (stats[key]*85*7.5)/(100-stats[key]))
            # physcrit, speccrit round float to tinyint
            elif key in ['14', '15']:
                self.compact_stats[value['name']] = round(stats[key])
            # potency, tenacity, phys_ca, float percent to tinyint
            elif key in ['17', '18']:
                self.compact_stats[value['name']] = round(stats[key]*100)
            #hp and prot
            elif key in ['1', '28']:
                self.compact_stats[value['name']] = round(stats[key]/10)
            # Crit dmg normalized to 1byte
            elif key == '16':
                self.compact_stats[value['name']] = round(
                    stats[key] * 100 - 150)
            #whatever is left
            else:
                self.compact_stats[value['name']] = int(stats[key])

        if data['has_ultimate']:
            self.zuo.append(self.zuo_dict.to_int('ult_' + data['base_id']))

        for zeta in data['zeta_abilities']:
            if zeta == 'uniqueskill_GALACTICLEGEND01':  # kludge for repeated 6th zeta for all gl
                self.zuo.append(self.zuo_dict.to_int(
                    'zeta_' + zeta + '_' + data['base_id']))
            else:
                self.zuo.append(self.zuo_dict.to_int('zeta_' + zeta))

        for omicron in data['omicron_abilities']:
            self.zuo.append(self.zuo_dict.to_int('omi_' + omicron))

        return True

    def get_sql_create_table(self):
        query = ''
        query += 'create table unit_stats ( \n'
        for key, value in self.template.items():
            if key == 'stats':
                for x in value:
                    if value[x]['gather']:
                        query += value[x]['name'] + \
                            ' ' + value[x]['type'] + ' unsigned not null ,\n'
            else:
                if value['gather']:
                    query += key + ' ' + \
                        value['type'] + ' unsigned not null,\n'

        query += 'PRIMARY KEY (us_allycode, us_gac_num, us_unit_id)'
        query += '\n);'
        return query

    def get_sql_table_header(self):
        header = ''
        for key, value in self.template.items():
            if key == 'stats':
                for x in value:
                    if value[x]['gather']:
                        header += ', ' + value[x]['name']
            else:
                if value['gather']:
                    header += ', ' + key
        header = header[1:]  # cut first coma
        return header

    def save_yourself_to_db(self, allycode, gac_num, cursor) -> bool:
        self.compact_stats['us_allycode'] = allycode
        self.compact_stats['us_gac_num'] = gac_num
        if not self.insert_unit_query:
            query = 'insert ignore into unit_stats (\n'
            for key in self.compact_stats:
                query += key + ', '
            query = query[:-2]  # chop last ', '
            query += ') values (\n'
            for key in self.compact_stats:
                query += '%s, '
            query = query[:-2]  # chop last ', '
            query += ')'
            self.insert_unit_query = query

        # print(self.insert_unit_query)
        # print(self.compact_stats)
        values = []
        for key in self.compact_stats:
            values.append(self.compact_stats[key])
        values = tuple(values)

        try:
            cursor.execute(self.insert_unit_query, values)
        except mysql.connector.Error:
            traceback.print_exc()
            logger.critical('unit insert failed')
            return False

        if not self.insert_zuo_query:
            query = 'insert into zuo_bundle (\n'
            query += 'zuo_dict_id, zuo_allycode, zuo_gac_num \n'
            query += ') values ( %s, %s, %s) '
            self.insert_zuo_query = query

        for zuo_item in self.zuo:
            try:
                cursor.execute(self.insert_zuo_query,
                               (zuo_item, allycode, gac_num))
            except mysql.connector.Error:
                traceback.print_exc()
                logger.critical('zuo insert failed')
                return False
        return True


def main():
    #pikappa = 479536574
    gac_num = 113
    job_table = '_job_scan_players'

    api = SwgohGgApi(gac_num)

    # print()
    # print()
    # a_datacron = Datacron(api.dc_dict)
    # print(a_datacron.get_sql_create_table())
    # unit_stats = UnitStats(api.unit_dict, api.zuo_dict)
    # print(unit_stats.get_sql_create_table())
    # input()

    api.scrape_players(job_table)
    logger.info('scraping complete')

    print(
        f'scraping complete, check db if there were any jobs omitted (codes left in {job_table}) ')
    # input()


if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
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
        description='tool to scrape players units and datacrons for further analysis')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    start = time.time()
    main()
    end = time.time()
    total_time = end - start
    print("\nExecution time: " + str(total_time))
