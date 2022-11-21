import logging

import os
import sys
import sqlite3
import traceback
from sqlite3 import DatabaseError as sqlite_error
from mysql.connector import DatabaseError as mysql_error
import mysql.connector
from datacron_v2 import AllDatacronStats, DatacronV2



logger = logging.getLogger(__name__)


class MyDb:
    def __init__(self) -> None:
        self.cursor: mysql.connector.cursor.MySQLCursor
        self.connection: mysql.connector.connection.MySQLConnection
        self.info = {}

    def connect(self):
        mysql_database_name = "swgoh_gac"
        mysql_user = os.environ.get("mysql_user")
        mysql_password = os.environ.get("mysql_password")
        mysql_host = os.environ.get("mysql_host")

        try:
            self.connection = mysql.connector.connect(
                host=mysql_host,
                user=mysql_user,
                password=mysql_password,
                database=mysql_database_name
            )
            self.cursor = self.connection.cursor()
        except mysql.connector.Error:
            logger.critical('Connection to db failed')
            traceback.print_exc()
            sys.exit(1)

    def get_info(self):

        logger.info('fetching db info')
        self.info = {}
        job_tables = ['_job_scan_units',
                      '_job_scan_battles', '_job_scan_leaderboards']
        data_tables = ['battles', 'unit_stats', 'top_allycodes',
                       'zuo_bundle', 'datacrons', 'participants']

        for table in data_tables + job_tables:
            self.info[table] = {}
        self.info['battles']['gac_num_name'] = 'bt_gac_num'
        self.info['unit_stats']['gac_num_name'] = 'us_gac_num'
        self.info['zuo_bundle']['gac_num_name'] = 'zuo_gac_num'
        self.info['datacrons']['gac_num_name'] = 'dc_gac_num'
        self.info['top_allycodes']['gac_num_name'] = 'ta_gac_num'
        self.info['_job_scan_units']['gac_num_name'] = 'gac_num'
        self.info['_job_scan_battles']['gac_num_name'] = 'gac_num'

        query_jobs = 'select count(*) from '
        query_gac_num = 'select min(gac_num), max(gac_num) from '

        for tablename in job_tables:
            logger.debug('Querying: %s', query_jobs + tablename)
            self.cursor.execute(query_jobs + tablename)
            rows = self.cursor.fetchall()
            self.info[tablename]['jobs'] = rows[0][0]

            if 'gac_num_name' in self.info[tablename]:
                logger.debug('Querying: %s', query_gac_num + tablename)
                self.cursor.execute(query_gac_num + tablename)
                rows = self.cursor.fetchall()
                row = rows[0]
                if row[0] != row[1]:
                    logger.critical('different gac_nums in %s', tablename)
                    assert False
                self.info[tablename]['gac_num'] = row[0]

        for tablename in data_tables:
            if 'gac_num_name' in self.info[tablename]:
                query_last_gac_num = 'select max(' + \
                    self.info[tablename]['gac_num_name']
                query_last_gac_num += ') from ' + tablename
                logger.debug('Querying: %s', query_last_gac_num)
                self.cursor.execute(query_last_gac_num)
                rows = self.cursor.fetchall()
                row = rows[0]
                self.info[tablename]['last_gac_num'] = row[0]

                if self.info[tablename]['last_gac_num']:
                    query_last_gac_entries = 'select count(*) from ' + \
                        tablename
                    query_last_gac_entries += ' where ' + \
                        self.info[tablename]['gac_num_name']
                    query_last_gac_entries += ' = ' + \
                        str(self.info[tablename]['last_gac_num'])
                    logger.debug('Querying: %s', query_last_gac_entries)
                    self.cursor.execute(query_last_gac_entries)
                    rows = self.cursor.fetchall()
                    row = rows[0]
                    self.info[tablename]['last_gac_entries'] = row[0]

                query_all_gac_nums = 'select distinct ' + \
                    self.info[tablename]['gac_num_name']
                query_all_gac_nums += ' from ' + tablename
                logger.debug('Querying: %s', query_all_gac_nums)
                self.cursor.execute(query_all_gac_nums)
                rows = self.cursor.fetchall()
                self.info[tablename]['all_gac_nums'] = []
                for row in rows:
                    self.info[tablename]['all_gac_nums'].append(row[0])

        query_battle_id = 'select max(battle_id) from battles'
        logger.debug('Querying: %s', query_battle_id)
        self.cursor.execute(query_battle_id)
        rows = self.cursor.fetchall()
        row = rows[0]
        self.info['battles']['last_battle_id'] = row[0]

        query_battle_id = 'select max(pcp_battle_id) from participants'
        logger.debug('Querying: %s', query_battle_id)
        self.cursor.execute(query_battle_id)
        rows = self.cursor.fetchall()
        row = rows[0]
        self.info['participants']['last_battle_id'] = row[0]

        return self.info


class LocalDb:
    def __init__(self) -> None:
        self.connection: sqlite3.Connection
        self.cursor: sqlite3.Cursor

    def connect(self, filename) -> bool:
        try:
            self.connection = sqlite3.connect(filename)
        except sqlite3.DatabaseError:
            logger.critical('db connection failed')
            return False

        self.cursor = self.connection.cursor()
        return True

    def copy_scrape_jobs(self, my_db: MyDb):
        query = 'select id, allycode, gac_num from _job_scan_battles order by id'
        my_db.cursor.execute(query)
        rows = my_db.cursor.fetchall()
        gac_num = rows[0][2]
        query = 'insert or ignore into local_job_scan_battles values (?, ?, ?)'
        self.cursor.executemany(query, rows)
        self.connection.commit()

        query = 'select unit_id, name, base_id, image_url from unit_dict'
        my_db.cursor.execute(query)
        rows = my_db.cursor.fetchall()
        query = 'insert or ignore into unit_dict values (?, ?, ?, ?) '
        self.cursor.executemany(query, rows)
        self.connection.commit()

        query = 'select dc_mc_id, mc_string from dc_mechanics_dict'
        my_db.cursor.execute(query)
        rows = my_db.cursor.fetchall()
        query = 'insert or ignore into dc_mechanics_dict values (?, ?) '
        self.cursor.executemany(query, rows)
        self.connection.commit()

        query = 'select distinct us_allycode, us_gac_num from unit_stats '\
                'where us_gac_num = %s'
        my_db.cursor.execute(query, (gac_num,))
        rows = my_db.cursor.fetchall()
        query = 'insert or ignore into local_snapped_allycodes values (?, ?) '
        self.cursor.executemany(query, rows)
        self.connection.commit()

    def pump_data_to_main_db(self, my_db: MyDb):
        a_dc = DatacronV2Db()
        a_battle = GacPlayerBattleDb()

        query = 'select * from local_battles'
        self.cursor.execute(query)
        battle_rows = self.cursor.fetchall()

        for battle_row in battle_rows:
            attacker_dc_id = battle_row[1]
            defender_dc_id = battle_row[2]
            a_battle.db_row = list(battle_row[1:])

            if attacker_dc_id:
                a_dc.load_self_from_db(attacker_dc_id, self.cursor, 'sqlite')
                if not a_dc.db_row:
                    logger.critical('failed to find datacron id: %s', attacker_dc_id)
                new_id = a_dc.find_id_in_db(my_db.cursor, 'mysql')
                if not new_id:
                    new_id = a_dc.insert_self_to_db(my_db.cursor, 'mysql')
                a_battle.db_row[0] = new_id

            if defender_dc_id:
                a_dc.load_self_from_db(defender_dc_id, self.cursor, 'sqlite')
                new_id = a_dc.find_id_in_db(my_db.cursor, 'mysql')
                if not new_id:
                    new_id = a_dc.insert_self_to_db(my_db.cursor, 'mysql')
                a_battle.db_row[1] = new_id

            logger.debug('pumping to db values %s', a_battle.db_row)
            a_battle.insert_self_to_db(my_db.cursor, 'mysql')
            my_db.connection.commit()


    def initialize_db(self):

        for table in ['local_jobs_completed', 'local_job_scan_battles',
                      'local_snapped_allycodes', 'local_battles', 'unit_dict',
                      'datacronsv2', 'dc_mechanics_dict']:
            query = 'drop table ' + table
            try:
                self.cursor.execute(query)
                self.connection.commit()
            except sqlite3.DatabaseError as error:
                logger.info('error on drop table %s', error)

        query = 'create table local_jobs_completed ('
        query += 'id integer primary key autoincrement, '
        query += 'allycode int unsigned not null, '
        query += 'gac_num int unsigned not null)'
        self.cursor.execute(query)

        query = 'create table local_job_scan_battles ('
        query += 'id int unsigned not null primary key, '
        query += 'allycode int unsigned not null, '
        query += 'gac_num int unsigned not null)'
        self.cursor.execute(query)

        query = 'create table local_snapped_allycodes ('
        query += 'allycode int unsigned not null, '
        query += 'gac_num int unsigned not null)'
        self.cursor.execute(query)

        query = '''
            CREATE TABLE `local_battles` (
            `battle_id` integer PRIMARY KEY AUTOINCREMENT,
            `attacker_dc_id` int,
            `defender_dc_id` int,
            `attacker` int unsigned not null,
            `defender` int unsigned not null,
            `banners` tinyint unsigned not null,
            `bt_date` int unsigned not null,
            `duration` smallint unsigned not null,
            `attempt` tinyint unsigned not null,
            `bt_gac_num` smallint unsigned not null,
            `a1` smallint unsigned not null,
            `a2` smallint unsigned default null,
            `a3` smallint unsigned default null,
            `a4` smallint unsigned default null,
            `a5` smallint unsigned default null,
            `d1` smallint unsigned default null,
            `d2` smallint unsigned default null,
            `d3` smallint unsigned default null,
            `d4` smallint unsigned default null,
            `d5` smallint unsigned default null,
            `d1_hpleft` tinyint unsigned default null,
            `d2_hpleft` tinyint unsigned default null,
            `d3_hpleft` tinyint unsigned default null,
            `d4_hpleft` tinyint unsigned default null,
            `d5_hpleft` tinyint unsigned default null
            )'''
        self.cursor.execute(query)

        query = '''
            CREATE TABLE `unit_dict` (
            `unit_id` smallint unsigned PRIMARY KEY,
            `name` varchar(255),
            `base_id` varchar(255),
            `image_url` varchar(255)
            )'''
        self.cursor.execute(query)

        query = '''
            CREATE TABLE `dc_mechanics_dict` (
            `dc_mc_id` integer PRIMARY KEY AUTOINCREMENT,
            `mc_string` text
            )'''
        self.cursor.execute(query)

        a_dc = DatacronV2()
        query = a_dc.get_sql_create_table(dbtype='sqlite')
        print(query)
        self.cursor.execute(query)


class DatacronV2Db:
    def __init__(self) -> None:
        self.db_row: list
        self.mysql_insert_query: str
        self.sqlite_insert_query: str
        self.dcstats = AllDatacronStats()
        self.construct_insert_queries()

    def construct_insert_queries(self):
        insert_query = 'insert into datacronsv2 ( '
        insert_values = ''

        for ability in self.dcstats.ability_indices:
            insert_query += 'dc_ability_' + str(ability) + ', '
            insert_values += '%s, '

        for stat in self.dcstats.stat_indices:
            insert_query += 'dc_stat_' + str(stat) + ', '
            insert_values += '%s, '

        insert_query += ' level) values ('
        insert_values += ' %s)'
        insert_query += insert_values

        self.mysql_insert_query = insert_query
        self.sqlite_insert_query = insert_query.replace('%s', '?')

        logger.debug('mysql_insert_query: %s', self.mysql_insert_query)
        logger.debug('sqlite_insert_query: %s', self.sqlite_insert_query)

        return

    def load_self_from_db(self, dc_id: int, cursor, dbtype: str) -> bool:
        query = 'select * from datacrons_v2 where dc_id='
        if dbtype == 'mysql':
            query += '%s'
        elif dbtype == 'sqlite':
            query += '?'
        else:
            logger.critical('unknown db type')
            return False
        try:
            cursor.execute(query, (dc_id,))
            rows = cursor.fetchall()
        except (mysql_error, sqlite_error) as err:
            logger.error('failed to load dc from db, id: %s', dc_id)
            logger.error('db error: %s', err)
            return False
        if not rows:
            logger.error('failed to load dc from db, id: %s', dc_id)
            return False

        self.db_row = []
        for value in rows[0][1:]:
            if isinstance(value, float):
                self.db_row.append(round(value, 5))                
            else:
                self.db_row.append(value)

        return True

    def find_id_in_db(self, cursor, dbtype: str) -> int:
        if not self.db_row or dbtype not in ['mysql', 'sqlite']:
            return False

        find_query = 'select * from datacronsv2 where '
        idx = 0
        db_row_trunc = []

        for ability in self.dcstats.ability_indices:
            find_query += 'dc_ability_' + str(ability)
            if self.db_row[idx]:
                find_query+= '= %s and '
                db_row_trunc.append(self.db_row[idx])
            else:
                find_query += ' is null and '
            idx += 1

        for stat in self.dcstats.stat_indices:
            find_query += 'dc_stat_' + str(stat)
            if self.db_row[idx]:
                find_query += ' =%s and '
                db_row_trunc.append(self.db_row[idx])
            else:
                find_query += ' is null and ' 
            idx += 1

        find_query += ' level=%s'
        db_row_trunc.append(self.db_row[idx])
        logger.debug('mysql_find_query: %s', find_query)

        try:
            if dbtype == 'mysql':
                cursor.execute(find_query, db_row_trunc)
            else:
                find_query = find_query.replace('%s','?')
                cursor.execute(find_query, db_row_trunc)
            rows = cursor.fetchall()
        except (mysql_error, sqlite_error) as err:
            logger.error('find dc in db error: %s', err)
            return False

        if not rows:
            logger.debug('dc not found in db %s', dbtype)
            return False

        logger.debug('found dc in db, dc_id: %s', rows[0][0])
        return rows[0][0]

    def insert_self_to_db(self, cursor, dbtype: str) -> int:
        if not self.db_row or dbtype not in ['mysql', 'sqlite']:
            return False
        logger.debug('inserting dc to db: %s', self.db_row)
        try:
            if dbtype == 'mysql':
                cursor.execute(self.mysql_insert_query, self.db_row)
            else:
                cursor.execute(self.sqlite_insert_query, self.db_row)
        except (mysql_error, sqlite_error) as err:
            logger.error('insert dc to db error: %s', err)
            return False
        logger.debug('inserted, dc_id: %s', cursor.lastrowid)
        return cursor.lastrowid


class GacPlayerBattleDb:
    def __init__(self) -> None:
        self.db_row: list
        self.mysql_insert_query: str
        self.mysql_create_table: str
        self.construct_queries()

    def construct_queries(self):
        self.mysql_create_table = '''
        CREATE TABLE battlesv2 (
battle_id int unsigned PRIMARY KEY AUTO_INCREMENT,
attacker_dc_id int unsigned,
defender_dc_id int unsigned,
attacker int unsigned not null,
defender int unsigned not null,
banners tinyint unsigned default null,
bt_date int unsigned not null,
duration smallint unsigned not null,
attempt tinyint unsigned not null,
bt_gac_num smallint unsigned not null,
a1 smallint unsigned not null,
a2 smallint unsigned default null,
a3 smallint unsigned default null,
a4 smallint unsigned default null,
a5 smallint unsigned default null,
d1 smallint unsigned default null,
d2 smallint unsigned default null,
d3 smallint unsigned default null,
d4 smallint unsigned default null,
d5 smallint unsigned default null,
d1_hpleft tinyint unsigned default null,
d2_hpleft tinyint unsigned default null,
d3_hpleft tinyint unsigned default null,
d4_hpleft tinyint unsigned default null,
d5_hpleft tinyint unsigned default null,
index idx_attacker_dc_id(attacker_dc_id),
index idx_defender_dc_id(defender_dc_id),
index idx_banners(banners),
index idx_bt_gac_num(bt_gac_num),

index idx_a1(a1),
index idx_a2(a2),
index idx_a3(a3),
index idx_a4(a4),
index idx_a5(a5),

index idx_b1(b1),
index idx_b2(b2),
index idx_b3(b3),
index idx_b4(b4),
index idx_b5(b5)
)
        '''

        battle_columns = [
            'attacker_dc_id', 'defender_dc_id',
            'attacker', 'defender',
            'banners', 'bt_date', 'duration', 'attempt', 'bt_gac_num',
            'a1', 'a2', 'a3', 'a4', 'a5',
            'd1', 'd2', 'd3', 'd4', 'd5',
            'd1_hpleft', 'd2_hpleft', 'd3_hpleft', 'd4_hpleft', 'd5_hpleft'
        ]

        query = 'insert into battlesv2 ('
        values = ''
        for col in battle_columns:
            query += col + ', '
            values += '%s, '
        query = query[:-2]
        values = values[:-2]

        query += ') values ('
        query += values + ')'

        self.mysql_insert_query = query

        logger.debug('mysql_insert_query: %s', self.mysql_insert_query)

        return

    def insert_self_to_db(self, cursor, dbtype: str) -> bool:
        if not self.db_row or dbtype not in ['mysql', 'sqlite']:
            return False
        logger.debug('inserting battle to db: %s',self.db_row)
        try:
            if dbtype == 'mysql':
                cursor.execute(self.mysql_insert_query, self.db_row)
            else:
                #cursor.execute(self.sqlite_insert_query, self.db_row)
                logger.critical('sqlite not ready yet')
        except (mysql_error, sqlite_error) as err:
            logger.error('insert dc to db error: %s', err)
            return False
