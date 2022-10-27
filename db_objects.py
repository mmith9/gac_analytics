import logging

import os
import sys
import sqlite3
import traceback
import mysql.connector


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

        query = 'select distinct us_allycode, us_gac_num from unit_stats '\
                'where us_gac_num = %s'
        my_db.cursor.execute(query, (gac_num,))
        rows = my_db.cursor.fetchall()
        query = 'insert or ignore into local_snapped_allycodes values (?, ?) '
        self.cursor.executemany(query, rows)
        self.connection.commit()

    def initialize_db(self):

        for table in ['local_jobs_completed', 'local_job_scan_battles',
                      'local_snapped_allycodes', 'local_battles', 'unit_dict']:
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
            `attacker` int unsigned not null,
            `defender` int unsigned not null,
            `banners` tinyint unsigned not null,
            `bt_date` int unsigned not null,
            `duration` smallint unsigned not null,
            `bt_gac_num` smallint unsigned not null,
            `a1` smallint unsigned not null,
            `a2` smallint unsigned not null,
            `a3` smallint unsigned not null,
            `a4` smallint unsigned not null,
            `a5` smallint unsigned not null,
            `d1` smallint unsigned not null,
            `d2` smallint unsigned not null,
            `d3` smallint unsigned not null,
            `d4` smallint unsigned not null,
            `d5` smallint unsigned not null
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
