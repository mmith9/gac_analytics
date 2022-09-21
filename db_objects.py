import logging

import os
import sys
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

        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                user=mysql_user,
                password=mysql_password,
                database=mysql_database_name
            )
            self.cursor = self.connection.cursor()
        except mysql.connector.Error:
            logger.critical('Connection to db failed')
            sys.exit(1)

    def get_info(self):
        print('logger debug')
        print('module db_objects, get_info() call')
        print(f'__name__ is {__name__}')
        print('got logger', logger)
        print('testing')
        print('logging crit')
        logger.critical('a crit msg')
        print('logging error')
        logger.error('an error msg')
        print('logging warning')
        logger.warning('a warning msg')
        print('logging info')
        logger.info('an info msg')
        print('logging debug')
        logger.debug('a debug msg')
        print('done testing logger')
        logger.info('Back to work now, fetching db info')
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
            self.cursor.execute(query_jobs + tablename)
            rows = self.cursor.fetchall()
            self.info[tablename]['jobs'] = rows[0][0]

            if 'gac_num_name' in self.info[tablename]:
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

                    self.cursor.execute(query_last_gac_entries)
                    rows = self.cursor.fetchall()
                    row = rows[0]
                    self.info[tablename]['last_gac_entries'] = row[0]

                query_all_gac_nums = 'select distinct ' + \
                    self.info[tablename]['gac_num_name']
                query_all_gac_nums += ' from ' + tablename
                self.cursor.execute(query_all_gac_nums)
                rows = self.cursor.fetchall()
                self.info[tablename]['all_gac_nums'] = []
                for row in rows:
                    self.info[tablename]['all_gac_nums'].append(row[0])

        query_battle_id = 'select max(battle_id) from battles'
        self.cursor.execute(query_battle_id)
        rows = self.cursor.fetchall()
        row = rows[0]
        self.info['battles']['last_battle_id'] = row[0]

        query_battle_id = 'select max(pcp_battle_id) from participants'
        self.cursor.execute(query_battle_id)
        rows = self.cursor.fetchall()
        row = rows[0]
        self.info['participants']['last_battle_id'] = row[0]

        return self.info
