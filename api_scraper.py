import sys
import json
import logging
import re
import traceback
import requests
import mysql.connector

import throttling
from gac_dictionaries import DictionaryPlus
from gac_dictionaries import LocalDictionary
from datacron import Datacron
from all_unit_stats import AllUnitStats
from unit_stats import UnitStats
from db_objects import MyDb

logger = logging.getLogger(__name__)


class SwgohGgApi:
    def __init__(self, my_db: MyDb, gac_num) -> None:
        self.cursor = my_db.cursor
        self.db_connection = my_db.connection
        self.unit_dict = DictionaryPlus(
            'unit_dict', self.cursor, self.db_connection)
        self.dc_dict = DictionaryPlus(
            'dc_mechanics_dict', self.cursor, self.db_connection)
        self.zuo_dict = DictionaryPlus(
            'zuo_dict', self.cursor, self.db_connection)
        self.stats_dict = LocalDictionary()
        self.gac_generate_num = gac_num
        self.rate_counter = throttling.RateCounter()
        self.rate_limiter: throttling.RateLimiter

        all_stats = AllUnitStats()
        self.stats_dict.update_from_dict(all_stats.stats)

    def scrape_player(self, allycode):
        url = 'http://api.swgoh.gg/player/' + str(allycode) + '/'
        try:
            response = requests.get(url)
        except ConnectionError:
            logger.warning('connection error while fetching %s', allycode)
            self.rate_counter.request_errored()
            return False

        try:
            player = json.loads(response.text)
        except json.decoder.JSONDecodeError:
            logger.error('failed to decode response code: %s',
                          response.status_code)
            self.rate_counter.request_failed()
            return False

        self.rate_counter.request_served()
        return player

    def scrape_players(self) -> bool:
        self.rate_limiter = throttling.RateLimiter(0)
        query = 'select allycode, gac_num from _job_scan_units order by id'
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

        jobs = []
        for row in rows:
            job = {'allycode':row[0], 'gac_num':row[1]}
            jobs.append(job)

        a_datacron = Datacron(self.dc_dict)
        unit_stats = UnitStats(self.unit_dict, self.zuo_dict)

        for job in jobs:
            is_error = False
            self.rate_limiter.wait_rate_limit()
            logger.debug('starting scraping player %s', job['allycode'])
            player = self.scrape_player(job['allycode'])
            if not player:
                logger.error('Failed to scrape player %s', job['allycode'])
                continue

            for unit in player['units']:
                if not unit_stats.reap_from(unit):
                    logger.debug('unit reap failed - ship')
                    continue

                if unit_stats.save_yourself_to_db(job['allycode'], job['gac_num'], self.cursor):
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
                if a_datacron.save_yourself_to_db(job['allycode'], job['gac_num'], self.cursor):
                    logger.debug('datacron saved to db')
                else:
                    logger.error('failed to save datacron to db')
                    is_error = True
                    break

            if is_error:
                logger.error('aborting player %s', job['allycode'])
                self.db_connection.rollback()
                continue

            logger.debug('scraped player %s', job['allycode'])

            query = 'delete from _job_scan_units where allycode = %s'
            try:
                self.cursor.execute(query, (job['allycode'],))
            except mysql.connector.Error:
                logger.error('failed to remove used allycode from db')
                is_error = True
                self.db_connection.rollback()
                continue

            self.db_connection.commit()
            logger.info('scraping of player %s complete', job['allycode'])
            self.rate_counter.log_rates()
        return True


    def prepare_jobs_scan_units(self):
        query = 'truncate table _job_scan_units'
        self.cursor.execute(query)

        query = 'insert into _job_scan_units (allycode, gac_num) '
        query+= 'select distinct ta_allycode, ' + str(self.gac_generate_num) + ' '
        query+= 'from top_allycodes order by rating desc'

        self.cursor.execute(query)
        self.db_connection.commit()

    def scrape_gac_events(self):
        print('bar')
        query = 'select ge_id, swgohgg_gac_season, swgohgg_gac_num, '
        query+= '    cg_event_id, cg_territory_map_id, cg_base_id '
        query+= 'from gac_events where cg_base_id is null'

        self.cursor.execute(query)
        db_rows = self.cursor.fetchall()
        logger.debug('loaded %s rows from db', len(db_rows))

        url = 'http://api.swgoh.gg/gac/events/'
        try:
            response = requests.get(url)
        except mysql.connector.Error:
            logger.critical(
                'failed to get events from api.swgoh.gg/gac/events')
            sys.exit(1)
        try:
            gac_events = json.loads(response.text)
        except UnicodeDecodeError:
            logger.critical('failed to decode json object')
            sys.exit(1)
        logger.debug('loaded %s gac objects from api', len(gac_events))

        for row in db_rows:
            gac_id = row[0]
            gac_season = row[1]

            pattern = r'CHAMPIONSHIPS_GRAND_ARENA_GA2_EVENT_SEASON_([0-9]+)'
            for item in gac_events:
                matching = re.search(pattern, item['game_event']['base_id'])
                if matching:

                    if gac_season == int(matching.group(1)):
                        query = 'update gac_events set '
                        query += 'cg_base_id = %s, cg_territory_map_id = %s '
                        query += 'where ge_id = %s'

                        self.cursor.execute(\
                            query,
                            (item['base_id'],item['game_event']['territory_map_id'], gac_id))
                        self.db_connection.commit()
                        break
