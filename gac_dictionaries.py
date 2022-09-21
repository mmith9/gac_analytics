import logging
import json
import traceback
import requests
import mysql.connector

logger = logging.getLogger(__name__)

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
            logger.critical(
                'phrase not found in dict:%s phrase:%s', self.table, dict_str)
            return False

# this can also update db with new units


class DictionaryPlus:
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
