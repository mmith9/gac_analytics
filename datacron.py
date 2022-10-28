import logging
import traceback
import mysql.connector
from gac_dictionaries import DictionaryPlus

logger = logging.getLogger(__name__)
from pprint import PrettyPrinter
pp=PrettyPrinter()

class Datacron:
    def __init__(self, dc_dict: DictionaryPlus) -> None:
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
        if dco['tier'] < 3:
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
            pp.pprint(self.level)
            pp.pprint(self.stats)
            pp.pprint(self.abilities)
            traceback.print_exc()
            return False
        return True

