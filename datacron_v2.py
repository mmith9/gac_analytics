import json
import logging
from sqlite3 import DatabaseError as sqlite_error
from mysql.connector import DatabaseError as mysql_error

from bs4 import Tag

from gac_dictionaries import DictionaryPlus
logger = logging.getLogger(__name__)


class AllDatacronStats:
    def __init__(self) -> None:
        self.stats = {}
        self.stats[16] = {'name': 'crit dmg %', 'type': 'float'}  # 16 (cd?)
        self.stats[17] = {'name': 'potency', 'type': 'float'}  # 17 (potency?)
        self.stats[18] = {'name': 'tenacity',
                          'type': 'float'}  # 18 (tenacity?)
        self.stats[19] = {'name': 'dodge', 'type': 'float'}  # 19 dodge
        self.stats[20] = {'name': 'deflection',
                          'type': 'float'}  # 20 deflection
        self.stats[21] = {'name': 'phys crit %',
                          'type': 'float'}  # 21 phys crit
        self.stats[22] = {'name': 'special crit %',
                          'type': 'float'}  # 22 special crit
        self.stats[23] = {'name': 'armor %', 'type': 'float'}  # 23 armor
        self.stats[24] = {'name': 'resistance %',
                          'type': 'float'}  # 24 resistance
        self.stats[25] = {'name': 'armor penetration %',
                          'type': 'float'}  # 25 arpen
        self.stats[26] = {'name': 'resistance pentration %',
                          'type': 'float'}  # 26 respen
        self.stats[27] = {'name': 'hp steal %',
                          'type': 'float'}  # 27 (hp steal?)
        self.stats[31] = {'name': 'phys dmg %',
                          'type': 'float'}  # 31 physical damage
        self.stats[32] = {'name': 'special dmg %',
                          'type': 'float'}  # 32 special damage
        self.stats[33] = {'name': 'phys acc %',
                          'type': 'float'}  # 33 phys accuracy
        self.stats[34] = {'name': 'special acc %',
                          'type': 'float'}  # 34 special accuracy
        self.stats[35] = {'name': 'phys ca %', 'type': 'float'}  # 35 phys ca
        self.stats[36] = {'name': 'special ca %',
                          'type': 'float'}  # 36 special ca
        self.stats[55] = {'name': 'hp %', 'type': 'float'}  # 55 (hp?)
        self.stats[56] = {'name': 'prot %', 'type': 'float'}  # 56 (prot?)
        self.ability_indices = (3, 6, 9)
        self.stat_indices = [x for x in self.stats]


class DatacronV2:
    def __init__(self) -> None:
        self.stats: dict
        self.abilities: dict
        self.level: int
        self.datacron_element: Tag

    def reap_from(self, datacron_element: Tag) -> bool:
        self.datacron_element = datacron_element
        dco = self.datacron_element
        dco_ = dco.find(class_='datacron-icon')
        if not dco_:
            return False
        if dco_.has_attr('data-player-datacron-tooltip-app'):
            dco = dco_['data-player-datacron-tooltip-app']
        else:
            return False
        try:
            dco_ = json.loads(dco)
        except json.JSONDecodeError:
            logger.error('failed to process json: %s', dco)
        stats = dco_['derived']['accumulated_stats']
        tiers = dco_['derived']['tiers']
        self.level = dco_['derived']['tier']

        self.stats = {}
        self.abilities = {}
        for stat in stats:
            self.stats[stat['stat_type']] = stat['stat_value']
        for tier in tiers:
            if not tier['derived']['has_data']:
                break
            if tier['derived']['tier_id'] in (3, 6, 9):
                assert tier['derived']['tier_id'] <= self.level
                ability = tier['derived']['ability_description']
                self.abilities[tier['derived']['tier_id']] = ability

        return True


# <object>\derived\tiers\<element #2>\derived\ability_description
#        name = self.unit_element.find(class_='character-portrait__img')
# <div class="datacron-icon datacron-icon--size-sm" data-player-datacron-tooltip-app= JSON DICT

    def save_yourself_to_db(self, cursor, dc_dict: DictionaryPlus, dbtype):
        query = 'insert into datacronsv2 ('
        values = []
        placeholders = ''
        if dbtype == 'mysql':
            placeholder = '%s'
        elif dbtype == 'sqlite':
            placeholder = '?'
        else:
            logger.critical('unknown dbtype')
            assert False

        for key, value in self.abilities.items():
            query += 'dc_ability_' + str(key)+', '
            values.append(dc_dict.to_int(value))
            placeholders += placeholder+', '
        for key, value in self.stats.items():
            query += 'dc_stat_' + str(key)+', '
            values.append(value)
            placeholders += placeholder + ', '

        query += 'level) values ('+placeholders+placeholder+')'
        values.append(self.level)
        logger.debug('conocted query: %s', query)
        try:
            cursor.execute(query, values)
        except (mysql_error, sqlite_error):
            logger.error('failed to save datacron to db')
            return False
        datacron_id = cursor.lastrowid
        logger.debug('saved datacron, id:%s', datacron_id)
        return datacron_id

    def get_sql_create_table(self, dbtype='mysql'):
        dcstats = AllDatacronStats()

        query = '''
        create table datacronsv2 (
            dc_id integer primary key auto_increment,
        '''
        for ability in dcstats.ability_indices:
            query += ' dc_ability_' + str(ability) + ' smallint unsigned,\n'
        for stat in dcstats.stat_indices:
            query += ' dc_stat_' + str(stat) + ' float,\n'

        query += ' level int,\n'

        if dbtype == 'mysql':
            for ability in dcstats.ability_indices:
                query += ' INDEX idx_ab_' + \
                    str(ability)+' (dc_ability_'+str(ability)+'),\n'
            for stat in dcstats.stat_indices:
                query += ' INDEX idx_st_' + \
                    str(stat)+'(dc_stat_'+str(stat)+'),\n'

        query = query[:-2]
        query += ') '

        if dbtype == 'sqlite':
            query = query.replace('auto_increment', 'autoincrement')

        return query
