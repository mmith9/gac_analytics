import traceback
import logging
import mysql.connector

from gac_dictionaries import DictionaryPlus
logger = logging.getLogger(__name__)

class UnitStats:
    def __init__(self, unit_dict: DictionaryPlus, zuo_dict: DictionaryPlus) -> None:
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
