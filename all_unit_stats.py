
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


