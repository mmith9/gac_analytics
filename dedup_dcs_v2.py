#!/usr/bin/env python -u




import time
import logging
import logging.config
from datacron_v2 import AllDatacronStats, DatacronV2

from db_objects import LocalDb
from constant_data import VERSION




logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


def main():
    localdb=LocalDb()
    allstat =AllDatacronStats()
    abilities=allstat.ability_indices
    stats=allstat.stat_indices


    localdb.connect('gac_temp_db.sqlite')
    cursor = localdb.cursor

    query = '''
select *, count(*) cnt from datacrons_v2
group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
having cnt>1
'''

    cursor.execute(query)
    rows=cursor.fetchall()
    dupecount=len(rows)
    print()
    print()
    for row in rows:
        print('\r left: ', dupecount,'  ', end='')
        dupecount-=1
        query = 'select * from datacrons_v2 where \n'
        index=1
        for ability in abilities:
            query+=' dc_ability_'+str(ability)
            if row[index] is not None:
                query+= ' = ' + str(row[index])
            else:
                query+= ' is null'
            query+=' and\n'
            index+=1

        for stat in stats:
            query+=' dc_stat_'+str(stat)
            if row[index] is not None:
                query+= ' = ' + str(row[index])
            else:
                query+= ' is null '
            query+=' and\n'
            index+=1
        query+='level ='+str(row[index])


#        print(query)
        cursor.execute(query)
        dupes = cursor.fetchall()
#        print(row)
#        print(query)

#        print(len(dupes),end='')
#        for dupe in dupes:
#            print(dupe)
        if len(dupes) == 0:
 #           print('M! ',end='')
            continue

        orig_id = dupes[0][0]
        for dupe in dupes[1:]:
            dupe_id = dupe[0]
            query= 'update local_battles set attacker_dc_id = ? \
                    where attacker_dc_id = ?'
#            print(query)
            cursor.execute(query, (orig_id, dupe_id))
            query= 'update local_battles set defender_dc_id = ? \
                    where defender_dc_id = ?'
#            print(query)                    
            cursor.execute(query, (orig_id, dupe_id))
            query='delete from datacrons_v2 where dc_id = ?'
#            print(query)
            cursor.execute(query, (dupe_id,))
        localdb.connection.commit()
 #       input('pause')





if __name__ == "__main__":

    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='slave worker for selenium on raspberry')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    start = time.time()
    main()
    end = time.time()
    total_time = end - start
    print("\nExecution time: " + str(total_time))
