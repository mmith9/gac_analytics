from typing import List
import re
import logging
import time
from bs4 import Tag
from datacron_v2 import DatacronV2

logger = logging.getLogger(__name__)


class GacUnit:
    def __init__(self, unit_element: Tag):
        self.unit_element = unit_element
        self.role: str
        self.base_id: str
        self.health: int
        self.prot: int

    def reap_unit(self):
        name = self.unit_element.find(class_='character-portrait__img')
        if not name:
            name = self.unit_element.find(class_='ship-portrait__img')

        self.base_id = name.attrs['data-base-id']

        statbar = self.unit_element.find(class_='gac-unit__bar-inner--prot')
        if statbar:
            statbar = statbar.attrs['style']
            matching = re.search('width: (\\d+)%', statbar)
            self.prot = int(matching.group(1))
        else:
            self.prot = -1

        statbar = self.unit_element.find(class_='gac-unit__bar-inner--hp')
        if statbar:
            statbar = statbar.attrs['style']
            matching = re.search('width: (\\d+)%', statbar)
            self.health = int(matching.group(1))
        else:
            self.health = -1


class GacTeam:
    def __init__(self, team_element: Tag):
        self.team_element = team_element
        self.members: List[GacUnit] = []
        self.datacron: DatacronV2

    def reap_team(self):
        unit_elements = self.team_element.find_all(class_='gac-unit')
        for unit_element in unit_elements:
            unit = GacUnit(unit_element)
            unit.reap_unit()
            self.members.append(unit)
        self.members[0].role = 'leader'
        for unit in self.members[1:]:
            unit.role = 'member'
        dc_element = self.team_element.find(class_='gac-squad__datacron')
        if dc_element:
            self.datacron = DatacronV2()
            self.datacron.reap_from(dc_element)
        else:
            self.datacron = False


class GacPlayerBattle:
    def __init__(self, battle_element: Tag) -> None:
        self.battle_element = battle_element
        self.type: str
        self.datetime: int
        self.outcome: str
        self.duration: int
        self.attempt: int
        self.attacker_team: GacTeam
        self.defender_team: GacTeam
        self.banners: int

    def reap_battle(self):

        if self.battle_element.find_all(class_='character-portrait__img'):
            # list not empty, squad battle
            self.type = 'squad'
        else:
            self.type = 'ship'
            self.attempt = 0
            return

        self.outcome = self.battle_element.find(
            class_="gac-summary__status").text
        self.datetime = self.battle_element.find(
            class_="gac-datetime").attrs['data-datetime']
        self.datetime = int(int(self.datetime)/1000)
        text = self.battle_element.find(class_='panel').text
        matching = re.search('Length: (\\d):(\\d\\d)', text)
        # displayed duration is sometimes weird and not caught by proper regexp
        if matching:
            self.duration = int(matching.group(1))*60 + int(matching.group(2))
        else:
            self.duration = 0
        matching = re.search('Attempt: (\\d+)', text)
        self.attempt = int(matching.group(1))
        if self.outcome.find("WIN") >= 0:
            matching = re.search('Banners: (\\d+)', text)
            self.banners = int(matching.group(1))
        elif self.outcome.find("LOSS") >= 0 or \
                self.outcome.find("TIMEOUT") >= 0 or \
                self.outcome.find("QUIT") >= 0:
            self.banners = 0
        else:
            logger.critical(
                'neither WIN nor LOSS/TIMEOUT/QUIT: %s', self.outcome)
            assert False

        teams = self.battle_element.find_all(class_='gac-squad')

        self.attacker_team = GacTeam(teams[0])
        self.attacker_team.reap_team()
        self.defender_team = GacTeam(teams[1])
        self.defender_team.reap_team()

    def get_dead_members_ids(self) -> list:
        the_dead=[]
        for unit in self.defender_team.members[1:]:
            if unit.health <= 0:
                the_dead.append(unit.base_id)
        return the_dead

    def is_def_leader_alive(self) -> bool:
        return self.defender_team.members[0].health > 0


class GacRound:
    def __init__(self, round_element: Tag, round_data: dict) -> None:
        self.round_data = round_data
        self.battles: List[GacPlayerBattle] = []
        self.round_element = round_element
        self.defender: int
        self.attacker = round_data['attacker']
        self.gac_num = round_data['gac_num']

    def reap_round(self):
        start_time = time.time()

        winner = self.round_element.find_all(class_="winner")
        if not winner:  # empty list
            logger.warning('could not find winner element')
            return False
        winner = winner[0]
        logger.debug('got winner element')

        loser = self.round_element.find_all(class_='loser')
        if not loser:  # empty list
            logger.warning('could not find winner element')
            return False
        loser = loser[0]
        logger.debug('got loser element')

        if 'href' in winner.attrs:
            enemy = winner.attrs['href']
        elif 'href' in loser.attrs:
            enemy = loser.attrs['href']
        else:
            logger.critical('No href found, cant find defender allycode')
            assert False
        matching = re.search('/p/([0-9]{9})/', enemy)
        if matching:
            self.defender = int(matching.group(1))
        else:
            logger.critical('failed to get defender allycode from regex')
            assert False

        if self.defender not in self.round_data['snapped_allycodes']:
            logger.info('combat : %s vs %s in round %s enemy allycode not snapped',
                        self.round_data['attacker'], self.defender, self.round_data['round_num'])
            return False

        battle_elements = self.round_element.find_all(
            class_='gac-player-battles')

        battle_elements.pop(0)  # first line is headline
        for battle_element in battle_elements:
            battle = GacPlayerBattle(battle_element)
            battle.reap_battle()
            self.battles.append(battle)

        reaping_time = time.time() - start_time
        if len(self.battles) > 0:
            logger.debug('reaped %s battles in %s (%s per battle)',
                         len(self.battles), str(round(reaping_time, 3)),
                         str(round(reaping_time / len(self.battles), 3)))
        else:
            logger.info('reaped 0 battles in %s', str(round(reaping_time, 3)))

        return True

    def find_previous_battle(self, cleanup_battle: GacPlayerBattle) -> GacPlayerBattle:
        for battle in self.battles:
            if battle.type != 'squad':
                continue

            is_match = True
            is_match &= (battle.attempt == cleanup_battle.attempt - 1)
            is_match &= (len(battle.defender_team.members) ==
                         len(cleanup_battle.defender_team.members))
            if is_match:
                for x in range(0, len(battle.defender_team.members)):
                    is_match &= (battle.defender_team.members[x].base_id \
                             == cleanup_battle.defender_team.members[x].base_id)
            if is_match:
                logger.debug('found previous battle')
                return battle
        logger.error('failed to find previous battle')
        return False

