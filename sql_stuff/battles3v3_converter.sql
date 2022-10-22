
CREATE TABLE `battles3v3` (
  `battle_id` int unsigned not null PRIMARY KEY AUTO_INCREMENT,
  `attacker` int unsigned not null,
  `defender` int unsigned not null,
  `banners` tinyint unsigned not null,
  `bt_date` int unsigned not null,
  `duration` smallint unsigned not null,
  `bt_gac_num` smallint unsigned not null,
  `a1` smallint unsigned not null,
  `a2` smallint unsigned not null,
  `a3` smallint unsigned not null,
  `d1` smallint unsigned not null,
  `d2` smallint unsigned not null,
  `d3` smallint unsigned not null
);


insert into battles3v3 (battle_id, attacker, defender, bt_date, duration, bt_gac_num, banners, a1,a2,a3,d1,d2,d3) 
select battle_id, attacker, defender, bt_date, duration, bt_gac_num, banners, 0,0,0,0,0,0 from battles 
where bt_gac_num in (115, 116, 117);

update battles3v3
inner join participants p1 on (battles3v3.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='defender') 
inner join participants p2 on (battles3v3.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='defender') 
inner join participants p3 on (battles3v3.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='defender') 
set battles3v3.d1 = p1.pcp_unit_id, battles3v3.d2=p2.pcp_unit_id, battles3v3.d3= p3.pcp_unit_id
;


select count(*) from battles3v3 where a1=0;
update battles3v3
inner join participants p1 on (battles3v3.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles3v3.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
inner join participants p3 on (battles3v3.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='attacker') 
set battles3v3.a1 = p1.pcp_unit_id, battles3v3.a2=p2.pcp_unit_id, battles3v3.a3= p3.pcp_unit_id
where battles3v3.a1 = 0
;

select count(*) from battles3v3 where a1=0;
update battles3v3
inner join participants p1 on (battles3v3.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles3v3.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
set battles3v3.a1 = p1.pcp_unit_id, battles3v3.a2=p2.pcp_unit_id
where battles3v3.a1 = 0
;

select count(*) from battles3v3 where a1=0;
update battles3v3
inner join participants p1 on (battles3v3.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
set battles3v3.a1 = p1.pcp_unit_id
where battles3v3.a1 = 0
;

