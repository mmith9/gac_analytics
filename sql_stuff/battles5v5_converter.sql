CREATE TABLE `battles5v5` (
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
  `a4` smallint unsigned not null,
  `a5` smallint unsigned not null,
  `d1` smallint unsigned not null,
  `d2` smallint unsigned not null,
  `d3` smallint unsigned not null,
  `d4` smallint unsigned not null,
  `d5` smallint unsigned not null
);

insert into battles5v5 (battle_id, attacker, defender, bt_date, duration, bt_gac_num, banners, a1,a2,a3,a4,a5,d1,d2,d3,d4,d5) 
select battle_id, attacker, defender, bt_date, duration, bt_gac_num, banners, 0,0,0,0,0,0,0,0,0,0 from battles 
where bt_gac_num in (113,114);

update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='defender') 
inner join participants p2 on (battles5v5.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='defender') 
inner join participants p3 on (battles5v5.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='defender') 
inner join participants p4 on (battles5v5.battle_id = p4.pcp_battle_id and p1.pcp_unit_id <> p4.pcp_unit_id and p3.pcp_unit_id < p4.pcp_unit_id and p4.pcp_side='defender') 
inner join participants p5 on (battles5v5.battle_id = p5.pcp_battle_id and p1.pcp_unit_id <> p5.pcp_unit_id and p4.pcp_unit_id < p5.pcp_unit_id and p5.pcp_side='defender') 

set battles5v5.d1 = p1.pcp_unit_id, battles5v5.d2=p2.pcp_unit_id, battles5v5.d3= p3.pcp_unit_id, battles5v5.d4=p4.pcp_unit_id, battles5v5.d5=p5.pcp_unit_id 
;

update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles5v5.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
inner join participants p3 on (battles5v5.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='attacker') 
inner join participants p4 on (battles5v5.battle_id = p4.pcp_battle_id and p1.pcp_unit_id <> p4.pcp_unit_id and p3.pcp_unit_id < p4.pcp_unit_id and p4.pcp_side='attacker') 
inner join participants p5 on (battles5v5.battle_id = p5.pcp_battle_id and p1.pcp_unit_id <> p5.pcp_unit_id and p4.pcp_unit_id < p5.pcp_unit_id and p5.pcp_side='attacker') 
set battles5v5.a1 = p1.pcp_unit_id, battles5v5.a2=p2.pcp_unit_id, battles5v5.a3= p3.pcp_unit_id, battles5v5.a4=p4.pcp_unit_id, battles5v5.a5=p5.pcp_unit_id 
;

select count(*) from battles5v5 where a1=0;
update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles5v5.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
inner join participants p3 on (battles5v5.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='attacker') 
inner join participants p4 on (battles5v5.battle_id = p4.pcp_battle_id and p1.pcp_unit_id <> p4.pcp_unit_id and p3.pcp_unit_id < p4.pcp_unit_id and p4.pcp_side='attacker') 
set battles5v5.a1 = p1.pcp_unit_id, battles5v5.a2=p2.pcp_unit_id, battles5v5.a3= p3.pcp_unit_id, battles5v5.a4=p4.pcp_unit_id
where battles5v5.a1 = 0
;

select count(*) from battles5v5 where a1=0;
update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles5v5.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
inner join participants p3 on (battles5v5.battle_id = p3.pcp_battle_id and p1.pcp_unit_id <> p3.pcp_unit_id and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_side='attacker') 
set battles5v5.a1 = p1.pcp_unit_id, battles5v5.a2=p2.pcp_unit_id, battles5v5.a3= p3.pcp_unit_id
where battles5v5.a1 = 0
;

select count(*) from battles5v5 where a1=0;
update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
inner join participants p2 on (battles5v5.battle_id = p2.pcp_battle_id and p1.pcp_unit_id <> p2.pcp_unit_id and p2.pcp_side='attacker') 
set battles5v5.a1 = p1.pcp_unit_id, battles5v5.a2=p2.pcp_unit_id
where battles5v5.a1 = 0
;

select count(*) from battles5v5 where a1=0;
update battles5v5
inner join participants p1 on (battles5v5.battle_id = p1.pcp_battle_id and p1.pcp_role = 'leader' and p1.pcp_side='attacker') 
set battles5v5.a1 = p1.pcp_unit_id
where battles5v5.a1 = 0
;

select  round(100*sum(case when banners > 0 then 1 end)/count(*),2) as prc ,count(*) as cnt_all, count(case when banners > 0 then 1 end) as cnt_win, round(avg(case when banners > 0 then banners end),2),
	d1, d2, d3, d4, d5, a1, a2, a3, a4, a5
from battles5v5
where d1=214 and (122 in (d2, d3, d4 , d5)) and (80 in (d2,d3,d4,d5))
group by d1, d2, d3, d4 ,d5, a1, a2, a3, a4, a5
having cnt_all >5
order by prc desc;
