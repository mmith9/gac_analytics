select count(*) as cnt_all
from battles b1 

inner join participants p1 on (p1.pcp_battle_id = b1.battle_id and p1.pcp_side = 'defender' and p1.pcp_role ='leader' and p1.pcp_unit_id =214)
inner join participants p2 on (p2.pcp_battle_id = b1.battle_id and p2.pcp_side = 'defender' and p2.pcp_unit_id = 19 )
inner join participants p3 on (p3.pcp_battle_id = b1.battle_id and p3.pcp_side = 'defender' and p3.pcp_unit_id = 122)
inner join participants p4 on (p4.pcp_battle_id = b1.battle_id and p4.pcp_side = 'defender' and p4.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id))
inner join participants p5 on (p5.pcp_battle_id = b1.battle_id and p5.pcp_side = 'defender' and p4.pcp_unit_id < p5.pcp_unit_id and p5.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id))

;
select b1.battle_id from battles b1 where battle_id in (
    select p1.pcp_battle_id from participants p1
    inner join participants p2 on p1.pcp_battle_id = p2.pcp_battle_id
    inner join participants p3 on p1.pcp_battle_id = p3.pcp_battle_id
    where
    (p1.pcp_unit_id= 214 and p1.pcp_role = 'leader' and p1.pcp_side= 'defender') and
    (p2.pcp_unit_id= 19 and p2.pcp_side= 'defender') and
    (p3.pcp_unit_id= 122 and p3.pcp_side= 'defender') ); 

select * from battles where battle_id in (
    select pcp_battle_id from participants where
    (pcp_unit_id= 214 and pcp_role= 'leader' and pcp_side= 'defender') or
    (pcp_unit_id= 19  and pcp_side= 'defender') or
    (pcp_unit_id= 122 and pcp_side= 'defender') 
    group by pcp_battle_id having count(pcp_battle_id) = 3);
    
SELECT * FROM battles TBL1
WHERE EXISTS (SELECT 1
              FROM participants TBL2
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 214 AND TBL2.pcp_role = 'leader' AND TBL2.pcp_side = 'defender'
             ) AND
      EXISTS (SELECT 1
              FROM participants TBL2
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 19 AND TBL2.pcp_side = 'defender'
             )AND
      EXISTS (SELECT 1
              FROM participants TBL2
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 122 AND TBL2.pcp_side = 'defender'
             );
             


select armor from unit_stats where us_unit_id =19 order by armor desc limit 20;

SELECT * FROM battles TBL1
WHERE EXISTS (SELECT 1
              FROM participants TBL2
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 214 AND TBL2.pcp_role = 'leader' AND TBL2.pcp_side = 'defender'
             ) AND
      EXISTS (SELECT 1
              FROM participants TBL2
              inner join unit_stats us2 on (us2.us_unit_id = tbl2.pcp_unit_id and us2.us_allycode = tbl1.attacker)
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 19 AND TBL2.pcp_side = 'defender' and us2.armor >1000
             )AND
      EXISTS (SELECT 1
              FROM participants TBL2
              WHERE TBL1.battle_id= TBL2.pcp_battle_id AND TBL2.pcp_unit_id= 122 AND TBL2.pcp_side = 'defender'
             )AND
      EXISTS (SELECT 1
              FROM datacrons dcs
              WHERE tbl1.defender = dcs.dc_allycode and dc_ability_9 = 48
             );

select b1.battle_id, b1.bt_date, b1.banners, b1.duration, b1.attacker, b1.defender, p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id, p4.pcp_unit_id, p5.pcp_unit_id from battles b1 
inner join participants p1 on (p1.pcp_battle_id = b1.battle_id and p1.pcp_side = 'defender' and p1.pcp_role ='leader' and p1.pcp_unit_id = 214)
inner join participants p2 on (p2.pcp_battle_id = b1.battle_id and p2.pcp_side = 'defender' and p2.pcp_unit_id = 19)
inner join participants p3 on (p3.pcp_battle_id = b1.battle_id and p3.pcp_side = 'defender' and p3.pcp_unit_id = 122)
inner join participants p4 on (p4.pcp_battle_id = b1.battle_id and p4.pcp_side = 'defender' and p4.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id))
inner join participants p5 on (p5.pcp_battle_id = b1.battle_id and p5.pcp_side = 'defender' and p5.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id) and p5.pcp_unit_id > p4.pcp_unit_id)
inner join unit_stats us2 on (p2.pcp_unit_id = us2.us_unit_id and b1.defender = us2.us_allycode and us2.us_gac_num = b1.bt_gac_num)
;

select round(us2.armor/1000)*1000 as stat1, count(*) as cnt_all, count(case when b1.banners > 5 then 1 end) as cnt_win,
avg(case when b1.banners > 5 then b1.banners end) , p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id, d4.name, d5.name


from battles b1 

inner join participants p1 on (p1.pcp_battle_id = b1.battle_id and p1.pcp_side = 'defender' and p1.pcp_role ='leader' and p1.pcp_unit_id = 214)
inner join participants p2 on (p2.pcp_battle_id = b1.battle_id and p2.pcp_side = 'defender' and p2.pcp_unit_id = 19)
inner join participants p3 on (p3.pcp_battle_id = b1.battle_id and p3.pcp_side = 'defender' and p3.pcp_unit_id = 122)
inner join participants p4 on (p4.pcp_battle_id = b1.battle_id and p4.pcp_side = 'defender' and p4.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id))
inner join participants p5 on (p5.pcp_battle_id = b1.battle_id and p5.pcp_side = 'defender' and p5.pcp_unit_id not in (p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id) and p5.pcp_unit_id > p4.pcp_unit_id)
inner join unit_stats us2 on (p2.pcp_unit_id = us2.us_unit_id and b1.defender = us2.us_allycode and us2.us_gac_num = b1.bt_gac_num)
inner join unit_dict d4 on (d4.unit_id = p4.pcp_unit_id)
inner join unit_dict d5 on (d5.unit_id = p5.pcp_unit_id)
group by p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id, p4.pcp_unit_id, p5.pcp_unit_id, stat1 order by stat1
;

select  count(*) as cnt_all, count(case when b1.banners > 0 then 1 end) as cnt_win, round(avg(case when b1.banners > 0 then b1.banners end),2)
, p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id, p4.pcp_unit_id, p5.pcp_unit_id, p6.pcp_unit_id, p7.pcp_unit_id, p8.pcp_unit_id, p9.pcp_unit_id, p10.pcp_unit_id  
from battles b1 

inner join participants p1 on (p1.pcp_battle_id = b1.battle_id and p1.pcp_side = 'defender' and p1.pcp_role ='leader' and p1.pcp_unit_id =214)
inner join participants p2 on (p2.pcp_battle_id = b1.battle_id and p2.pcp_side = 'defender' and p2.pcp_unit_id not in (p1.pcp_unit_id ))
inner join participants p3 on (p3.pcp_battle_id = b1.battle_id and p3.pcp_side = 'defender' and p2.pcp_unit_id < p3.pcp_unit_id and p3.pcp_unit_id not in (p1.pcp_unit_id))
inner join participants p4 on (p4.pcp_battle_id = b1.battle_id and p4.pcp_side = 'defender' and p3.pcp_unit_id < p4.pcp_unit_id and p4.pcp_unit_id not in (p1.pcp_unit_id))
inner join participants p5 on (p5.pcp_battle_id = b1.battle_id and p5.pcp_side = 'defender' and p4.pcp_unit_id < p5.pcp_unit_id and p5.pcp_unit_id not in (p1.pcp_unit_id))


inner join participants p6 on (p6.pcp_battle_id = b1.battle_id and p6.pcp_side = 'attacker' and p6.pcp_role = 'leader')
inner join participants p7 on (p7.pcp_battle_id = b1.battle_id and p7.pcp_side = 'attacker' and p7.pcp_unit_id not in (p6.pcp_unit_id))
inner join participants p8 on (p8.pcp_battle_id = b1.battle_id and p8.pcp_side = 'attacker' and p8.pcp_unit_id not in (p6.pcp_unit_id) and p8.pcp_unit_id > p7.pcp_unit_id)
inner join participants p9 on (p9.pcp_battle_id = b1.battle_id and p9.pcp_side = 'attacker' and p9.pcp_unit_id not in (p6.pcp_unit_id) and p9.pcp_unit_id > p8.pcp_unit_id)
inner join participants p10 on (p10.pcp_battle_id = b1.battle_id and p10.pcp_side = 'attacker' and p10.pcp_unit_id not in (p6.pcp_unit_id) and p10.pcp_unit_id > p9.pcp_unit_id)



where b1.bt_gac_num in (114)
group by p1.pcp_unit_id, p2.pcp_unit_id, p3.pcp_unit_id, p4.pcp_unit_id, p5.pcp_unit_id, p6.pcp_unit_id, p7.pcp_unit_id, p8.pcp_unit_id, p9.pcp_unit_id, p10.pcp_unit_id

order by cnt_all desc

;
analyze table participants update histogram on pcp_battle_id, pcp_unit_id, pcp_side, pcp_role;

;
d1.name, d2.name, d3.name, d4.name, d5.name, d6.name, d7.name, d8.name, d9.name, d10.name
inner join unit_dict d1 on (d1.unit_id = p1.pcp_unit_id)
inner join unit_dict d2 on (d2.unit_id = p2.pcp_unit_id)
inner join unit_dict d3 on (d3.unit_id = p3.pcp_unit_id)
inner join unit_dict d4 on (d4.unit_id = p4.pcp_unit_id)
inner join unit_dict d5 on (d5.unit_id = p5.pcp_unit_id)


inner join unit_dict d6 on (d6.unit_id = p6.pcp_unit_id)
inner join unit_dict d7 on (d7.unit_id = p7.pcp_unit_id)
inner join unit_dict d8 on (d8.unit_id = p8.pcp_unit_id)
inner join unit_dict d9 on (d9.unit_id = p9.pcp_unit_id)
inner join unit_dict d10 on (d10.unit_id = p10.pcp_unit_id)
;
select * from participants limit 100;

