select count(*) from _job_scan_units;
select count(*) from _job_scan_battles;
select count(*) from top_allycodes;


select * from _job_scan_units limit 10;
select distinct us_gac_num from unit_stats;
select count(*) from unit_stats where us_gac_num = 116;


select ta_allycode, 116 from top_allycodes limit 10;

select * from _job_scan_leaderboards;
insert into _job_scan_leaderboards (league, division, page) select 1, 2, 3 from unit_stats limit 10;

select * from _job_scan_battles limit 10;
select count(*) from battles where bt_gac_num = 115;

select * from top_allycodes where ta_allycode >= 887912426 limit 10;

select * from unit_stats where us_allycode = 887912426  limit 5;

select count(distinct ta_allycode) from top_allycodes;
select count(distinct us_allycode) from unit_stats where us_gac_num = 115;

select distinct bt_gac_num from battles;
select distinct bt_gac_num from battles where bt_date >= 1665522000 ;
select distinct bt_gac_num from battles where bt_date < 1665522000 and bt_date >= 1664312400;
select distinct bt_gac_num from battles where bt_date < 1664312400 and bt_date >= 1663707600;

select count(*) from battles where bt_date < 1665522000 and bt_date >= 1664312400;
select count(*) from battles where bt_gac_num = 117;

select count(*) from battles where bt_gac_num =117 and bt_date >= 1665522000;

select * from battles where bt_gac_num =117 and bt_date >= 1665522000;

