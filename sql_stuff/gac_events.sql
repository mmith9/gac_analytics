CREATE TABLE `gac_events` (
  `ge_id` int PRIMARY KEY AUTO_INCREMENT,
  `swgohgg_gac_season` int,
  `swgohgg_gac_num` int,
  `cg_event_id` int,
  `cg_start_time` int,
  `cg_end_time` int,
  `cg_season_def_id` text,
  `cg_season_id` text
);

insert into gac_events (swgohgg_gac_num) select distinct bt_gac_num from battles;
select * from gac_events;
update gac_events set cg_ga2_event_num = swgohgg_gac_num-86;

select * from participants limit 100;
select * from unit_dict where unit_id=118;