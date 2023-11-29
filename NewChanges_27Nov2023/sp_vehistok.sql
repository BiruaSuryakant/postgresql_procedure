CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_vehistok(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$ 

DECLARE
    sourcedbcount integer;
    v_sqlstate text;
	v_message text;
	v_context text;
  	cust_insertedRow_count integer;
 	cust_deletedRows_count integer;
	cust_insertedRow_count_log integer;
	table_name varchar(50) = 'vehistok';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then

	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.vehistok where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT;
	  
	--This will fetch the data from remote database and insert into intermediate database
	insert into pinnacle_interim_database.vehistok("year",
	doors, trans_type, no_gears, vstockno, miles, ext_col, "trim", mod_sys, "cost", enter_date,
	seller_lnk, seller_mem, tot_tags, tot_extra, numtags, numextra, breakdate, eng_no, vin_no,
	"comments",	"location", int_col, purch_chq, prnt_date, pick_name, crush_date, no_cyl,
	eng_size, status, body_style, curr_sales, sale_reduc, tow_charge, bid_comm, pool_fee,
	pickup_mem, pickup_lnk, bidder_lnk, bidder_mem, prebid_lnk, prebid_mem, purch_date,
	towable_yn, store_chrg, keys, adjust_nam, title_num, title_stat, title_type, tow_cheq,
	store_cheq, prod_date, "abs", aircon, powersteer, elect_wind, wheel_size, towcom_lnk,
	towcom_mem, est_arrdat, prm_damage, sec_damage, proj_sales, claim_num, clear_date,
	xtra_damag, notify_dat, not_tow_dt, fuel_type, drive_type, user_id, delete_dat, radio_yn,
	v_classify, arrive_dat, sold_date, inv_date, dsmntl_dat, indsmt_dat, stall, pu_notes,
	axle_code, whoto_lnk, whoto_mem, dismantler, power_lock, mirrors, glassscolor, cruise,
	fender, intrimcode, extcolcode, forsale_pr, sold_price, part_req, part_added, airbag,
	battery, sparewheel, jack, wheels, miscstk, anticiprev, converted, multiloc, dism_comms,
	regno, log1, log2, log3, log4, log5, keyno, orgno, transcode, transdate, transtime,
	meldcode, duplicode, bakcode, eng_no2, towhook, spoiler, suspension, sunroof, belt,
	arnauto, bui, processed, dism_start, dism_end, handheld, pinpadtot, antrevb4in, misccost,
	depollcost, vendorcost, hasimages, driver_lnk, owner_lnk, stocknumber_id, licenseplate,
	weight, instanceid)
	select * from dblink(remote_conn,CONCAT('select "year",
	doors, trans_type, no_gears, vstockno, miles, ext_col, "trim", mod_sys, "cost", enter_date,
	seller_lnk, seller_mem, tot_tags, tot_extra, numtags, numextra, breakdate, eng_no, vin_no,
	"comments", "location", int_col, purch_chq, prnt_date, pick_name, crush_date, no_cyl,
	eng_size, status, body_style, curr_sales, sale_reduc, tow_charge, bid_comm, pool_fee,
	pickup_mem, pickup_lnk, bidder_lnk, bidder_mem, prebid_lnk, prebid_mem, purch_date,
	towable_yn, store_chrg, keys, adjust_nam, title_num, title_stat, title_type, tow_cheq,
	store_cheq, prod_date, "abs", aircon, powersteer, elect_wind, wheel_size, towcom_lnk,
	towcom_mem, est_arrdat, prm_damage, sec_damage, proj_sales, claim_num, clear_date,
	xtra_damag, notify_dat, not_tow_dt, fuel_type, drive_type, user_id, delete_dat, radio_yn,
	v_classify, arrive_dat, sold_date, inv_date, dsmntl_dat, indsmt_dat, stall, pu_notes,
	axle_code, whoto_lnk, whoto_mem, dismantler, power_lock, mirrors, glassscolor, cruise,
	fender, intrimcode, extcolcode, forsale_pr, sold_price, part_req, part_added, airbag,
	battery, sparewheel, jack, wheels, miscstk, anticiprev, converted, multiloc, dism_comms,
	regno, log1, log2, log3, log4, log5, keyno, orgno, transcode, transdate, transtime,
	meldcode, duplicode, bakcode, eng_no2, towhook, spoiler, suspension, sunroof, belt,
	arnauto, bui, processed, dism_start, dism_end, handheld, pinpadtot, antrevb4in, misccost,
	depollcost, vendorcost, hasimages, driver_lnk, owner_lnk, stocknumber_id, licenseplate,
	weight, ',db_instance_id,' from proview.vehistok'))
	AS P("year" int4, doors varchar(50), trans_type varchar(50), no_gears bpchar(1),
	vstockno varchar(8), miles int4, ext_col varchar(50), trim varchar(30), mod_sys bpchar(4),
	"cost" numeric(14, 2), enter_date date, seller_lnk int4, seller_mem varchar(10),
	tot_tags numeric(14, 2), tot_extra numeric(14, 2), numtags int4, numextra int4,
	breakdate date, eng_no varchar(30),	vin_no varchar(17), "comments" text, "location" varchar,
	int_col varchar(50), purch_chq varchar(80), prnt_date date, pick_name varchar(8),
	crush_date date, no_cyl bpchar(2), eng_size numeric(7, 2), status bpchar(1),
	body_style varchar(50), curr_sales numeric(14, 2), sale_reduc numeric(7, 2),
	tow_charge numeric(14, 2), bid_comm numeric(14, 2), pool_fee numeric(14, 2),
	pickup_mem varchar(10), pickup_lnk int4, bidder_lnk int4, bidder_mem varchar(10),
	prebid_lnk int4, prebid_mem varchar(10), purch_date date, towable_yn bpchar(1),
	store_chrg numeric(14, 2), keys bpchar(1), adjust_nam varchar(30), title_num varchar(20),
	title_stat bpchar(3), title_type varchar(20), tow_cheq varchar(80), store_cheq varchar(80),
	prod_date date, "abs" bpchar(1), aircon bpchar(1), powersteer bpchar(1),
	elect_wind bpchar(1), wheel_size int4, towcom_lnk int4, towcom_mem varchar(10),
	est_arrdat date, prm_damage varchar(50), sec_damage varchar(50), proj_sales numeric(10, 2),
	claim_num varchar(20), clear_date date,	xtra_damag varchar(60),	notify_dat date,
	not_tow_dt date, fuel_type varchar(50), drive_type varchar(50), user_id varchar,
	delete_dat date, radio_yn bpchar(1), v_classify varchar(50), arrive_dat date,
	sold_date date, inv_date date, dsmntl_dat date, indsmt_dat date, stall varchar(10),
	pu_notes varchar, axle_code varchar(4), whoto_lnk int4, whoto_mem varchar(10),
	dismantler text, power_lock bpchar(1), mirrors varchar(50), glassscolor bpchar(1),
	cruise bpchar(1), fender bpchar(1), intrimcode varchar(10), extcolcode varchar(10),
	forsale_pr numeric(14, 2), sold_price numeric(14, 2), part_req varchar(40),
	part_added bpchar(1), airbag bpchar(1), battery bpchar(1), sparewheel bpchar(1),
	jack bpchar(1), wheels bpchar(1), miscstk bool, anticiprev numeric(14, 2),
	converted bpchar(1), multiloc int4, dism_comms text, regno varchar(10), log1 bool,
	log2 bool, log3 bool, log4 bool, log5 bool, keyno varchar(15), orgno varchar(8),
	transcode varchar(8), transdate date, transtime varchar(5), meldcode varchar(4),
	duplicode varchar(2), bakcode varchar(6), eng_no2 varchar(10), towhook bpchar(1),
	spoiler bpchar(1), suspension bpchar(1), sunroof bpchar(1), belt bpchar(1),
	arnauto bpchar(1), bui bool, processed varchar(1), dism_start varchar(5),
	dism_end varchar(5), handheld bool, pinpadtot numeric(14, 2), antrevb4in numeric(14, 2),
	misccost numeric(14, 2), depollcost numeric(14, 2), vendorcost numeric(14, 2),
	hasimages bool, driver_lnk int4, owner_lnk int4, stocknumber_id int4, licenseplate varchar(10),
	weight numeric(7, 2), instanceid int4 );
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select year from proview.vehistok')
	AS P("year" int4);
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'vehistok',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');

	end if;
	
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_vehistok',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));	
		
commit;
end; 
 $procedure$
;
