CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_phones(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'phones';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin

	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then
	
	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.phones where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT;
	
	--This will fetch the data from remote database and insert into intermediate database	
	insert into pinnacle_interim_database.phones(name1,
	name2, add1, add2, add3, city, phone_1, phone_2, phone_3, fax, memo1, memo2, memo3,
	con_1, con_2, category, secret, con_3, shortcode, entype, geogloc, "source", link,
	site, state, zip, email, bidder, "location", seller, towcomp, id, "old", taxrate1,
	taxrate2, discount, converted, traceid, yardid, chgtax2, country, frtprint, userfield1,
	userfield2, userfield3, instanceid)
	select * from dblink(remote_conn,CONCAT('select name1,
	name2, add1, add2, add3, city, phone_1, phone_2, phone_3, fax, memo1, memo2, memo3,
	con_1, con_2, category, secret, con_3, shortcode, entype, geogloc, "source", link, site,
	state, zip, email, bidder, "location", seller, towcomp, id, "old", taxrate1, taxrate2,
	discount, converted, traceid, yardid, chgtax2, country, frtprint, userfield1, userfield2,
	userfield3,', db_instance_id,' from proview.phones'))
	AS P(name1 varchar(45), name2 varchar(50), add1 varchar(40), add2 varchar(40),
	add3 varchar(40), city varchar(40), phone_1 varchar(20), phone_2 varchar(20),
	phone_3 varchar(20), fax varchar(20), memo1 text, memo2 varchar(45), memo3 varchar(45),
	con_1 varchar(25), con_2 varchar(25), category varchar(29), secret bool, con_3 varchar(25),
	shortcode bpchar(6), entype bpchar(1), geogloc bpchar(4), "source" bpchar(4), link int4,
	site int4, state bpchar(3), zip varchar(10), email text, bidder bpchar(1), "location" bpchar(1),
	seller bpchar(1), towcomp bpchar(1), id int4, "old" bool, taxrate1 numeric(6, 3),
	taxrate2 numeric(6, 3), discount numeric(4, 1), converted bpchar(1), traceid varchar(16),
	yardid bpchar(4), chgtax2 bpchar(1), country bpchar(2), frtprint int4, userfield1 varchar(20),
	userfield2 varchar(20), userfield3 varchar(20), instanceid int4);
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select name1 from proview.phones')
	AS P(name1 varchar(45));
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'phones',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');

	end if;

	
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_phones',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));	
		
commit;
end; 
 $procedure$
;
