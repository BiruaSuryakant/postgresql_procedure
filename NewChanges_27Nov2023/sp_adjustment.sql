CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_adjustment(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
declare cnt record;  
declare 
  	tableIdVrbl integer;
  	sourceidvrbl integer;
 	assignsourceid record; 
    sourcedbcount integer;
	v_sqlstate text; 
	v_message text;
	v_context text;
  	cust_insertedRow_count integer;
 	cust_deletedRows_count integer;
	cust_insertedRow_count_log integer;
	table_name varchar(50) = 'adjustment';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
	cut_off_time varchar(50);
	lastsuccessfullexecuteddate varchar(50) = '1570-01-01 00:00:00';;
begin
		cut_off_time = ' ' || (select cast((select cutofftime from pinnacle_interim_database.source_db_mapping where sourceid = db_instance_id) as varchar));
	
		if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
		and lh.tablename = table_name
		and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),start_time)) as timestamp)) 
		and (select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp))) = 0 then
			
		--This block will execute for the first time for all the record from remote databse
	    IF (select count(*) from pinnacle_interim_database.adjustment where instanceid = db_instance_id) = 0 THEN 
	     insert into pinnacle_interim_database.adjustment(adjustid, customer, "user", "date", amount, adjusttype,
		reason,	allocateddate,	allocated,	unallocated, instanceid)
		select * from dblink('remote_conn',CONCAT('select adjustid,	customer, user,	date, amount, adjusttype,
		reason,	allocateddate,	allocated,	unallocated,', db_instance_id,' from proview.adjustment')) 
		AS P(adjustid int4,	customer varchar(45), "user" varchar, "date" timestamp, amount numeric,
		adjusttype text, reason varchar, allocateddate timestamp, allocated numeric, unallocated numeric, instanceid int4);
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database	
		select into sourcedbcount count(*) from dblink(remote_conn,'select adjustid from proview.adjustment') 
				AS P(adjustid int4);
		
		--insert log table 
		perform pinnacle_interim_database.add_log(db_instance_id,'adjustment',sourcedbcount,cust_insertedRow_count,
				cust_deletedRows_count,'Success',0,'');
			
		else
		--Fetch the last successfull date and time, when job was executed
	    lastsuccessfullexecuteddate = (select max(lastsuccessfulldatetime) from pinnacle_interim_database.log_history lh where lh.tablename = table_name and lh.returncode = success_code);
		
		if lastsuccessfullexecuteddate IS NOT NULL and lastsuccessfullexecuteddate != '1570-01-01 00:00:00' then
		--This block will execute based on when job was successfull executed date
	    --if lastsuccessfullexecuteddate  IS NOT null then	 
	    insert into pinnacle_interim_database.adjustment(adjustid,customer,	"user",	"date", amount,	adjusttype,
		reason,	allocateddate,	allocated, unallocated,	instanceid)
		select * from dblink('remote_conn',CONCAT('select adjustid,
		customer, user,	date, amount, adjusttype, reason, allocateddate, allocated,	unallocated,',
		db_instance_id,E' from proview.adjustment where allocateddate  between 
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 		
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'')) 
		AS P(adjustid int4,	customer varchar(45), "user" varchar, "date" timestamp,	amount numeric,	adjusttype text,
		reason varchar,	allocateddate timestamp, allocated numeric,	unallocated numeric, instanceid int4);
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
		
		--Getting number of records from source db which being inserted into interm database	
		select into sourcedbcount count(*) from dblink(remote_conn,E'select adjustid from proview.adjustment where allocateddate  between
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 	
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'') AS P(adjustid int4);
	
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'adjustment',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
		
		end if;
	    
		END IF;
	
		end if;
	
	exception
	when others then
	rollback;
	v_sqlstate = returned_sqlstate,
    v_message = message_text,
    v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_adjustment',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));

commit;
end; 
 $procedure$
;
