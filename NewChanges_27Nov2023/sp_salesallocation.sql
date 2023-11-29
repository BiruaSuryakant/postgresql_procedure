CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_salesallocation(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$ 

DECLARE
    sourcedbcount integer;
  	cust_insertedRow_count integer; 
 	cust_deletedRows_count integer;
	cust_insertedRow_count_log integer;
	v_sqlstate text;
    v_message text;
    v_context text;
	table_name varchar(50) = 'salesallocation';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
	cut_off_time varchar(50);
	lastsuccessfullexecuteddate varchar(50)  = '1570-01-01 00:00:00';
begin
	cut_off_time = ' ' || (select cast((select cutofftime from pinnacle_interim_database.source_db_mapping where sourceid = db_instance_id) as varchar));
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),start_time)) as timestamp)) 
	and (select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp))) = 0 then
	
		--This block will execute for the first time for all the record from remote database
		IF (select count(*) from pinnacle_interim_database.salesallocation where instanceid = db_instance_id) = 0 THEN 
		insert into pinnacle_interim_database.salesallocation(
		allocationid, allocationdate, "user", multiloc, ledgerbillid, ledgerpayid, hidden, amount, instanceid)
		select * from dblink(remote_conn,CONCAT('select allocationid,
		allocationdate, user, multiloc, ledgerbillid, ledgerpayid, hidden, amount, ',db_instance_id,' from proview.salesallocation'))
		AS P(allocationid int4, allocationdate timestamp, "user" varchar, multiloc int4, ledgerbillid int4, ledgerpayid int4,
		hidden bool, amount numeric(10, 2), instanceid int4);  
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,'select allocationid from proview.salesallocation')
	 	AS P(allocationid int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'salesallocation',sourcedbcount,
		cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
 
		else
		--Fetch the last successfull date and time, when job was executed
	    lastsuccessfullexecuteddate = (select max(lastsuccessfulldatetime) from pinnacle_interim_database.log_history lh where lh.tablename = table_name and lh.returncode = success_code);
		if lastsuccessfullexecuteddate IS NOT NULL and lastsuccessfullexecuteddate != '1570-01-01 00:00:00' then
		--This block will execute based on when job was successfull executed date
	    --if lastsuccessfullexecuteddate  IS NOT null then	   		
		insert into pinnacle_interim_database.salesallocation(
		allocationid, allocationdate, "user", multiloc, ledgerbillid, ledgerpayid, hidden, amount, instanceid)
		select * from dblink(remote_conn,CONCAT('select allocationid,
		allocationdate, user, multiloc, ledgerbillid, ledgerpayid, hidden, amount,
		',db_instance_id,E' from proview.salesallocation where allocationdate between 
		\'' || (select cast((concat(cast(lastsuccessfullexecuteddate as date),cut_off_time)) as timestamp)) || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\''))
		AS P(allocationid int4, allocationdate timestamp, "user" varchar, multiloc int4, ledgerbillid int4, ledgerpayid int4,
		hidden bool, amount numeric(10, 2), instanceid int4);
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,E'select allocationid
		from proview.salesallocation where allocationdate between 
		\'' || (select cast((concat(cast(lastsuccessfullexecuteddate as date),cut_off_time)) as timestamp)) || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'') AS P(allocationid int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'salesallocation',sourcedbcount,
		cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');

		end if;	    
		END IF;

	end if;
	
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
    v_sqlstate = returned_sqlstate,
    v_message = message_text,
    v_context = pg_exception_context;
    perform pinnacle_interim_database.add_log(db_instance_id,'sp_salesallocation',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 	
		
commit;	
end; 
 $procedure$
;
