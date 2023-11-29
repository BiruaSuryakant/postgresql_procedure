CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_emails(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'emails';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then

	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.emails where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT; 

	--This will fetch the data from remote database and insert into intermediate database  
	insert into pinnacle_interim_database.emails(link,
	contact, emailaddress, main, instanceid)
	select * from dblink(remote_conn,CONCAT('select link,
	contact, emailaddress, main,', db_instance_id,' from proview.emails'))
	AS P(link int4, contact text, emailaddress text, main bool, instanceid int4);
	
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database	
	select into sourcedbcount count(*) from dblink(remote_conn,'select link from proview.emails')
	AS P(code varchar(10));
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'emails',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
	
	end if;
 
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_emails',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));
		
commit;
end; 
 $procedure$
;
