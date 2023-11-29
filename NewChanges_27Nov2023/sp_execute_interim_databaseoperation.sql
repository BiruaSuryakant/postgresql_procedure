CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_execute_interim_databaseoperation(IN db_instance_id integer, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
declare cnt record;
  	tableIdVrbl integer; 
  	sourceidvrbl integer;
 	assignsourceid record; 
    sourcedbcount integer; 
  	cust_insertedRow_count integer;
 	cust_deletedRows_count integer; 
	cust_insertedRow_count_log integer;
	v_sqlstate text;
    v_message text; 
    v_context text;
   	job_startingtime timestamp;
   	job_endtime timestamp;
   	failure_code varchar(10) = 'Failure';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
	decripted_password varchar(100);
	decription_key varchar(50) = 'AESOP@pinnacle';
begin
	raise notice 'main procedure';
	job_startingtime = now();
   	select databasename,dbport,dbserver,dbuserid,dbpassword into cnt from pinnacle_interim_database.source_db_mapping 
   	where pinnacle_interim_database.source_db_mapping.instanceid = db_instance_id;
   	raise notice 'before decription';
    --Decrypting the password to connect with remote database
   	decripted_password = (select (pgp_sym_decrypt(cnt.dbpassword::bytea, decription_key)));
   	raise notice 'after decription';
  	PERFORM dblink_connect('remote_conn',CONCAT('dbname=', cnt.databasename ,' port=',cnt.dbport,
  			' host=',cnt.dbserver,' user=',cnt.dbuserid ,' password=',decripted_password));
	begin 
		
		----------------------------Refresh every day-------------------------------------
		call pinnacle_interim_database.sp_customers(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_damagecodes(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_emails(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_models(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_parts(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_phones(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_setprice(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_sites(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_stock(db_instance_id,'remote_conn',prm_date);
	
		call pinnacle_interim_database.sp_vehistok(db_instance_id,'remote_conn',prm_date);
		--------------------End Refresh Every Day-------------------		
		--------------------Start Only New Records-------------------------
		call pinnacle_interim_database.sp_adjustalloc(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_adjustment(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_invoicedetail(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_invoices(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_oldparts(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_payalloc(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_payments(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_purchaseallocation(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_purchaseorder(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_purchaseorderdetail(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_quotes(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_salesallocation(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_salesledger(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_search(db_instance_id,'remote_conn',prm_date);
		--------------------------End Only New Records-------------------------------------
		--------------------------Start New and Update Records-------------------------
		call pinnacle_interim_database.sp_newstock(db_instance_id,'remote_conn',prm_date);
		
		call pinnacle_interim_database.sp_purchaseledger(db_instance_id,'remote_conn',prm_date);
		-------------------------End New and Update Records------------------------------

		job_endtime = (select now());
		raise notice 'Completed execution of all procedures';
		perform pinnacle_interim_database.add_job_history(db_instance_id,job_startingtime,job_endtime,success_code);
		end;   	
	
   		--Disconnecting from database
		perform dblink_disconnect('remote_conn');
		exception
	 	when others then
	 	GET STACKED DIAGNOSTICS
        v_sqlstate = returned_sqlstate,
        v_message = message_text,
        v_context = pg_exception_context;
       	perform pinnacle_interim_database.add_log(db_instance_id,'sp_execute_interim_databaseoperation',0,0,0,
       	'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 	perform pinnacle_interim_database.add_job_history(db_instance_id,job_startingtime,job_endtime,failure_code);
	 	perform dblink_disconnect('remote_conn');
raise notice 'Operation completed';
end; 
 $procedure$
;
