CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_sites(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'sites';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then

	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.sites where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT;
	  
	--This will fetch the data from remote database and insert into intermediate database
	insert into pinnacle_interim_database.sites(multiloc,
	shortcode, yardname, maincontact, address1, address2, address3, city, zip, state, country,
	phone1, phone2, fax, email, yardurl, dealernumber, taxid, licensenumber, urgid, edenname,
	edenstore, instanceid)
	select * from dblink(remote_conn,CONCAT('select multiloc,
	shortcode, yardname, maincontact, address1, address2, address3, city, zip, state, country,
	phone1, phone2, fax, email, yardurl, dealernumber, taxid, licensenumber, urgid, edenname,
	edenstore,',db_instance_id,' from proview.sites'))
	AS P(multiloc int4, shortcode varchar(3), yardname varchar(50), maincontact varchar(40),
	address1 varchar(40), address2 varchar(40), address3 varchar(40), city varchar(40),
	zip varchar(10), state bpchar(3), country bpchar(2), phone1 varchar(20), phone2 varchar(50),
	fax varchar(20), email varchar(50), yardurl varchar(50), dealernumber varchar(15),
	taxid varchar(15), licensenumber varchar, urgid bpchar(4), edenname varchar(9),
	edenstore int4, instanceid int4);
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select multiloc from proview.sites')
	AS P(multiloc int4);
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'sites',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');

	end if;
 
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
    v_sqlstate = returned_sqlstate,
    v_message = message_text,
    v_context = pg_exception_context;
    perform pinnacle_interim_database.add_log(db_instance_id,'sp_sites',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 	
		
commit;
end; 
 $procedure$
;
