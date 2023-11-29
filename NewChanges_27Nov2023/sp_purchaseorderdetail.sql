CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_purchaseorderdetail(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'purchaseorderdetail';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
	cut_off_time varchar(50);
	lastsuccessfullexecuteddate varchar(50) = '1570-01-01 00:00:00';
begin
	cut_off_time = ' ' || (select cast((select cutofftime from pinnacle_interim_database.source_db_mapping where sourceid = db_instance_id) as varchar));
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),start_time)) as timestamp)) 
	and (select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp))) = 0 then
	
		--This block will execute for the first time for all the record from remote database
		IF (select count(*) from pinnacle_interim_database.purchaseorderdetail where instanceid = db_instance_id) = 0 THEN 
		insert into pinnacle_interim_database.purchaseorderdetail(purchaseorderdetail_id,
		purchaseorder_id, disporder, detatiltype, quantity, part, icnumber, icver, model, modelyear, vstockno, mileskm,
		itemtext, istaxable, taxinclusive, itemprice, totalitemprice, totalitemtax1, totalitemtax2, taxrate1, taxrate2, itemnumber,
		parentnumber, detailnumber, itemtax1, itemtax2, dateexpected, vendorinvnumber, datereceived, datecancelled,
		vendorvstockno, vendorvin, vendortag, itemnew, cancelreason, cancelnote, dateaddedintoinventory, "unique",
		instanceid)
		select * from dblink(remote_conn,CONCAT('select purchaseorderdetail_id,
		purchaseorder_id, disporder, detatiltype, quantity, part, icnumber, icver, model, modelyear, vstockno, mileskm,
		itemtext, istaxable, taxinclusive, itemprice, totalitemprice, totalitemtax1, totalitemtax2, taxrate1, taxrate2, itemnumber,
		parentnumber, detailnumber, itemtax1, itemtax2, dateexpected, vendorinvnumber, datereceived, datecancelled,
		vendorvstockno, vendorvin, vendortag, itemnew, cancelreason, cancelnote, dateaddedintoinventory,
		"unique", ',db_instance_id,' from proview.purchaseorderdetail'))
		AS P(purchaseorderdetail_id int4, purchaseorder_id int4, disporder int4, detatiltype text, quantity int4, part bpchar(2),
		icnumber bpchar(6), icver bpchar(1), model bpchar(4), modelyear int4, vstockno varchar(8), mileskm bool,
		itemtext text, istaxable bool, taxinclusive bool, itemprice numeric(10, 2), totalitemprice numeric(10, 2),
		totalitemtax1 numeric(10, 2), totalitemtax2 numeric(10, 2), taxrate1 numeric(6, 3), taxrate2 numeric(6, 3),
		itemnumber int4, parentnumber int4, detailnumber int4, itemtax1 numeric(10, 2), itemtax2 numeric(10, 2),
		dateexpected date, vendorinvnumber int8, datereceived date, datecancelled date, vendorvstockno varchar(8),
		vendorvin varchar(17), vendortag varchar(10), itemnew bool, cancelreason text, cancelnote varchar,
		dateaddedintoinventory date, "unique" int8, instanceid int4);  
		
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,'select purchaseorderdetail_id from proview.purchaseorderdetail')
		AS P(purchaseorderdetail_id int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'purchaseorderdetail',sourcedbcount,
		cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
	
		else
		--Fetch the last successfull date and time, when job was executed
	    lastsuccessfullexecuteddate = (select max(lastsuccessfulldatetime) from pinnacle_interim_database.log_history lh where lh.tablename = table_name and lh.returncode = success_code);
		if lastsuccessfullexecuteddate IS NOT NULL and lastsuccessfullexecuteddate != '1570-01-01 00:00:00' then
		--This block will execute based on when job was successfull executed date
	    --if lastsuccessfullexecuteddate  IS NOT null then	   	
		insert into pinnacle_interim_database.purchaseorderdetail(purchaseorderdetail_id,
		purchaseorder_id, disporder, detatiltype, quantity, part, icnumber, icver, model, modelyear, vstockno, mileskm,
		itemtext, istaxable, taxinclusive, itemprice, totalitemprice, totalitemtax1, totalitemtax2, taxrate1, taxrate2, itemnumber,
		parentnumber, detailnumber, itemtax1, itemtax2, dateexpected, vendorinvnumber, datereceived, datecancelled,
		vendorvstockno, vendorvin, vendortag, itemnew, cancelreason, cancelnote, dateaddedintoinventory, "unique",
		instanceid)
		select * from dblink(remote_conn,CONCAT('select purchaseorderdetail_id,
		purchaseorder_id, disporder, detatiltype, quantity, part, icnumber, icver, model, modelyear, vstockno, mileskm,
		itemtext, istaxable, taxinclusive, itemprice, totalitemprice, totalitemtax1, totalitemtax2, taxrate1, taxrate2, itemnumber,
		parentnumber, detailnumber, itemtax1, itemtax2, dateexpected, vendorinvnumber, datereceived, datecancelled,
		vendorvstockno, vendorvin, vendortag, itemnew, cancelreason, cancelnote, dateaddedintoinventory,
		"unique",',db_instance_id,E' from proview.purchaseorderdetail where purchaseorder_id in 
		(select purchaseorder_id from proview.purchaseorder where updated between 
		\'' || (select cast((concat(cast(lastsuccessfullexecuteddate as date),cut_off_time)) as timestamp)) || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\')'))
		AS P(purchaseorderdetail_id int4, purchaseorder_id int4, disporder int4, detatiltype text, quantity int4, part bpchar(2),
		icnumber bpchar(6), icver bpchar(1), model bpchar(4), modelyear int4, vstockno varchar(8), mileskm bool,
		itemtext text, istaxable bool, taxinclusive bool, itemprice numeric(10, 2), totalitemprice numeric(10, 2),
		totalitemtax1 numeric(10, 2), totalitemtax2 numeric(10, 2), taxrate1 numeric(6, 3), taxrate2 numeric(6, 3),
		itemnumber int4, parentnumber int4, detailnumber int4, itemtax1 numeric(10, 2), itemtax2 numeric(10, 2),
		dateexpected date, vendorinvnumber int8, datereceived date, datecancelled date, vendorvstockno varchar(8),
		vendorvin varchar(17), vendortag varchar(10), itemnew bool, cancelreason text, cancelnote varchar,
		dateaddedintoinventory date, "unique" int8, instanceid int4);  
		
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,E'select purchaseorderdetail_id
		from proview.purchaseorderdetail where purchaseorder_id in (select purchaseorder_id from proview.purchaseorder where updated between 
		\'' || (select cast((concat(cast(lastsuccessfullexecuteddate as date),cut_off_time)) as timestamp)) || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\')') AS P(purchaseorderdetail_id int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'purchaseorderdetail',sourcedbcount,
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
    perform pinnacle_interim_database.add_log(db_instance_id,'sp_purchaseorderdetail',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));    
		
commit;	
end; 
 $procedure$
;
