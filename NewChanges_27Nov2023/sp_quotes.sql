CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_quotes(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'quotes';
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
		IF (select count(*) from pinnacle_interim_database.quotes where instanceid = db_instance_id) = 0 THEN
		insert into pinnacle_interim_database.quotes(
		ordernumber, vers, datecreated, timecreated, updated, orderstatus, paytype, claimno, custtype,
		custcode, delrecord, deliv_zip, total, tax1, tax2, salespers, wo_sales_p, taxexempt, taxno, freight,
		freightt1, freightt2, cust_po, till_no, shipp_meth, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2,
		dept_code, traceid, multiloc, ro_num, orderby, order_dept, instanceid)
		select * from dblink(remote_conn,CONCAT('select ordernumber,
		vers, datecreated, timecreated, updated, orderstatus, paytype, claimno, custtype, custcode,
		delrecord, deliv_zip, total, tax1, 	tax2, salespers, wo_sales_p, taxexempt, taxno, freight, freightt1,
		freightt2, cust_po, till_no, shipp_meth, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, dept_code,
		traceid, multiloc, ro_num, orderby, order_dept, ',db_instance_id,' from proview.quotes'))
		AS P(ordernumber int4, vers bpchar(1), datecreated date, timecreated text, updated timestamp,
		orderstatus text, paytype bpchar(1), claimno varchar(25), custtype bpchar, custcode int4,
		delrecord int4, deliv_zip varchar(10), total numeric, tax1 numeric, tax2 numeric, salespers varchar,
		wo_sales_p varchar, taxexempt text, taxno varchar(20), freight numeric, freightt1 numeric,
		freightt2 numeric, cust_po varchar(20), till_no int4, shipp_meth bpchar(1), wtotal numeric(10, 2),
		wtax1 numeric(10, 2), wtax2 numeric(10, 2), surtotal numeric(10, 2), surtax1 numeric(10, 2),
		surtax2 numeric(10, 2), dept_code varchar(30), traceid varchar(16), multiloc int4, ro_num varchar(25),
		orderby bpchar(10), order_dept varchar(10), instanceid int4);  
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,'select ordernumber from proview.quotes')
		AS P(ordernumber int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'quotes',sourcedbcount,
		cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
  
		else
		--Fetch the last successfull date and time, when job was executed
	    lastsuccessfullexecuteddate = (select max(lastsuccessfulldatetime) from pinnacle_interim_database.log_history lh where lh.tablename = table_name and lh.returncode = success_code);
		if lastsuccessfullexecuteddate IS NOT NULL and lastsuccessfullexecuteddate != '1570-01-01 00:00:00' then
		--This block will execute based on when job was successfull executed date
	    --if lastsuccessfullexecuteddate  IS NOT null then	   	
		insert into pinnacle_interim_database.quotes(
		ordernumber, vers, datecreated, timecreated, updated, orderstatus, paytype, claimno, custtype,
		custcode, delrecord, deliv_zip, total, tax1, tax2, salespers, wo_sales_p, taxexempt, taxno, freight,
		freightt1, freightt2, cust_po, till_no, shipp_meth, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2,
		dept_code, traceid, multiloc, ro_num, orderby, order_dept, instanceid)
		select * from dblink(remote_conn,CONCAT('select ordernumber,
		vers, datecreated, timecreated, updated, orderstatus, paytype, claimno, custtype, custcode,
		delrecord, deliv_zip, total, tax1, 	tax2, salespers, wo_sales_p, taxexempt, taxno, freight, freightt1,
		freightt2, cust_po, till_no, shipp_meth, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, dept_code,
		traceid, multiloc, ro_num, orderby, order_dept, ',db_instance_id,E' from proview.quotes where updated  between 
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\''))
		AS P(ordernumber int4, vers bpchar(1), datecreated date, timecreated text, updated timestamp,
		orderstatus text, paytype bpchar(1), claimno varchar(25), custtype bpchar, custcode int4,
		delrecord int4, deliv_zip varchar(10), total numeric, tax1 numeric, tax2 numeric, salespers varchar,
		wo_sales_p varchar, taxexempt text, taxno varchar(20), freight numeric, freightt1 numeric,
		freightt2 numeric, cust_po varchar(20), till_no int4, shipp_meth bpchar(1), wtotal numeric(10, 2),
		wtax1 numeric(10, 2), wtax2 numeric(10, 2), surtotal numeric(10, 2), surtax1 numeric(10, 2),
		surtax2 numeric(10, 2), dept_code varchar(30), traceid varchar(16), multiloc int4, ro_num varchar(25),
		orderby bpchar(10), order_dept varchar(10), instanceid int4);  
	
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,E'select ordernumber
		from proview.quotes where updated  between 
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'') AS P(ordernumber int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'quotes',sourcedbcount,
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
    perform pinnacle_interim_database.add_log(db_instance_id,'sp_quotes',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 	
		
commit;	
end; 
 $procedure$
;
