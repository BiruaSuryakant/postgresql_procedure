CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_invoices(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$ 

declare 
    sourcedbcount integer; 
    v_sqlstate text;
	v_message text;
	v_context text;  
  	cust_insertedRow_count integer; 
 	cust_deletedRows_count integer;
	cust_insertedRow_count_log integer;
	table_name varchar(50) = 'invoices';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),start_time)) as timestamp)) 
	and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then
	--This block will execute for the first time for all the record from remote database
	IF (select count(*) from pinnacle_interim_database.invoices where instanceid = db_instance_id) = 0 THEN 
	insert into pinnacle_interim_database.invoices(contact,	dept_code, ro_num, claimno, taxno, cust_po,
	customervin, traceid, cust_det,	deliv_det, deliv_zip, order_dept, salespers, wo_sales_p, "time",
	pay_det, taxexempt,	cred_desc, taxrate1,	taxrate2, taxrate, ftaxrate,
	interest, tax3,	intamt,	thresh1, thresh2, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, total, paid_amt,
	amtoutst, tax1, tax2, freight, freightt1, freightt2, invoice_no, stmnt, custcode, custrecno, delrecord, work_ordno, 
	pono, till_no, credit_no, cpblink, cbpid, dpblink, o_invoice, dpbid, multiloc, frtlink, frtid, quote_num, invoice_id, 
	invoice_da, work_orddt, qbxfer_dt, sourceyard, deliv_zone, orderby, invtype, paytype, ver, shipp_meth, converted, 
	paytax, taxcode, fcode, ftaxcode, custtype, paid, intflag, taxtax, taxgen, taxwty, taxsur, taxfrt, taxlab, flocaltax, 
	tax1rule1, tax2rule1, taxonly, instanceid)
	select * from dblink(remote_conn, CONCAT('select contact, dept_code, ro_num, claimno, taxno, cust_po,
	customervin, traceid, cust_det, deliv_det, deliv_zip, order_dept, salespers, wo_sales_p, time, pay_det, taxexempt, 
	cred_desc, taxrate1, taxrate2, taxrate, ftaxrate, interest, tax3, intamt, thresh1, 
	thresh2, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, total, paid_amt, amtoutst, tax1, tax2, freight, freightt1, 
	freightt2, invoice_no, stmnt, custcode, custrecno, delrecord, work_ordno, pono, till_no, credit_no, cpblink, 
	cbpid, dpblink, o_invoice, dpbid, multiloc, frtlink, frtid, quote_num, invoice_id, invoice_da, work_orddt, qbxfer_dt, 
	sourceyard, deliv_zone, orderby, invtype, paytype, ver, shipp_meth, converted, paytax, taxcode, fcode, ftaxcode, 
	custtype, paid, intflag, taxtax, taxgen, taxwty, taxsur, taxfrt, taxlab, flocaltax, tax1rule1, tax2rule1, taxonly,',
	db_instance_id,' from proview.invoices')) 
	AS P( contact varchar(50), dept_code varchar(30),
	ro_num varchar(25), claimno varchar(25), taxno varchar(20), cust_po varchar(20), customervin varchar(17), 
	traceid varchar(16), cust_det varchar(10), deliv_det varchar(10), deliv_zip varchar(10), order_dept varchar(10), 
	salespers varchar, wo_sales_p varchar, "time" text, pay_det text, taxexempt text, cred_desc text, 
	taxrate1 numeric(6, 3), taxrate2 numeric(6, 3), taxrate numeric(6, 3), ftaxrate numeric(6, 3), interest numeric(6, 2), 
	tax3 numeric(14, 2), intamt numeric(14, 2), thresh1 numeric(14, 2), thresh2 numeric(14, 2), wtotal numeric(10, 2), 
	wtax1 numeric(10, 2), wtax2 numeric(10, 2), surtotal numeric(10, 2), surtax1 numeric(10, 2), surtax2 numeric(10, 2), 
	total numeric, paid_amt numeric, amtoutst numeric, tax1 numeric, tax2 numeric, freight numeric, freightt1 numeric, 
	freightt2 numeric, invoice_no int4, stmnt int4, custcode int4, custrecno int4, delrecord int4, work_ordno int4, 
	pono int4, till_no int4, credit_no int4, cpblink int4, cbpid int4, dpblink int4, o_invoice int4, dpbid int4, 
	multiloc int4, frtlink int4, frtid int4, quote_num int4, invoice_id int4, invoice_da date, work_orddt date, 
	qbxfer_dt date, sourceyard bpchar(6), deliv_zone bpchar(2), orderby bpchar(10), invtype bpchar(1), paytype bpchar(1), 
	ver bpchar(1), shipp_meth bpchar(1), converted bpchar(1), paytax bpchar(1), taxcode bpchar(1), fcode bpchar(1), 
	ftaxcode bpchar(1), custtype bpchar, paid bool, intflag bool, taxtax bool, taxgen bool, taxwty bool, taxsur bool, 
	taxfrt bool, taxlab bool, flocaltax bool, tax1rule1 bool, tax2rule1 bool, taxonly bool, instanceid int4);
	  
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select invoice_id from proview.invoices') AS P(invoice_id int4);
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'invoices',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'',(SELECT max(invoice_id)  from pinnacle_interim_database.invoices));
	
	else 
	insert into pinnacle_interim_database.invoices(contact,	dept_code, ro_num, claimno, taxno, cust_po,
	customervin, traceid, cust_det,	deliv_det, deliv_zip, order_dept, salespers, wo_sales_p, "time",
	pay_det, taxexempt,	cred_desc,	taxrate1,	taxrate2, taxrate, ftaxrate,
	interest, tax3,	intamt,	thresh1, thresh2, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, total, paid_amt,
	amtoutst, tax1, tax2, freight, freightt1, freightt2, invoice_no, stmnt, custcode, custrecno, delrecord, work_ordno, 
	pono, till_no, credit_no, cpblink, cbpid, dpblink, o_invoice, dpbid, multiloc, frtlink, frtid, quote_num, invoice_id, 
	invoice_da, work_orddt, qbxfer_dt, sourceyard, deliv_zone, orderby, invtype, paytype, ver, shipp_meth, converted, 
	paytax, taxcode, fcode, ftaxcode, custtype, paid, intflag, taxtax, taxgen, taxwty, taxsur, taxfrt, taxlab, flocaltax, 
	tax1rule1, tax2rule1, taxonly, instanceid)
	select * from dblink(remote_conn, CONCAT('select contact, dept_code, ro_num, claimno, taxno, cust_po,
	customervin, traceid, cust_det, deliv_det, deliv_zip, order_dept, salespers, wo_sales_p, time, pay_det, taxexempt, 
	cred_desc, taxrate1, taxrate2, taxrate, ftaxrate, interest, tax3, intamt, thresh1, 
	thresh2, wtotal, wtax1, wtax2, surtotal, surtax1, surtax2, total, paid_amt, amtoutst, tax1, tax2, freight, freightt1, 
	freightt2, invoice_no, stmnt, custcode, custrecno, delrecord, work_ordno, pono, till_no, credit_no, cpblink, 
	cbpid, dpblink, o_invoice, dpbid, multiloc, frtlink, frtid, quote_num, invoice_id, invoice_da, work_orddt, qbxfer_dt, 
	sourceyard, deliv_zone, orderby, invtype, paytype, ver, shipp_meth, converted, paytax, taxcode, fcode, ftaxcode, 
	custtype, paid, intflag, taxtax, taxgen, taxwty, taxsur, taxfrt, taxlab, flocaltax, tax1rule1, tax2rule1, taxonly,',
	db_instance_id,E' from proview.invoices where invoice_id  > 
	\'' || (SELECT max(invoice_id)  from pinnacle_interim_database.invoices where instanceid = db_instance_id) || E'\'')) 
	AS P( contact varchar(50), dept_code varchar(30),
	ro_num varchar(25), claimno varchar(25), taxno varchar(20), cust_po varchar(20), customervin varchar(17), 
	traceid varchar(16), cust_det varchar(10), deliv_det varchar(10), deliv_zip varchar(10), order_dept varchar(10), 
	salespers varchar, wo_sales_p varchar, "time" text, pay_det text, taxexempt text, cred_desc text,
	taxrate1 numeric(6, 3), taxrate2 numeric(6, 3), taxrate numeric(6, 3), ftaxrate numeric(6, 3), interest numeric(6, 2), 
	tax3 numeric(14, 2), intamt numeric(14, 2), thresh1 numeric(14, 2), thresh2 numeric(14, 2), wtotal numeric(10, 2), 
	wtax1 numeric(10, 2), wtax2 numeric(10, 2), surtotal numeric(10, 2), surtax1 numeric(10, 2), surtax2 numeric(10, 2), 
	total numeric, paid_amt numeric, amtoutst numeric, tax1 numeric, tax2 numeric, freight numeric, freightt1 numeric, 
	freightt2 numeric, invoice_no int4, stmnt int4, custcode int4, custrecno int4, delrecord int4, work_ordno int4, 
	pono int4, till_no int4, credit_no int4, cpblink int4, cbpid int4, dpblink int4, o_invoice int4, dpbid int4, 
	multiloc int4, frtlink int4, frtid int4, quote_num int4, invoice_id int4, invoice_da date, work_orddt date, 
	qbxfer_dt date, sourceyard bpchar(6), deliv_zone bpchar(2), orderby bpchar(10), invtype bpchar(1), paytype bpchar(1), 
	ver bpchar(1), shipp_meth bpchar(1), converted bpchar(1), paytax bpchar(1), taxcode bpchar(1), fcode bpchar(1), 
	ftaxcode bpchar(1), custtype bpchar, paid bool, intflag bool, taxtax bool, taxgen bool, taxwty bool, taxsur bool, 
	taxfrt bool, taxlab bool, flocaltax bool, tax1rule1 bool, tax2rule1 bool, taxonly bool, instanceid int4);
	 

	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	--select into sourcedbcount count(*) from dblink(remote_conn,E'select invoice_id from proview.invoices where invoice_id >
	--\'' || (SELECT invoice_id FROM pinnacle_interim_database.invoices ORDER BY invoice_id DESC LIMIT 1) || E'\'') AS P(invoice_id int4);
	select into sourcedbcount count(*) from dblink(remote_conn,'select invoice_id from proview.invoices') AS P(invoice_id int4);

	--insert log table
	--perform pinnacle_interim_database.add_log(db_instance_id,'invoices',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'',(SELECT invoice_id FROM pinnacle_interim_database.invoices ORDER BY invoice_id DESC LIMIT 1));
    
    perform pinnacle_interim_database.add_log(db_instance_id,'invoices',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'',(SELECT max(invoice_id) from pinnacle_interim_database.invoices where instanceid = db_instance_id));
	
	END IF;
	end if;
	
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_invoices',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));
		
commit;

end; 
 $procedure$
;
