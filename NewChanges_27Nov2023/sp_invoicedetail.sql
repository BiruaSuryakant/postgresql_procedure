CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_invoicedetail(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
declare cnt record;
  	tableIdVrbl integer;
    v_sqlstate text;
    v_message text;
	v_context text; 
  	sourceidvrbl integer; 
 	assignsourceid record;
    sourcedbcount integer;
  	cust_insertedRow_count integer; 
 	cust_deletedRows_count integer;
	cust_insertedRow_count_log integer;
	table_name varchar(50) = 'invoicedetail';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp))	and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then
	--This block will execute for the first time for all the record from remote databse
	IF (select count(*) from pinnacle_interim_database.invoicedetail where instanceid = db_instance_id) = 0 THEN 
	insert into pinnacle_interim_database.invoicedetail(invoicedetail_id,
	invoice_id, disporder, detatiltype, inventory_id, "unique", itemtext, price, tax1, tax2, taxcode, taxrate,
	taxonly, taxtext, part, icnumber, icver, iccomments, model, modelyear, vstockno, vin, tag, odoreading,
	newpart, corepart, multiloc, wosalerec, foundprice, quoteprice, orderprice, detailnumber, itemnumber,
	parentnumber, reason, altindex, instanceid)
	select * from dblink(remote_conn, CONCAT('select invoicedetail_id,
	invoice_id, disporder, detatiltype, inventory_id, "unique", itemtext, price, tax1, tax2, taxcode, taxrate,
	taxonly, taxtext, part, icnumber, icver, iccomments, model, modelyear, vstockno, vin, tag, odoreading,
	newpart, corepart, multiloc, wosalerec, foundprice, quoteprice, orderprice, detailnumber, itemnumber,
	parentnumber, reason, altindex,',db_instance_id,' from proview.invoicedetail'))
	AS P(invoicedetail_id int4, invoice_id int4, disporder int4, detatiltype text, inventory_id int4,
	"unique" int8, itemtext text, price numeric(10, 2), tax1 numeric(10, 2), tax2 numeric(10, 2),
	taxcode bpchar(3), taxrate numeric(6, 3), taxonly bool, taxtext varchar(20), part bpchar(2),
	icnumber bpchar(6), icver bpchar(1), iccomments text, model bpchar(4), modelyear int4, vstockno varchar(8),
	vin varchar(17), tag varchar(10), odoreading int4, newpart bool, corepart bool, multiloc int4, wosalerec varchar(10),
	foundprice numeric(10, 2), quoteprice numeric(10, 2), orderprice numeric(10, 2), detailnumber int4,
	itemnumber int4, parentnumber int4, reason varchar(50), altindex varchar, instanceid int4);

	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;

	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select invoicedetail_id from proview.invoicedetail')
	AS P(invoicedetail_id int4);

	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'invoicedetail',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'',(SELECT max(invoicedetail_id) from pinnacle_interim_database.invoicedetail));

	else
	insert into pinnacle_interim_database.invoicedetail(invoicedetail_id,
	invoice_id, disporder, detatiltype, inventory_id, "unique", itemtext, price, tax1, tax2, taxcode, taxrate,
	taxonly, taxtext, part, icnumber, icver, iccomments, model, modelyear, vstockno, vin, tag, odoreading,
	newpart, corepart, multiloc, wosalerec, foundprice, quoteprice, orderprice, detailnumber, itemnumber,
	parentnumber, reason, altindex, instanceid)
	select * from dblink(remote_conn, CONCAT('select invoicedetail_id,
	invoice_id, disporder, detatiltype, inventory_id, "unique", itemtext, price, tax1, tax2, taxcode,
	taxrate, taxonly, taxtext, part, icnumber, icver, iccomments, model, modelyear, vstockno, vin, tag,
	odoreading, newpart, corepart, multiloc, wosalerec, foundprice, quoteprice, orderprice, detailnumber,
	itemnumber, parentnumber, reason, altindex,',db_instance_id,E' from proview.invoicedetail
	where invoicedetail_id > \'' || (SELECT max(invoicedetail_id) from pinnacle_interim_database.invoicedetail where instanceid = db_instance_id) || E'\''))
	AS P(invoicedetail_id int4, invoice_id int4, disporder int4, detatiltype text, inventory_id int4,
	"unique" int8, itemtext text, price numeric(10, 2), tax1 numeric(10, 2), tax2 numeric(10, 2),
	taxcode bpchar(3), taxrate numeric(6, 3), taxonly bool, taxtext varchar(20), part bpchar(2),
	icnumber bpchar(6), icver bpchar(1), iccomments text, model bpchar(4), modelyear int4, vstockno varchar(8),
	vin varchar(17), tag varchar(10), odoreading int4, newpart bool, corepart bool, multiloc int4, wosalerec varchar(10),
	foundprice numeric(10, 2), quoteprice numeric(10, 2), orderprice numeric(10, 2), detailnumber int4,
	itemnumber int4, parentnumber int4, reason varchar(50), altindex varchar, instanceid int4);

	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;

	--Getting number of records from source db which being inserted into interm database

	select into sourcedbcount count(*) from dblink(remote_conn,E'select invoicedetail_id	
	from proview.invoicedetail where invoicedetail_id > 
	\'' || (SELECT max(invoicedetail_id) from pinnacle_interim_database.invoicedetail) || E'\'')
    AS P(invoicedetail_id int4);

	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'invoicedetail',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'',(SELECT max(invoicedetail_id) from pinnacle_interim_database.invoicedetail where instanceid = db_instance_id));

	END IF;

	end if;
	
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_invoicedetail',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));
	
		
commit; 
end; 
 $procedure$
;
