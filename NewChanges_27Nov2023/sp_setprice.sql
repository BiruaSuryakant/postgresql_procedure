CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_setprice(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'setprice';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then

	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.setprice where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT;
	
	--This will fetch the data from remote database and insert into intermediate database
	insert into pinnacle_interim_database.setprice(ic_no,
	part_code, price, price2, price_fact, price_date, price2date, core_buy, core, core_fact,
	core_date, list, priceupdn, coreupdn, recommend, surplus, model, newcost, new_date,
	npmarkup1, npmarkup2, nminlevel, minprice, instanceid)
	select * from dblink(remote_conn,CONCAT('select ic_no,
	part_code, price, price2, price_fact, price_date, price2date, core_buy, core, core_fact,
	core_date, list, priceupdn, coreupdn, recommend, surplus, model, newcost, new_date,
	npmarkup1, npmarkup2, nminlevel, minprice,', db_instance_id,' from proview.setprice'))
	AS P(ic_no bpchar(6), part_code bpchar(2), price numeric, price2 numeric(14, 2),
	price_fact numeric, price_date date, price2date date, core_buy numeric(14, 2),
	core numeric, core_fact numeric(5, 2), core_date date, list numeric, priceupdn bpchar(1),
	coreupdn bpchar(1), recommend bpchar(3), surplus numeric(14, 2), model bpchar(4),
	newcost numeric, new_date date, npmarkup1 numeric(5, 2), npmarkup2 numeric(5, 2),
	nminlevel int4, minprice numeric, instanceid int4);
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database
	select into sourcedbcount count(*) from dblink(remote_conn,'select ic_no from proview.setprice')
	AS P(ic_no bpchar(6));
	
	--insert log table
	perform pinnacle_interim_database.add_log(db_instance_id,'setprice',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');

	end if;
 
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
    v_sqlstate = returned_sqlstate,
    v_message = message_text,
    v_context = pg_exception_context;
    perform pinnacle_interim_database.add_log(db_instance_id,'sp_setprice',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 			
commit;
end; 
 $procedure$
;
