CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_newstock(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'newstock';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
	cut_off_time varchar(50);
	lastsuccessfullexecuteddate varchar(50)  = '1570-01-01 00:00:00';
begin
    --This code is for fetch the cut off time based on instance id
	cut_off_time = ' ' || (select cast((select cutofftime from pinnacle_interim_database.source_db_mapping where sourceid = db_instance_id) as varchar));

	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp))	and (select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp))) = 0 then
		--This block will execute for the first time for all the record from remote database
		IF (select count(*) from pinnacle_interim_database.newstock where instanceid = db_instance_id) = 0 then

		insert into pinnacle_interim_database.newstock(ic,
		model,part,"year",cc,"unique","condition",bin,miles,"comments",
		vstockno,datein,ene_type,box_type,bod_type,bod_col,yardid,ticket_no,rprice,
		tprice,wprice,dismantler,storeman,checkval,global_acc,status,assembly,holdstatus,repair_hrs,
		objectword,resolve,transmit,costprice,lastpriced,altindex,vpblink,converted,
		multiloc,doublecond,verified,popart,inventoryid,isprivate,checkedon,lastupdate,
		privatenote,instanceid)
		select * from dblink(remote_conn,CONCAT('select ic,model,part,"year",cc,"unique","condition",bin,miles,"comments",
		vstockno,datein,ene_type,box_type,bod_type,bod_col,yardid,ticket_no,rprice,
		tprice,wprice,dismantler,storeman,checkval,global_acc,status,assembly,holdstatus,repair_hrs,
		objectword,resolve,transmit,costprice,lastpriced,altindex,vpblink,converted,
		multiloc,doublecond,verified,popart,inventoryid,isprivate,checkedon,lastupdate,
		privatenote,',db_instance_id,'from proview.newstock')) 
		AS P(ic bpchar(6), model bpchar(4), part bpchar(2), "year" bpchar(4), cc bpchar(4), "unique" int8,
		"condition" bpchar(1), bin varchar, miles int4, "comments" varchar, vstockno varchar(8),
		datein date, ene_type bpchar(1), box_type bpchar(1), bod_type bpchar(1), bod_col varchar(50),
		yardid bpchar(6), ticket_no bpchar(10), rprice numeric(10, 2), tprice numeric(14, 2),
		wprice numeric(14, 2), dismantler varchar, storeman bpchar(3), checkval int8, global_acc bpchar(1),
		status bpchar(1), assembly bool, holdstatus bpchar, repair_hrs numeric(5, 2), objectword int4,
		resolve varchar(5), transmit bool, costprice numeric(10, 2), lastpriced date, altindex bpchar(6),
		vpblink int4, converted bpchar(1), multiloc int4, doublecond bpchar(2), verified date,
		popart bool, inventoryid int4, isprivate bool, checkedon timestamp, lastupdate timestamp,
		privatenote text, instanceid int4);
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;

		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,'select inventoryid from proview.newstock') AS P(inventoryid int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'newstock',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
 
		else

		--Fetch the last successfull date and time, when job was executed
	    lastsuccessfullexecuteddate = (select max(lastsuccessfulldatetime) from pinnacle_interim_database.log_history lh where lh.tablename = table_name and lh.returncode = success_code);
		if lastsuccessfullexecuteddate IS NOT NULL and lastsuccessfullexecuteddate != '1570-01-01 00:00:00' then
		--This block will execute based on when job was successfull executed date
	    --if lastsuccessfullexecuteddate  IS NOT null then	   	
		delete from pinnacle_interim_database.newstock where inventoryid in (select * from dblink(remote_conn,CONCAT
		(E'select inventoryid from proview.newstock where lastupdate between 
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'')) AS P(inventoryid int4));
	
	    raise notice '5 ';
		  
		insert into pinnacle_interim_database.newstock(ic,
		model,part,"year",cc,"unique","condition",bin,miles,"comments",
		vstockno,datein,ene_type,box_type,bod_type,bod_col,yardid,ticket_no,rprice,
		tprice,wprice,dismantler,storeman,checkval,global_acc,status,assembly,holdstatus,repair_hrs,
		objectword,resolve,transmit,costprice,lastpriced,altindex,vpblink,converted,
		multiloc,doublecond,verified,popart,inventoryid,isprivate,checkedon,lastupdate,
		privatenote,instanceid)
		select * from dblink(remote_conn,CONCAT('select ic,model,part,"year",cc,"unique","condition",bin,miles,"comments",
		vstockno,datein,ene_type,box_type,bod_type,bod_col,yardid,ticket_no,rprice,
		tprice,wprice,dismantler,storeman,checkval,global_acc,status,assembly,holdstatus,repair_hrs,
		objectword,resolve,transmit,costprice,lastpriced,altindex,vpblink,converted,
		multiloc,doublecond,verified,popart,inventoryid,isprivate,checkedon,lastupdate,
		privatenote,',db_instance_id,E' from proview.newstock where lastupdate between
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'')) 
		AS P(ic bpchar(6), model bpchar(4), part bpchar(2), "year" bpchar(4), cc bpchar(4), "unique" int8,
		"condition" bpchar(1), bin varchar, miles int4, "comments" varchar, vstockno varchar(8),
		datein date, ene_type bpchar(1), box_type bpchar(1), bod_type bpchar(1), bod_col varchar(50),
		yardid bpchar(6), ticket_no bpchar(10), rprice numeric(10, 2), tprice numeric(14, 2),
		wprice numeric(14, 2), dismantler varchar, storeman bpchar(3), checkval int8, global_acc bpchar(1),
		status bpchar(1), assembly bool, holdstatus bpchar, repair_hrs numeric(5, 2), objectword int4,
		resolve varchar(5), transmit bool, costprice numeric(10, 2), lastpriced date, altindex bpchar(6),
		vpblink int4, converted bpchar(1), multiloc int4, doublecond bpchar(2), verified date,
		popart bool, inventoryid int4, isprivate bool, checkedon timestamp, lastupdate timestamp,
		privatenote text, instanceid int4);
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	    raise notice '6 ';
		--Getting number of records from source db which being inserted into interm database
		select into sourcedbcount count(*) from dblink(remote_conn,E'select inventoryid from proview.newstock where lastupdate between 
		\'' || lastsuccessfullexecuteddate || E'\' and \'' || 
		(select cast((concat(cast(prm_date as date),cut_off_time)) as timestamp)) || E'\'')	AS P(inventoryid int4);
	
		--insert log table
		perform pinnacle_interim_database.add_log(db_instance_id,'newstock',sourcedbcount,cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
        raise notice '7 ';
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
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_newstock',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));
		
commit;
end; 
 $procedure$
;
