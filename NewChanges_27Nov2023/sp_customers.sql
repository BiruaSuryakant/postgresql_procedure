CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_customers(IN db_instance_id integer, IN remote_conn character varying, IN prm_date timestamp without time zone)
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
	table_name varchar(50) = 'customers';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
	
	if(select count(*) from pinnacle_interim_database.log_history lh where lh.sourceid = db_instance_id 
	and lh.tablename = table_name
	and lh.returncode = success_code and lh.createddate between (select cast((concat(cast(prm_date as date),
	start_time)) as timestamp)) and (select cast((concat(cast(prm_date as date),end_time)) as timestamp))) = 0 then
	
	--This will delete all the records from intermediate database based on instanceid
	delete from pinnacle_interim_database.customers where instanceid = db_instance_id;
	GET DIAGNOSTICS cust_deletedRows_count = ROW_COUNT;

	--This will fetch the data from remote database and insert into intermediate database	
	insert into pinnacle_interim_database.customers("name",
	shortcode, bline1, bline2, bline3, bline4, bzip, bstate, bcontact, bemail, bphone1, bname1,
	bphone2, bname2, phone3, bname3, bfax, bdelzone, creditlim, creditcash, balance, custcat,
	"hold", disc1, disc2, disc3, disc4, disc5, taxexempt, accounting, site, custrecord, pblink,
	account, interest, taxno, notes, taxdate, open_bal, open_date, converted, traceid, salesp,
	noneu, country, stateday, statementemail, instanceid)
	select * from dblink(remote_conn,CONCAT('select "name",
	shortcode, bline1, bline2, bline3, bline4, bzip, bstate, bcontact, bemail, bphone1, bname1,
	bphone2, bname2, phone3, bname3, bfax, bdelzone, creditlim, creditcash, balance, custcat,
	"hold", disc1, disc2, disc3, disc4, disc5, taxexempt, accounting, site, custrecord, pblink,
	account, interest, taxno, notes, taxdate, open_bal, open_date, converted, traceid, salesp,
	noneu, country, stateday, statementemail,',
	db_instance_id,' from proview.customers'))
	AS P("name" varchar(45), shortcode varchar(6), bline1 varchar(40), bline2 varchar(40),
	bline3 varchar(40),	bline4 varchar(40), bzip varchar(10), bstate bpchar(3), bcontact varchar(50),
	bemail text, bphone1 varchar(20), bname1 varchar(25), bphone2 varchar(20), bname2 varchar(25),
	phone3 varchar(20),	bname3 varchar(25), bfax varchar(20), bdelzone varchar(2),
	creditlim numeric(10, 2), creditcash bpchar(1), balance numeric, custcat text,
	"hold" bpchar(1), disc1 numeric(5, 2), disc2 numeric(5, 2), disc3 numeric(5, 2),
	disc4 numeric(5, 2), disc5 numeric(5, 2), taxexempt text, accounting bpchar(1),
	site int4, custrecord int4, pblink int4, account varchar(10), interest bool,
	taxno varchar(20), notes text, taxdate date, open_bal numeric(14, 2), open_date date,
	converted bpchar(1), traceid varchar(16), salesp varchar, noneu bool, country bpchar(2),
	stateday int4, statementemail varchar(60), instanceid int4);
	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	
	--Getting number of records from source db which being inserted into interm database	
	select into sourcedbcount count(*) from dblink(remote_conn,'select name from proview.customers')
	AS P("name" varchar(45));
	
	--insert log table		
	perform pinnacle_interim_database.add_log(db_instance_id,'customers',sourcedbcount,
	cust_insertedRow_count,cust_deletedRows_count,'Success',0,'');
	
	end if;
 
	exception
	when others then
	rollback;
	GET STACKED DIAGNOSTICS
	v_sqlstate = returned_sqlstate,
	v_message = message_text,
	v_context = pg_exception_context;
	perform pinnacle_interim_database.add_log(db_instance_id,'sp_customers',0,0,0,'Failure',0,concat(v_sqlstate,v_message,v_context));

		
commit;
end; 
 $procedure$
;
