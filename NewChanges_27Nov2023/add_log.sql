CREATE OR REPLACE FUNCTION pinnacle_interim_database.add_log(srcid integer, tblname character varying, totalsourcecount integer, totalinserted integer, totaldeleted integer, returncode text, totalupdatedrecord integer, errdesc text, lastinserted integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$ 
declare 
   cust_insertedRow_count integer; 
begin
	if (select count(*) from pinnacle_interim_database.source_table_mapping where pinnacle_interim_database.source_table_mapping.tablename = tblname) > 0 then
   	insert into pinnacle_interim_database.log_history(sourceid,
    sourcename,
    tableid,
    tablename,
    lastsuccessfulldatetime, 
    sourcerecordcount,
    numberofinsertedrecords,
    numberofdeletedrecords,
    numberofupdatedrecords,
    returncode,
    createddate,errordescription,lastinsertedrecord)values (srcid,
    (select description from pinnacle_interim_database.source_db_mapping where sourceid = srcid),
    (select tableid from pinnacle_interim_database.source_table_mapping where pinnacle_interim_database.source_table_mapping.tablename = tblname),
    tblname, 
    (SELECT NOW()),
    totalsourcecount,
    totalinserted,
    totaldeleted,
    totalupdatedrecord,
    returncode,
    (SELECT NOW()),errdesc, lastinserted);
   	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
  	else
  	   insert into pinnacle_interim_database.log_history(sourceid,
    sourcename,
    tableid,
    tablename,
    lastsuccessfulldatetime, 
    sourcerecordcount,
    numberofinsertedrecords,
    numberofdeletedrecords,
    numberofupdatedrecords,
    returncode,
    createddate,errordescription,lastinsertedrecord)values (srcid,
    (select description from pinnacle_interim_database.source_db_mapping where sourceid = srcid),
    1,
    tblname, 
    (SELECT NOW()),
    totalsourcecount,
    totalinserted,
    totaldeleted,
    totalupdatedrecord,
    returncode,
    (SELECT NOW()),errdesc,lastinserted);
   	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
  
  	end if;
  
   return cust_insertedRow_count;
end;
$function$
;
