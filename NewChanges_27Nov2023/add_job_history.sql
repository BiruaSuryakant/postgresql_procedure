CREATE OR REPLACE FUNCTION pinnacle_interim_database.add_job_history(instanceid integer, job_startingtime timestamp without time zone, job_endtime timestamp without time zone, job_status character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$ 
declare 
   cust_insertedRow_count integer;
begin
	if(select count(*) from pinnacle_interim_database.job_instance_history lh 
	where lh.instance_id = instanceid and
		  lh.status = 'Failure' and 
		  lh.dateofexecution between 
		  (select cast((concat(cast(now() as date),' 00:00:00')) as timestamp)) and 
		  (select cast((concat(cast(now() as date),' 23:59:00')) as timestamp))) = 0 then
		  
		insert into pinnacle_interim_database.job_instance_history(
	    instance_id,
	    dateofexecution,
	    starttime,
	    endtime,
	    status)values (instanceid,
	    (select now()),
	    job_startingtime,
	    job_endtime,
	    job_status);
	   	GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
		  
	else	
		update pinnacle_interim_database.job_instance_history set 
		starttime = job_startingtime,
		endtime = job_endtime,
		status = job_status
		where instance_id = instanceid;
		GET DIAGNOSTICS cust_insertedRow_count = ROW_COUNT;
	end if;
  
   return cust_insertedRow_count;  
end;
$function$
;
