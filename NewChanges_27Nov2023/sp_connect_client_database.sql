CREATE OR REPLACE PROCEDURE pinnacle_interim_database.sp_connect_client_database()
 LANGUAGE plpgsql
AS $procedure$
declare
  	id_instance int;
  	testcount int; 
  	increasedtime time; 
  	decreasedtime time;
  	systemtime time;
    v_sqlstate text;
    v_message text;
    v_context text;
    cursor_instanceid REFCURSOR;
	rec_instanceid   record;
	cursor_failed_instanceid REFCURSOR;
	failed_instanceid  int;
	failure_code varchar(10) = 'Failure';
	success_code varchar(50) = 'Success';
	start_time varchar(50) = ' 00:00:00';
	end_time varchar(50) = ' 23:59:59';
begin
		raise notice 'starting procedure';
		-- Job will select the instance based on sheduled time between +-10 minutes 
		systemtime = (select cast(to_char(now(), 'HH24:MI') as time));		
		increasedtime = (select systemtime + (10 ||' minutes')::interval);		
		decreasedtime = (select systemtime - (10 ||' minutes')::interval);
	
		open cursor_instanceid for select instanceid from pinnacle_interim_database.source_db_mapping
		where "Hour" between decreasedtime and increasedtime;
		loop
	    	-- fetch row into the cursor instance
	      	fetch cursor_instanceid into id_instance;
	    	-- exit when no more row to fetch
	      	exit when not found;
	      	
	      	if(select count(*) from pinnacle_interim_database.job_instance_history lh 
			where lh.instance_id = id_instance
			and lh.status = success_code and lh.dateofexecution between (select cast((concat(cast(now() as date),start_time)) as timestamp)) 
			and (select cast((concat(cast(now() as date),end_time)) as timestamp))) = 0 then
				raise notice 'calling the main procedure';
			call pinnacle_interim_database.sp_execute_interim_databaseoperation(id_instance, cast(now()
			as date));
			end if;
   		end loop;  
   		-- close the cursor
   		close cursor_instanceid;
   	
   		
   		open cursor_failed_instanceid for select instance_id from pinnacle_interim_database.job_instance_history
   		lh where lh.status = failure_code and lh.dateofexecution between (select cast((concat(cast(now() as date),
   		start_time)) as timestamp)) and (select cast((concat(cast(now() as date),end_time)) as timestamp));
		loop
	    	-- fetch row into the failed cursor instance
	      	fetch cursor_failed_instanceid into failed_instanceid;
	    	-- exit when no more row to fetch
	      	exit when not found;
			call pinnacle_interim_database.sp_execute_interim_databaseoperation(failed_instanceid, cast(now()
			as date));      
   		end loop; 
   		-- close the cursor
   		close cursor_failed_instanceid;
   	
		exception
	 	when others then
	 	GET STACKED DIAGNOSTICS
        v_sqlstate = returned_sqlstate,
        v_message = message_text,
        v_context = pg_exception_context;
       	perform pinnacle_interim_database.add_log(id_instance,'sp_connect_client_database',0,0,0,
       	'Failure',0,concat(v_sqlstate,v_message,v_context));    
	 	
		
		commit;
end; 
$procedure$
;
