CREATE TABLE pinnacle_interim_database.log_history (
	logid int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	sourceid int4 NULL,
	sourcename varchar(100) NULL,
	tableid int4 NULL,
	tablename varchar(100) NULL,
	lastsuccessfulldatetime timestamp NULL,
	lastinsertedrecord int4 NULL,
	sourcerecordcount int4 NULL,
	numberofinsertedrecords int4 NULL,
	numberofdeletedrecords int4 NULL,
	numberofupdatedrecords int4 NULL,
	returncode text NULL,
	createddate timestamp NULL,
	errordescription text NULL,
	CONSTRAINT log_history_pkey1 PRIMARY KEY (logid),
	CONSTRAINT fk_source FOREIGN KEY (sourceid) REFERENCES pinnacle_interim_database.source_db_mapping(sourceid),
	CONSTRAINT fk_tables FOREIGN KEY (tableid) REFERENCES pinnacle_interim_database.source_table_mapping(tableid)
); 