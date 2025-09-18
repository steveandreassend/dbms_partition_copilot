WHENEVER SQLERROR EXIT 1;
WHENEVER OSERROR EXIT 1;

--Check user is SYS
BEGIN
  IF USER != 'SYS' THEN
    RAISE_APPLICATION_ERROR(-20000,'Must be run a SYS');
  END IF;
END;
/

DECLARE
  l_cnt INTEGER;
BEGIN
  /* LBYL approach */
  SELECT COUNT(1)
  FROM user_tables
  WHERE table_name = UPPER('DBMS_PARTITION_COPILOT_TABS');

  IF l_cnt > 0 THEN
    EXECUTE IMMEDIATE 'DROP TABLE dbms_partition_copilot_tabs CASCADE CONSTRAINTS';
  END IF;
END;
/

DECLARE
  l_cnt INTEGER;
BEGIN
  /* LBYL approach */
  SELECT COUNT(1)
  FROM user_tables
  WHERE table_name = UPPER('DBMS_PARTITION_COPILOT_PARMS');

  IF l_cnt > 0 THEN
    EXECUTE IMMEDIATE 'DROP TABLE dbms_partition_copilot_parms CASCADE CONSTRAINTS';
  END IF;
END;
/

DECLARE
  l_cnt INTEGER;
BEGIN
  /* LBYL approach */
  SELECT COUNT(1)
  FROM user_tables
  WHERE table_name = UPPER('DBMS_PARTITION_COPILOT_SETTINGS');

  IF l_cnt > 0 THEN
    EXECUTE IMMEDIATE 'DROP TABLE dbms_partition_copilot_settings CASCADE CONSTRAINTS';
  END IF;
END;
/

CREATE TABLE dbms_partition_copilot_tabs (
  ID INTEGER PRIMARY KEY,
  TABLE_OWNER VARCHAR2(128) NOT NULL,
  TABLE_NAME VARCHAR2(128) NOT NULL,
  LAST_MANAGED_DATE TIMESTAMP,
  LAST_UNMANAGED_DATE TIMESTAMP,
  LAST_CHANGE_USER VARCHAR(128)
) TABLESPACE USERS;

ALTER TABLE dbms_partition_copilot_tabs ADD CONSTRAINT uq_dbms_partition_copilot UNIQUE (TABLE_OWNER, TABLE_NAME) ENABLE;

CREATE OR REPLACE TRIGGER trg_dbms_partition_copilot_tabs_ins_upd
BEFORE INSERT OR UPDATE ON dbms_partition_copilot_tabs
FOR EACH ROW
BEGIN
  :NEW.TABLE_NAME := UPPER(:NEW.TABLE_NAME);
  :NEW.TABLE_OWNER := UPPER(:NEW.TABLE_OWNER);
  :NEW.LAST_CHANGE_USER := UPPER(:NEW.LAST_CHANGE_USER);
END TRG_dbms_partition_copilot_tabs_ins_upd;
/

CREATE SEQUENCE seq_dbms_partition_copilot;

CREATE TABLE dbms_partition_copilot_parms (
  ID               INTEGER PRIMARY KEY,
  PARAMETER_NAME   VARCHAR2(64) NOT NULL,
  DESCRIPTION      VARCHAR2(256),
  PERMITTED_VALUES VARCHAR2(512),
  REGEX_STRING     VARCHAR2(128),
  MANDATORY        CHAR(1), --does this mean it is nullable?
  CHANGEABLE       CHAR(1)
) TABLESPACE USERS;

ALTER TABLE dbms_partition_copilot_parms
ADD CONSTRAINT ck1_dbms_partition_copilot_parms CHECK (MANDATORY IN ('Y','N') ENABLE;

ALTER TABLE dbms_partition_copilot_parms
ADD CONSTRAINT ck2_dbms_partition_copilot_parms CHECK (CHANGEABLE IN ('Y','N') ENABLE;

CREATE INDEX ind2_dbms_partition_copilot_parms
ON dbms_partition_copilot_parms (parameter_name)
TABLESPACE USERS;

CREATE OR REPLACE TRIGGER trg_dbms_partition_copilot_parms_ins_upd
BEFORE INSERT OR UPDATE ON dbms_partition_copilot_parms
FOR EACH ROW
BEGIN
  IF :NEW.PERMITTED_VALUES IS NOT NULL AND :NEW.REGEX_STRING IS NOT NULL THEN
    raise_application_error(-20000, 'Specify either the string literal permitted values or the regexp to validate input, not both');
  END IF;
END TRG_dbms_partition_copilot_parms_ins_upd;
/

ALTER TABLE dbms_partition_copilot_parms
ADD CONSTRAINT UQ_dbms_partition_copilot_parms
UNIQUE (PARAMETER_NAME)
ENABLE;

CREATE SEQUENCE seq_dbms_partition_copilot_parms;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'MANAGED',
  'Defines whether a table is being actively managed by dbms_partition_copilot',
  'Y|N',
  'Y',
  'Y'
);
COMMIT;

/*
Guidance for TABLE_RANGE_TYPE:

Typically WEEKLY or MONTHLY is the optimal setting.
DAILY is suitable for really large tables.
QUARTERLY is suitable for smaller tables. This option is not recommended.
--Future feature: RANGE plus HASH composite partitions so that WEEKLY or MONTHLY can be as granular as DAILY

DAILY creates partitions with ${NAME_PREFIX}_$DDD_YYYY.
e.g. ${NAME_PREFIX}_001_2023 holds records for 1st January 2023

WEEKLY creates partitions with ${NAME_PREFIX}_$WW_YYYY.
This counts every 7 days, it is not for a Sun-Sat week.
e.g. ${NAME_PREFIX}_01_2023 holds records for 1st-7th January 2023

MONTHLY creates partitions with ${NAME_PREFIX}_$MON_YYYY.
e.g. ${NAME_PREFIX}_JAN_2023 holds records for 1st-31st January 2023

QUARTERLY creates partitions with ${NAME_PREFIX}_$QQ_YYYY.
e.g. ${NAME_PREFIX}_Q1_2023 holds records for 1st January 2023 to 31st March 2023
*/

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TABLE_RANGE_TYPE',
  'Defines the time window for the range partitioning for the table',
  'DAILY|WEEKLY|MONTHLY|QUARTERLY',
  'Y',
  'N'
);
COMMIT;

/*
Guidance:

Rule: TABLE_RANGE_TYPE frequency must be <= TABLESPACE_RANGE_TYPE

It is recommended to set TABLE_RANGE_TYPE = TABLESPACE_RANGE_TYPE so that there is a 1:1 mapping for partitions to tablespaces.

TABLESPACE_RANGE_TYPE allows multiple partitions to be stored in the same tablespace to reduce the number of datafiles.
e.g.
  DAY_OF_YEAR{001-007} partitions can be rolled up to be stored in WEEK01 tablespace for the corresponding year
  WEEK{01-05} of JAN can be rolled up to be stored in the JAN tablespace
  MONTH{JAN, FEB, MAR} partitions can be stored in Q1 tablespace
*/
INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TABLESPACE_RANGE_TYPE',
  'Defines the time window for the range partitioning for the tablespace',
  'DAILY|WEEKLY|MONTHLY|QUARTERLY',
  'Y',
  'N'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TAB_ACTIVE_BUFFER_POOL',
  'Database buffer cache to be used to store active table partitions',
  'DEFAULT|KEEP|RECYCLE',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TAB_INACTIVE_BUFFER_POOL',
  'Database buffer cache to be used to store inactive table partitions',
  'DEFAULT|KEEP|RECYCLE',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'IND_ACTIVE_BUFFER_POOL',
  'Database buffer cache to be used to store active index partitions',
  'DEFAULT|KEEP|RECYCLE',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'IND_INACTIVE_BUFFER_POOL',
  'Database buffer cache to be used to store inactive index partitions',
  'DEFAULT|KEEP|RECYCLE',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'DB_BLOCK_SIZE',
  'The DB_BLOCK_SIZE of the tablespaces storing the table',
  '2048|4096|8192|16384|32768',
  'N',
  'N'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'ACTIVE_ASM_DISKGROUP',
  'ASM Diskgroup for active partitions',
  '^+[A-Za-z0-9_]',
  'Y',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'INACTIVE_ASM_DISKGROUP',
  'ASM Diskgroup for inactive partitions',
  '^+[A-Za-z0-9_]',
  'Y',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'PREALLOCATED_PARTITIONS',
  'The number of partitions to preallocate',
  '[0-9]',
  'Y',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'INACTIVE_PARTITIONS',
  'The number of partitions to retain once they are no longer active',
  '[0-9]',
  'Y',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'PROCESS_JOB_ID',
  'ID of the DBMS_SCHEDULER job to add, modify, and remove partitions',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'PROCESS_JOB_NAME',
  'Name of the DBMS_SCHEDULER job to add, modify, and remove partitions',
  '[A-Za-z0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'CHECK_JOB_ID',
  'ID of the DBMS_SCHEDULER job to check partition health',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'CHECK_JOB_NAME',
  'Name of the DBMS_SCHEDULER job to check partition health',
  '[A-Za-z0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'ARCHIVE_JOB_ID',
  'ID of the DBMS_SCHEDULER job to archive inactive partitions',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'ARCHIVE_JOB_NAME',
  'Name of the DBMS_SCHEDULER job to archive inactive partitions',
  '[A-Za-z0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'COMPRESS_DELAY_DAYS',
  'After how many days after a partition becomes inactive is it compressed, if configured',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'READONLY_DELAY_DAYS',
  'After how many days after a partition becomes inactive is it made READ-ONLY, if configured',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

/*
ALL_TAB_PARTITIONS
ALL_TAB_SUBPARTITIONS

COMPRESSION:
  NONE - The partition is composite, and a default setting is not specified for compression.
  ENABLED - The setting for compression is enabled.
  DISABLED - The setting for compression is disabled.

COMPRESS_FOR:
  BASIC
  ADVANCED
  QUERY LOW
  QUERY HIGH
  ARCHIVE LOW
  ARCHIVE HIGH
  NULL
*/

/*
DBA_TABLESPACES

  DEF_TAB_COMPRESSION
    ENABLED - The setting for compression is enabled.
    DISABLED - The setting for compression is disabled.

  DEF_INDEX_COMPRESSION
  ENABLED - The setting for compression is enabled.
  DISABLED - The setting for compression is disabled.

  COMPRESS_FOR:
    BASIC
    ADVANCED
    QUERY LOW
    QUERY HIGH
    ARCHIVE LOW
    ARCHIVE HIGH
    NULL

  INDEX_COMPRESS_FOR
    ADVANCED LOW
    ADVANCED HIGH
    NULL

  CREATE TABLE sales (
      prod_id     NUMBER     NOT NULL,
      cust_id     NUMBER     NOT NULL, ... )
   PCTFREE 5 NOLOGGING NOCOMPRESS
   PARTITION BY RANGE (time_id)
   ( partition sales_2013 VALUES LESS THAN(TO_DATE(...)) ROW STORE COMPRESS BASIC,
     partition sales_2014 VALUES LESS THAN (MAXVALUE) ROW STORE COMPRESS ADVANCED );
*/

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'COMPRESSION_INACTIVE',
  'Compression options for inactive table partitions',
  'NOCOMPRESS|COMPRESS|COMPRESS FOR DIRECT_LOAD OPERATIONS|COMPRESS FOR ALL OPERATIONS',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,PERMITTED_VALUES,MANDATORY,CHANGEABLE)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'COMPRESSION_ACTIVE',
  'Compression options for active table partitions',
  'NOCOMPRESS|COMPRESS|COMPRESS FOR DIRECT_LOAD OPERATIONS|COMPRESS FOR OLTP',
  'N',
  'Y'
);
COMMIT;

/*
NOCOMPRESS - The table or partition is not compressed. This is the default action when no compression clause is specified.
COMPRESS - This option is considered suitable for data warehouse systems. Compression is enabled on the table or partition during direct-path inserts only.
COMPRESS FOR DIRECT_LOAD OPERATIONS - This option has the same affect as the simple COMPRESS keyword.
COMPRESS FOR ALL OPERATIONS - This option is considered suitable for OLTP systems. As the name implies, this option enables compression for all operations, including regular DML statements. This option requires the COMPATIBLE initialization parameter to be set to 11.1.0 or higher. In 11gR2 this option has been renamed to COMPRESS FOR OLTP and the original name has been deprecated. This option requires the Advanced Compression license.

SELECT table_name, partition_name, compression, compress_for FROM user_tab_partitions;

Will be used at a partition and tablespace level as follows:

a) Compression enabled for new active partitions:
CREATE BIGFILE TABLESPACE xyz_1 SIZE 1G DEFAULT COMPRESS;
ALTER TABLE ADD PARTITION xyy_1 VALUES LESS THAN TO_DATE(?) COMPRESS

b) Compression disabled for active partitions, enabled for inactive partitions:
When the partition becomes inactive, the job will:
ALTER TABLESPACE xyz_1 DEFAULT COMPRESS;
ALTER TABLE MODIFY PARTITION xyy_1 COMPRESS;
ALTER TABLE MOVE PARTITION xyy_1 NOLOGGING COMPRESS PARALLEL (DEGREE ?) UPDATE INDEXES;
*/

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TABLESPACE_MINSIZE',
  'The starting size for BIGFILE tablespaces with the unit (M,G,T), e.g. 100M or 100G or 1T',
  '[0-9][M|G|T]$',
  'Y',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'NAME_PREFIX',
  'The unique string for this table to prefix the name of all partitions and tablespaces',
  '[A-Za-z]',
  'Y',
  'N'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'DDL_PARALLEL_DEGREE',
  'The degree of parallelism for all DDL operations on the partitions',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'READONLY_INACTIVE',
  'When partitions become inactive set the tablespace to READ-ONLY mode',
  'Y|N',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'TABLESPACE_ENCRYPTION',
  'If set, the TDE encryption mode used when creating new tablespaces',
  'AES128|AES192|AES256|GOST256|SEED128|3DES168',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'REDO_LOGGING',
  'Whether to disable redo log generation during DDL and DML',
  'LOGGING|NOLOGGING',
  'N',
  'Y'
);
COMMIT;

/*
GRP capability to allow you to undo the partition drop.

If CREATE_GRP=Y, it will drop the partition, but the dedicated tablespace will be dropped only once the GRP has been removed.

If using PDBs you’ve got this problem that you must allow drop tablespace with GRPs by setting a parameter:
CDB$ROOT: alter system set "_allow_drop_ts_with_grp"=true;
Setting that parameter will mean that a dropped tablespace will invalidate any GRPs and your ability to recover.
ref: https://mikedietrichde.com/2018/06/05/drop-a-tablespace-in-a-pdb-with-a-guaranteed-restore-point-being-active/
*/

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'CREATE_GRP',
  'Whether to create a temporary Guaranteed Restore Point prior to dropping a partition',
  'Y|N',
  'N',
  'Y'
);
COMMIT;

INSERT INTO dbms_partition_copilot_parms
(ID, PARAMETER_NAME,DESCRIPTION,REGEX_STRING)
VALUES (
  seq_dbms_partition_copilot_parms.NEXTVAL,
  'GRP_DURATION_MINS',
  'Number of minutes after dropping the partition to drop the Guaranteed Restore Point',
  '[0-9]',
  'N',
  'Y'
);
COMMIT;

CREATE TABLE dbms_partition_copilot_settings (
  ID INTEGER PRIMARY KEY,
  PARAMETER_ID INTEGER NOT NULL,
  TABLE_ID INTEGER NOT NULL,
  SETTING VARCHAR2(256),
  LAST_CHANGE_DATE TIMESTAMP,
  LAST_CHANGE_USER VARCHAR(128)
) TABLESPACE USERS;

CREATE INDEX ind2_dbms_partition_copilot_settings
ON dbms_partition_copilot_settings (PARAMETER_ID)
TABLESPACE USERS;

CREATE INDEX ind3_dbms_partition_copilot_parms
ON dbms_partition_copilot_settings (TABLE_ID)
TABLESPACE USERS;

ALTER TABLE dbms_partition_copilot_settings ADD CONSTRAINT fk1_dbms_partition_copilot_settings FOREIGN KEY (PARAMETER_ID) REFERENCES dbms_partition_copilot_parms (ID) ENABLE;

ALTER TABLE dbms_partition_copilot_settings ADD CONSTRAINT fk2_dbms_partition_copilot_settings
FOREIGN KEY (TABLE_ID) REFERENCES dbms_partition_copilot_tabs (ID) ENABLE;
