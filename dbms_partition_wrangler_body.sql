
CREATE OR REPLACE PACKAGE BODY dbms_partition_wrangler IS

  -- protect against SQL injection
  FUNCTION is_valid_string(
      p_string IN VARCHAR2
  ) RETURN BOOLEAN IS
  BEGIN

  /* For dynamic SQL use:

    l_sql := 'SELECT description FROM open_tab WHERE code = ' ||
              sys.DBMS_ASSERT.ENQUOTE_LITERAL(p_code);

    l_sql := 'SELECT COUNT(*) INTO :l_count FROM ' ||
    sys.DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name);

    EXECUTE IMMEDIATE l_sql INTO l_count;

  */

    -- checks for valid table name chars
    IF regexp_like(p_table_owner,'[A-Za-z0-9_$#]') THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END is_valid_string;

  --write to the alert log
  PROCEDURE log_event(
    p_username  IN VARCHAR2,
    p_action    IN VARCHAR2,
    p_message   IN VARCHAR2
  ) IS
  BEGIN
    dbms_system.ksdwrt(1,'ACTION:'||p_action||' - MESSAGE:'||p_message);
  END log_event;

  --returns the specified setting for the table
  FUNCTION get_parameter(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_parameter       IN VARCHAR2
  ) RETURN VARCHAR2
  IS
    l_return dbms_partition_wrangler_settings.SETTING%TYPE;
  BEGIN

    SELECT SETTING INTO l_return
    FROM dbms_partition_wrangler_settings
    WHERE table_id IN (
      SELECT id
      FROM dbms_partition_wrangler_tabs
      WHERE table_owner = UPPER(p_table_owner)
      AND table_name = UPPER(p_table_name)
    )
    AND PARAMETER_ID IN (
      SELECT ID
      FROM dbms_partition_wrangler_parms
      WHERE PARAMETER_NAME = UPPER(p_parameter)
    );

    RETURN l_return;

  END get_parameter;

  -- generate a lock name based upon table owner and table name
  FUNCTION get_lock_name(
      p_table_owner IN VARCHAR2,
      p_table_name IN VARCHAR2
  ) RETURN VARCHAR2 IS
  BEGIN
    RETURN 'DBMS_P_W_'||UPPER(p_table_owner)||':'||UPPER(p_table_name)
  END get_lock_name;

  -- check if lock exists
  FUNCTION is_locked(
      p_table_owner IN VARCHAR2,
      p_table_name IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count   INTEGER;
    l_lock_id NUMBER;
  BEGIN
    l_lock_id := dbms_lock.allocate_unique(
      get_lock_name(p_table_owner => p_table_owner, p_table_name => p_table_name),
      l_lock_id
    );

    SELECT COUNT(1) INTO l_count
    FROM gv$lock
    WHERE lock_type = 'UL'
    AND id1 = l_lock_id;

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_locked;

  -- internal function to get a lock handle
  -- (private for use by request_lock and release_lock)
  FUNCTION get_lock_handle (
    p_lock_name IN VARCHAR2
  ) RETURN VARCHAR2 IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_lock_handle VARCHAR2(256);
  BEGIN
    DBMS_LOCK.ALLOCATE_UNIQUE(
      p_lock_name     => p_lock_name,
      p_lock_handle   => l_lock_handle,
      expiration_secs => 864000
    ); -- 10 days

    RETURN lock_handle;

  END get_lock_handle;

  PROCEDURE set_lock(
      p_table_owner IN VARCHAR2,
      p_table_name IN VARCHAR2
  ) IS
    l_lock_status NUMBER;
    l_lock_name   VARCHAR2(256);
  BEGIN
    --proceed if unlocked
    --LBYL approach
    IF is_locked(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is already locked by dbms_partition_wrangler');
    END IF;

    l_lock_name := get_lock_name(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --Exclusive Mode, Wait Forever
    l_lock_status := DBMS_LOCK.REQUEST(
       lockhandle         => get_lock_handle(l_lock_name),
       lockmode           => DBMS_LOCK.X_MODE,
       timeout            => DBMS_LOCK.MAXWAIT,
       release_on_commit  => FALSE
     );

     CASE l_lock_status
       WHEN 0 THEN NULL;
       WHEN 2 THEN raise_application_error(-20000,'Deadlock detected');
       WHEN 4 THEN raise_application_error(-20000,'Lock already obtained');
       ELSE raise_application_error(-20000,'Request lock failed: ' || l_lock_status);
     END CASE;

  END set_lock;

  --release lock if held
  PROCEDURE release_lock(
      p_table_owner IN VARCHAR2,
      p_table_name IN VARCHAR2
  ) IS
    l_lock_status NUMBER;
    l_lock_name   VARCHAR2(256);
  BEGIN
    --only proceed if locked
    IF NOT is_locked(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is not locked by dbms_partition_wrangler');
    END IF;

    l_lock_name := get_lock_name(p_table_owner => p_table_owner, p_table_name => p_table_name);

    l_lock_status := DBMS_LOCK.RELEASE(
      lockhandle => get_lock_handle(l_lock_name)
    );

    IF l_lock_status > 0 THEN
      raise_application_error(-20000,'Release lock failed: ' || l_lock_status);
    END IF;

  END release_lock;

  --is table range partitioned
  FUNCTION is_range_partitioned(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count INTEGER;
  BEGIN
    SELECT COUNT(1) INTO l_count
    FROM dba_part_tables
    WHERE table_owner = UPPER(p_table_owner)
    AND table_name = UPPER(p_table_name)
    AND PARTITIONING_TYPE = 'RANGE';

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_range_partitioned;

  --is table managed by dbms_partition_wrangler?
  --checks if table already registered
  --WRONG - should check parameter for managed state
  FUNCTION is_managed_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_val dbms_partition_wrangler_settings.SETTING%TYPE;
  BEGIN

    l_val := get_parameter(
      p_table_owner     => p_table_owner,
      p_table_name      => p_table_name,
      p_parameter       => 'MANAGED'
    );

    IF l_val = 'Y' THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_managed_table;

  --checks if table already registered with dbms_partition_wrangler
  FUNCTION is_registered_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count INTEGER;
  BEGIN
    SELECT COUNT(1) INTO l_count
    FROM dbms_partition_wrangler_tabs
    WHERE table_owner = UPPER(p_table_owner)
    AND table_name = UPPER(p_table_name);

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_registered_table;

--add a table to be managed
--schedules jobs for this table
  PROCEDURE register_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS

  BEGIN
   --set lock
    --check if table already registered
    IF is_managed_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is already registered');
    END IF;

    --check if table exists and is range partitioned
    IF NOT is_range_partitioned(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table must be range partitoned');
    END IF;

    --add entry for table
    INSERT INTO dbms_partition_wrangler_settings (
      table_owner,
      table_name
    )
    VALUES (
      UPPER(p_table_owner),
      UPPER(p_table_name)
    );
    COMMIT;

    --add mandatory and default settings for table
    /*
    modify_table(
     p_table_owner =>
     p_table_name =>
     p_parameter =>
     p_value =>
   );
   */

   log_event(
     p_username   => USER,
     p_action     => 'REGISTER_TABLE',
     p_message    => UPPER(p_table_owner)||'.'||UPPER(p_table_name)
   );

    --remove lock

  END register_table;

  --remove a table from being managed
  --schedules jobs for this table
  PROCEDURE unregister_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN
    --check if table already registered
    IF NOT is_managed_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is not registered');
    END IF;

    --check if table exists and is range partitioned
    IF NOT is_range_partitioned(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table must be range partitoned');
    END IF;

    --remove all jobs
    remove_all_jobs(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --delete the table settings
    DELETE FROM dbms_partition_wrangler_settings
    WHERE table_owner = UPPER(p_table_owner)
    AND table_name = UPPER(p_table_name);
    COMMIT;

    --delete the table entry
    DELETE FROM dbms_partition_wrangler_tabs
    WHERE table_owner = UPPER(p_table_owner)
    AND table_name = UPPER(p_table_name);
    COMMIT;

  END unregister_table;


  --set parameters for a registered table
  --should pass in a PLSQL table of 2 parameters instead
  PROCEDURE modify_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_parameter       IN VARCHAR2,
    p_value           IN VARCHAR2
  ) IS
  BEGIN
    NULL;
  END modify_table;

  /*
  Places table in managed state:
  1. Sets managed flag
  2. Removes scheduled jobs for table using remove_all_jobs()
  3. Re-schedules jobs for this table using schedule_all_jobs()
  */
  PROCEDURE set_managed_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN
    --sets lock
    set_lock();

    --checks
    is_valid_string();

    --sets managed flag
    modify_table(

    );

    --in case of a malfunction, remove all jobs in case they exist to avoid duplication
    --EAFP approach - it's easier to ask forgiveness than permission
    BEGIN
      remove_all_jobs(p_table_owner => p_table_owner, p_table_name => p_table_name);
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    --schedule jobs
    schedule_all_jobs(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --log action
    log_event();

    --release_lock
    release_lock();

  END set_managed_table;

/*
Places table in unmanaged state:
1. Sets unmanaged flag
2. Removes scheduled jobs for table using remove_all_jobs()
*/
PROCEDURE set_unmanaged_table(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

/*
Top-level procedure to run the add pre-allocated partitions and drop inactive partitions:
1. Calls add_partition()
2. Calls drop_partitions()
*/
PROCEDURE process_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

--Add pre-allocated partition, with associated tablespace if required
PROCEDURE add_partition(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

--Drop inactive historical partitions, with associated tablespaces if required
PROCEDURE drop_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

--report the configuration for the table using dbms_output
PROCEDURE report_table(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

/*
Procedure returns output using dbms_output to report the table partition status
Checks:
1. Preallocated partitions exist
2. Number of inactive partitions <=  specified limit
3. Compression enforced for inactive partitions
4. ASM disk group correct for inactive partitions
5. Tablespace set read-only for inactive partitions
6. Active partition exists
7. Cbeck DBMS_SCHEDULER jobs are configured and running
8. Checks default mandatory parameters are set

Return Code 0 means all checks passed
Non-Zero means errors.
*/

PROCEDURE check_table(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2,
  p_return_code     OUT INTEGER
);

/*
Reads the table metadata to advise recommended settings using dbms_output
Checks:
1. Compression size estimates
2. Read-only tablespaces utilization
3. Partition and Tablespace mappings
4. Tablespace settings for partitions
5. Preallocated partitions
6. RMAN stored configuration settings (optimization ON, does not check SECTION SIZE) (not table specific)
*/

PROCEDURE get_recommendations(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

--procedure to archive inactive partitions (set read-only, ASM move, compress)
PROCEDURE archive_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
);

--counts preallocated partitions for a specified managed table
FUNCTION count_preallocated_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
) RETURN INTEGER;

--counts historical partitions for a specified managed table
FUNCTION count_historical_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
) RETURN INTEGER;

--counts active partitions for a specified managed table
FUNCTION count_active_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
) RETURN INTEGER;

--counts active + preallocated + historical partitions for a specified managed table
FUNCTION count_all_partitions(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2
) RETURN INTEGER;

--returns the build version of DBMS_PARTITION_WRANGLER
FUNCTION get_version RETURN VARCHAR2;
--is_table_owner?
--invokers rights so that you only modify your table?

--PRIVATE: procedure to set a managed tablespace to read-only mode
PROCEDURE set_readonly(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2,
  p_tablespace_name IN VARCHAR2
) IS
BEGIN
END set_readonly;

--PRIVATE: procedure to set a managed tablespace to read-write mode
PROCEDURE set_readwrite(
  p_table_owner     IN VARCHAR2,
  p_table_name      IN VARCHAR2,
  p_tablespace_name IN VARCHAR2
) IS
BEGIN
END set_readwrite;

/*
PRIVATE PROCEDURES:
log_event
set_lock
release_lock
audit_access
create_tablespace
encrypt_tablespace
drop_tablespace
shrink_tablespace
truncate_partition
drop_partition
create_partition
move_tablespace
compress_partition
uncompress_partition
submit_job
remove_job
schedule_all_jobs
remove_all_jobs
is_backup_optimized

PRIVATE FUNCTIONS:
can_be_partitioned
get_next_date
is_table
is_managed_tablespace
is_range_partitioned
is_preallocated
is_active_partition
is_inactive_partition
is_tablespace
has_expired_partitions
find_expired_partitions
is_expired_partition
get_db_version
get_range_unit
is_table_owner
get_date_suffix
get_partition_name
get_buffer_pool
is_buffer_pool
is_db_block_size
get_id --of what?
is_asm_diskgroup
get_tablespace_asm_diskgroup
get_active_tablespace
get_inactive_tablespace
is_asm_diskgroup_tablespace
is_partition_locked

*/
/* Moving data files online with ALTER DATABASE MOVE DATAFILE */

/* Create a new disk group DATA2 using ASMCA, ASMCMD, or SQL */
/* Then create appropriate directories in the DATA2 disk group */

--ALTER DISKGROUP data2 ADD DIRECTORY '+DATA2/ORCL';

--ALTER DISKGROUP data2 ADD DIRECTORY '+DATA2/ORCL/DATAFILE';

/* Move the EXAMPLE data file in DATA to EXAMPLE_STORAGE in DATA2
ALTER DATABASE MOVE DATAFILE '+DATA/ORCL/DATAFILE/EXAMPLE.266.798707687'
      TO '+DATA2/ORCL/DATAFILE/EXAMPLE_STORAGE';

*/

  --Map owner and table to job
  PROCEDURE remove_job(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
    l_job_exists number;
  BEGIN
    SELECT count(1) INTO l_job_exists
    FROM dba_scheduler_jobs
    WHERE job_name = '?';

    IF l_job_exists = 1 THEN
      dbms_scheduler.drop_job(job_name => '?');
    END IF;
  END remove_job;

END dbms_partition_wrangler;
/

CREATE PUBLIC SYNONYM dbms_partition_wrangler FOR dbms_partition_wrangler;
