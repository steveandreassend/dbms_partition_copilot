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

  FUNCTION is_parameter(
    p_parameter IN VARCHAR2
  ) RETURN BOOLEAN
    l_count INTEGER;
  BEGIN
    --Checks
    IF NOT is_valid_string(p_string => p_parameter) THEN
      raise_application_error(-20000,'Invalid string for p_parameter');
    END IF;

    SELECT COUNT(1) INTO l_count
    FROM dbms_partition_wrangler_parms
    WHERE PARAMETER_NAME = UPPER(p_parameter);

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_parameter;

  --common function to validate input parameters for a given table
  --avoid duplicating this functionality in each procedure
  FUNCTION check_object_parameters(
    p_table_owner       IN VARCHAR2,
    p_table_name        IN VARCHAR2,
    p_object_type       IN VARCHAR2
  ) RETURN BOOLEAN IS
  BEGIN
    IF p_object_type = 'TABLESPACE' THEN
      IF NOT is_valid_string(p_string => p_table_owner) THEN
        raise_application_error(-20000,'Invalid value for p_table_owner');
      END IF;

      IF NOT is_valid_string(p_string => p_table_name) THEN
        raise_application_error(-20000,'Invalid value for p_table_name');
      END IF;

      IF NOT is_valid_string(p_string => p_tablespace_name) THEN
        raise_application_error(-20000,'Invalid value for p_tablespace_name');
      END IF;

      IF NOT is_tablespace(p_tablespace_name => p_tablespace_name) THEN
        raise_application_error(-20000,'Invalid value for p_tablespace_name');
      END IF;

      IF NOT is_managed_tablespace(p_tablespace_name => p_tablespace_name) THEN
        raise_application_error(-20000,'Tablespace is not managed by DBMS_PARTITION_WRANGLER');
      END IF;

      RETURN TRUE;

    ELSIF p_object_type = 'TABLE' THEN
      IF NOT is_valid_string(p_string => p_table_owner) THEN
        raise_application_error(-20000,'Invalid value for p_table_owner');
      END IF;

      IF NOT is_valid_string(p_string => p_table_name) THEN
        raise_application_error(-20000,'Invalid value for p_table_name');
      END IF;

      --check if table exists and is range partitioned
      IF NOT is_range_partitioned(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
        raise_application_error(-20000,'Table must be range partitioned');
      END IF;

      RETURN TRUE;

    ELSIF p_object_type = 'PARTITION' THEN
      IF NOT is_valid_string(p_string => p_table_owner) THEN
        raise_application_error(-20000,'Invalid value for p_table_owner');
      END IF;

      IF NOT is_valid_string(p_string => p_table_name) THEN
        raise_application_error(-20000,'Invalid value for p_table_name');
      END IF;

      --check if table exists and is range partitioned
      IF NOT is_range_partitioned(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
        raise_application_error(-20000,'Table must be range partitioned');
      END IF;

      --cannot work on a partition if the table isnt registered with DBMS_PARTITION_WRANGLER
      IF NOT is_registered_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
        raise_application_error(-20000,'Table is not registered');
      END IF;

    END IF;

    RETURN FALSE;
  END check_object_parameters;

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

  --checks if parameter can be null
  FUNCTION is_parameter_nullable(
    p_parameter IN VARCHAR2
  ) RETURN BOOLEAN
    l_count INTEGER;
  IS
  BEGIN
    /* REVIEW: does mandatory mean nullable??? or do we newed a nullable setting */
    SELECT COUNT(1) INTO l_count
    FROM dbms_partition_wrangler_parms
    WHERE mandatory = 'Y';

    IF l_count > 0 THEN
      RETURN FALSE;
    ELSE
      RETUR TRUE;
    END IF;
  END is_parameter_nullable;

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
    --check
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

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
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

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

    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --check if table already registered
    IF is_managed_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is already registered');
    END IF;

    --set lock
    set_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --add entry for table
    INSERT INTO DBMS_PARTITION_WRANGLER_TABS (
      ID,
      table_owner,
      table_name
    )
    VALUES (
      SEQ_DBMS_PARTITION_WRANGLER.NEXTVAL,
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
    release_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

  END register_table;

  --remove a table from being managed
  --schedules jobs for this table
  PROCEDURE unregister_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --check if table already registered
    IF NOT is_managed_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is not registered');
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

  --get the ID of a parameter
  FUNCTION get_parameter_id(
    p_parameter IN VARCHAR2
  ) RETURN INTEGER IS
    l_val dbms_partition_wrangler_parms.ID%TYPE;
  BEGIN
    SELECT ID INTO l_val
    FROM dbms_partition_wrangler_parms
    WHERE PARAMETER_NAME = UPPER(p_parameter);

    RETURN l_val;

  END get_parameter_id;

  --get the ID of a table
  FUNCTION get_table_id(
    p_table_owner IN VARCHAR2,
    p_table_name IN VARCHAR2
  ) RETURN INTEGER IS
    l_val dbms_partition_wrangler_tabs.ID%TYPE;
  BEGIN
    SELECT ID INTO l_val
    FROM dbms_partition_wrangler_tabs
    WHERE table_owner = UPPER(p_table_owner)
    AND table_name = UPPER(p_table_name);

    RETURN l_val;

  END get_table_id;

  --set parameters for a registered table
  --should pass in a PLSQL table of 2 parameters instead
  PROCEDURE modify_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_parameter       IN VARCHAR2,
    p_value           IN VARCHAR2 DEFAULT NULL
  ) IS
    l_table_id dbms_partition_wrangler_tabs.ID%TYPE;
    l_parameter_id dbms_partition_wrangler_parms.ID%TYPE;
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    IF NOT is_registered_table(p_table_owner => p_table_owner, p_table_name => p_table_name) THEN
      raise_application_error(-20000,'Table is not registered');
    END IF;

    IF NOT is_valid_string(p_string => p_parameter) THEN
      raise_application_error(-20000,'Invalid value for p_parameter');
    END IF;

    IF p_value IS NOT NULL THEN
      IF NOT is_valid_string(p_string => p_value) THEN
        raise_application_error(-20000,'Invalid value for p_value');
      END IF;
    ELSE
      IF NOT is_parameter_nullable(p_parameter) THEN
        raise_application_error(-20000,'Parameter '||p_parameter||' cannot be null');
      END IF;
    END IF;

    IF NOT is_parameter(p_parameter => p_parameter) THEN
      raise_application_error(-20000,'Invalid parameter specified');
    END IF;

    --get input parameters
    l_parameter_id := get_parameter_id(p_parameter);
    l_table_id := get_table_id(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --add the parameter or update it if it exists
    --UPSERT
    MERGE INTO dbms_partition_wrangler_settings
    USING source_table
      ON search_condition
    WHEN MATCHED THEN
        UPDATE SET SETTING = p_value
        WHERE PARAMETER_ID = l_parameter_id
        AND table_id = l_table_id;
    WHEN NOT MATCHED THEN
        INSERT (col1,col2,...)
        VALUES(value1,value2,...);
    COMMIT;

    --log
    log_event(
      p_username   => USER,
      p_action     => 'MODIFY_TABLE',
      p_message    => UPPER(p_table_owner)||'.'||UPPER(p_table_name)||' Parameter:'||p_parameter||' Value:'||p_value
    );

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

    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --sets lock
    set_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --sets managed flag
    modify_table(
      p_table_owner     => UPPER(p_table_owner),
      p_table_name      => UPPER(p_table_name),
      p_parameter       => 'MANAGED',
      p_value           => 'Y'
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

    --release_lock
    release_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --log action
    log_event(
      p_username   => USER,
      p_action     => 'SET_MANAGED_TABLE',
      p_message    => UPPER(p_table_owner)||'.'||UPPER(p_table_name)
    );

  END set_managed_table;

  /*
  Places table in unmanaged state:
  1. Sets unmanaged flag
  2. Removes scheduled jobs for table using remove_all_jobs()
  */
  PROCEDURE set_unmanaged_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN

    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --sets lock
    set_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --sets managed flag
    modify_table(
      p_table_owner     => UPPER(p_table_owner),
      p_table_name      => UPPER(p_table_name),
      p_parameter       => 'MANAGED',
      p_value           => 'N'
    );

    --in case of a malfunction, remove all jobs in case they exist to avoid duplication
    --EAFP approach - it's easier to ask forgiveness than permission
    BEGIN
      remove_all_jobs(p_table_owner => p_table_owner, p_table_name => p_table_name);
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    --release_lock
    release_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --log action
    log_event(
      p_username   => USER,
      p_action     => 'SET_UNMANAGED_TABLE',
      p_message    => UPPER(p_table_owner)||'.'||UPPER(p_table_name)
    );

  END set_unmanaged_table;

  --whether all specified partitions have been allocated?
  FUNCTION is_preallocated()
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_limit INTEGER;
    l_partition_count INTEGER;
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    l_limit := get_parameter(
      p_table_owner   => UPPER(p_table_owner),
      p_table_name    => UPPER(p_table_name),
      p_parameter     => 'PREALLOCATED_PARTITIONS'
    );

    IF l_limit IS NULL THEN
      raise_application_error(-20000,'Unable to get parameter PREALLOCATED_PARTITIONS');
    END IF;

    --a partition is pre-allocated if the date is higher than SYSDATE
/*
      This: https://dba.stackexchange.com/questions/210004/extract-date-from-partition-high-value
WARNING: extractvalue is deprecated: use XMLTABLE instead:

SELECT warehouse_name warehouse,
   warehouse2."Water", warehouse2."Rail"
   FROM warehouses,
   XMLTABLE('/Warehouse'
      PASSING warehouses.warehouse_spec
      COLUMNS
         "Water" varchar2(6) PATH 'WaterAccess',
         "Rail" varchar2(6) PATH 'RailAccess')
      warehouse2;
*/
    WITH date_partition AS (
    SELECT
      partition_name,
      extractvalue(dbms_xmlgen.getxmltype('SELECT high_value
        FROM DBA_TAB_PARTITIONS
        WHERE owner = ''' || UPPER(p_table_name) ||
        ''' AND table_name = ''' || UPPER(p_table_owner) ||
        ''' AND PARTITION_NAME = ''' || t.PARTITION_NAME || ''''),
        '//text()'
      ) AS high_value
    FROM dba_tab_partitions t
    WHERE owner = UPPER(p_table_name)
    AND table_name = UPPER(p_table_owner)
    ), final_result (
      SELECT partition_name,
             TO_DATE(substr(high_value,11,10),'YYYY-DD-MM') high_value
    FROM date_partition
    )
    SELECT COUNT(1) INTO l_partition_count
    FROM final_result
    WHERE high_value > SYSDATE;

    IF l_count >= l_limit THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_preallocated;

  /*
  Top-level procedure to add, compress, move, readonly, truncate, shrink, drop partititions and tablespaces
  */
  PROCEDURE process_partitions(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --log_event
    log_event(
      p_username   => USER,
      p_action     => 'PROCESS_PARTITIONS',
      p_message    => 'START '||UPPER(p_table_owner)||'.'||UPPER(p_table_name)
    );

    --sets lock
    set_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --determine if new partitions need to be pre-allocated based upon the configured parameter
    WHILE NOT is_preallocated() LOOP
      add_partition(p_table_owner => p_table_owner, p_table_name => p_table_name)
    END LOOP;

    --determine if inactive partitions need to be moved to a different buffer pool

    /* bulk compress partitions once they are [mostly] inactive, and shrink the tablespace storage */
    --determine if inactive partitions need to be compressed

    /* optional feature to move tablespaces to a cheaper slower ASM disk group for archiving
       if all partitions on it are inactive, particularly in the case of rolled up tablespaces */
    --determine if inactive tablespace partitions need to be moved to another ASM disk group

    /* this is the base case, set TS to RO mode once it is no longer active for DML
       this action will prevent ROW MOVEMENT for the partition */
    --determine if inactive tablespace partitions need to be set to READ-ONLY mode

    /* instead of dropping partitions, this option truncates the partition to leave the structure intact */
    --determine if inactive partitions need to be truncated because they are expired, and dropped at a later date
    --determine if inactive tablespace with truncated partitions need to be shrunk?

    /* this is the base case, old partitions older than X are dropped and their tablespace too.
       rolled-up tablespaces (e.g. 1 quartlerly tablespace, 3 monthly partitions) will not be dropped until empty */
    --determine if inactive partitions need to be dropped because they are expired

    --release lock
    release_lock(p_table_owner => p_table_owner, p_table_name => p_table_name);

    --log action
    log_event(
      p_username   => USER,
      p_action     => 'PROCESS_PARTITIONS',
      p_message    => 'END '||UPPER(p_table_owner)||'.'||UPPER(p_table_name)
    );

  END process_partitions;

  --Add pre-allocated partition, with associated tablespace if required
  PROCEDURE add_partition(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) IS
  BEGIN
  END add_partition;

  --Drop inactive historical partitions, with associated tablespaces if required
  PROCEDURE drop_partition(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_partition_name  IN VARCHAR2
  ) IS
    l_restore_point INTEGER;
  BEGIN
    --checks
    PARTITION

    is_partition

    IF NOT is_expired_partition() THEN
      raise_application_error();
    END IF;

    --
    l_restore_point := get_parameter();
    IF l_restore_point = 'Y' THEN
      --create GRP
      create_grp();
    END IF;

    EXECUTE IMMEDIATE 'ALTER TABLE '||p_table_owner||'.'||sys.DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name)||' DROP PARTITION '||p_partition_name;

    --Handle
    IF l_restore_point = 'Y' THEN
      --shrink the tablespace size to the current HWM to free up disk space
      --because the tablespace is dropped at a later date
      shrink_tablespace();

      --Create a job to drop the restore point according to the request
      submit_job();

      --Suspend tablespace dropping by submitting a post-dated job +24 hours after the GRP is dropped
      --Flashback Database requires that the tablespace exist if the GRP is to be used to recovery the dropped partition
      submit_job();
    ELSE
      --direct drop of tablespace if no GRP is used
      drop_tablespace();
    END IF;
  END drop_partition;

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

--is_table_owner?
--invokers rights so that you only modify your table?

  --returns the build version of DBMS_PARTITION_WRANGLER
  FUNCTION get_version RETURN VARCHAR2 IS
  BEGIN
    RETURN g_version;
  END get_version;

  --check if TS RO
  FUNCTION is_readonly(
    p_tablespace_name IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count INTEGER;
  BEGIN
    SELECT COUNT(1) INTO l_count
    FROM dba_tablespaces
    WHERE tablespace_name = UPPER(p_tablespace_name)
    AND STATUS = 'READ ONLY';

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END is_readonly;

  --check if tablespace exists
  FUNCTION is_tablespace(
    p_tablespace_name IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count INTEGER;
  BEGIN
    SELECT COUNT(1) INTO l_count
    FROM dba_tablespaces
    WHERE tablespace_name = UPPER(p_tablespace_name);

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END is_tablespace;

  --check if TS is covered as part of the package
  FUNCTION is_managed_tablespace(
    p_tablespace_name IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count INTEGER;
  BEGIN
    SELECT COUNT(1) INTO l_count
    FROM dba_tablespaces
    WHERE tablespace_name = UPPER(p_tablespace_name)
    AND tablespace_name IN (
      SELECT tablespace_name
      FROM dba_extents
      WHERE (owner,segment_name) IN (
        SELECT table_owner, table_name
        FROM dbms_partition_wrangler_tabs
      )
    ) OR (
      SELECT tablespace_name
      FROM dba_tab_partitions
      WHERE (table_owner,table_name) IN (
        SELECT table_owner, table_name
        FROM dbms_partition_wrangler_tabs
      )
    ) OR (
      SELECT tablespace_name
      FROM DBA_TAB_SUBPARTITIONS
      WHERE (table_owner,table_name) IN (
        SELECT table_owner, table_name
        FROM dbms_partition_wrangler_tabs
      )
    );

    IF l_count > 0 THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;

  END is_managed_tablespace;

  --PRIVATE: procedure to set a managed tablespace to read-only mode
  PROCEDURE set_readonly(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) IS
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_tablespace_name   => UPPER(p_tablespace_name),
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    IF is_readonly(p_tablespace_name) THEN
      raise_application_error(-20000,'Tablespaces already READ-ONLY: '||p_tablespace_name);
    END IF;

    --apply
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||sys.DBMS_ASSERT.SQL_OBJECT_NAME(p_tablespace_name)||' READ ONLY';

    --log
    log_event(
      p_username   => USER,
      p_action     => 'SET_READONLY',
      p_message    => p_tablespace_name
    );

  END set_readonly;

  --check if tablespace is empty
  FUNCTION is_tablespace_empty(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) RETURN BOOLEAN IS
    l_count NUMBER;
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_tablespace_name   => UPPER(p_tablespace_name)
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    SELECT SUM(bytes) INTO l_count
    FROM dba_extents
    WHERE tablespace_name = UPPER(p_tablespace_name);

    IF l_count > 0 THEN
      RETURN FALSE;
    ELSE
      RETURN TRUE;
    END IF;
  END is_tablespace_empty;

  --PRIVATE: procedure to set a managed tablespace to read-write mode
  PROCEDURE set_readwrite(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) IS
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    IF NOT is_readonly(p_tablespace_name) THEN
      raise_application_error(-20000,'Tablespaces already READ-WRITE: '||p_tablespace_name);
    END IF;

    --apply
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||sys.DBMS_ASSERT.SQL_OBJECT_NAME(p_tablespace_name)||' READ WRITE';

    --log
    log_event(
      p_username   => USER,
      p_action     => 'SET_READWRITE',
      p_message    => p_tablespace_name
    );

  END set_readwrite;

  --PRIVATE: procedure to set a managed tablespace to read-write mode
  PROCEDURE drop_tablespace(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) IS
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    IF NOT is_tablespace_empty(p_tablespace_name) THEN
      raise_application_error(-20000,'Tablespace is not empty');
    END IF;

    --do not force drop contents in case of a rolled up tablespace
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||sys.DBMS_ASSERT.SQL_OBJECT_NAME(p_tablespace_name)||' INCLUDING DATAFILES';

    --log
    log_event(
      p_username   => USER,
      p_action     => 'DROP_TABLESPACE',
      p_message    => p_tablespace_name
    );

  END drop_tablespace;

  PROCEDURE shrink_tablespace(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) IS
    l_db_block_size INTEGER;
    l_file_id INTEGER;
    l_hwm INTEGER;
    l_size VARCHAR2(256);
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    --get actual DB_BLOCK_SIZE even if specified
    SELECT block_size INTO l_db_block_size
    FROM dba_tablespaces
    WHERE tablespace_name = UPPER(p_tablespace_name);

    --get file_id
    SELECT file_id
    FROM dba_data_files
    WHERE tablespace_name = UPPER(p_tablespace_name);

    --find highest block ID of any extents in tablespace
    BEGIN
      SELECT MAX(block_id+blocks-1) INTO l_hwm
      FROM dba_extents
      WHERE file_id IN (
        SELECT file_id
        FROM dba_data_files
        WHERE tablespace_name = UPPER(p_tablespace_name)
      );

      l_size := l_hwm * l_db_block_size;

    EXCEPTIONS
      --handle where tablespace is empty by failing to get l_hwm
      WHEN OTHERS THEN
        l_size := NVL(get_parameter(
          p_table_owner   => p_table_owner,
          p_table_name    => p_table_name,
          p_parameter     => 'TABLESPACE_MINSIZE'
        ),1*l_db_block_size);
    END;

    --apply
    EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '||l_file_id||' RESIZE '||l_size;

    --log
    log_event(
      p_username   => USER,
      p_action     => 'SHRINK_TABLESPACE',
      p_message    => p_tablespace_name
    );

  END shrink_tablespace;

  --offline encryption of tablespace
  PROCEDURE encrypt_tablespace(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_tablespace_name IN VARCHAR2
  ) IS
    l_val dbms_partition_wrangler_settings.SETTING%TYPE;
  BEGIN
    --checks
    IF NOT check_object_parameters(
      p_table_owner       => UPPER(p_table_owner),
      p_table_name        => UPPER(p_table_name),
      p_object_type       => 'TABLESPACE')
    ) THEN
      --this will never trigger because the function will trigger first it validation fails
      raise_application_error(-20000,'Invalid parameters');
    END IF;

    -- get encryption mode
    l_val := get_parameter(
      p_table_owner   => p_table_owner,
      p_table_name    => p_table_name,
      p_parameter     => 'TABLESPACE_ENCRYPTION'
    );

    --could/should force AES256
    IF l_val IS NULL THEN
      raise_application_error(-20000,'Unable to retrieve encryption mode');
    END IF;

    --apply
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||p_tablespace_name||' OFFLINE NORMAL';
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||p_tablespace_name||' ENCRYPTION OFFLINE USING '''||l_val||''' ENCRYPT';
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||p_tablespace_name||' ONLINE';

    --log
    log_event(
      p_username   => USER,
      p_action     => 'ENCRYPT_TABLESPACE',
      p_message    => p_tablespace_name
    );

  END encrypt_tablespace;

/*
PRIVATE PROCEDURES:
log_event
set_lock
release_lock
audit_access
create_tablespace
drop_tablespace
shrink_tablespace
encrypt_tablespace
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
is_partition_locked -- this needed?

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
