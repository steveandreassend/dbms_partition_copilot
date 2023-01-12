CREATE OR REPLACE PACKAGE dbms_partition_wrangler IS

  g_version VARCHAR2(10) := '1.0';

  --add a table to be managed
  --schedules jobs for this table
  PROCEDURE register_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  );

  --remove a table from being managed
  --schedules jobs for this table
  PROCEDURE unregister_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  );

  --is table managed by dbms_partition_wrangler?
  FUNCTION is_managed_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  ) RETURN BOOLEAN;

  --set parameters for a registered table
  --should pass in a PLSQL table of 2 parameters instead
  PROCEDURE modify_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_parameter       IN VARCHAR2,
    p_value           IN VARCHAR2 DEFAULT NULL
  );

  --returns the specified setting for the table
  FUNCTION get_parameter(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2,
    p_parameter       IN VARCHAR2
  ) RETURN VARCHAR2;

  /*
  Places table in managed state:
  1. Sets managed flag
  2. Removes scheduled jobs for table using remove_all_jobs()
  3. Re-schedules jobs for this table using schedule_all_jobs()
  */
  PROCEDURE set_managed_table(
    p_table_owner     IN VARCHAR2,
    p_table_name      IN VARCHAR2
  );

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
  Top-level procedure to add, compress, move, readonly, truncate, shrink, drop partititions and tablespaces
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

END dbms_partition_wrangler;
/
