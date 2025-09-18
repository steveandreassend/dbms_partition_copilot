WHENEVER SQLERROR EXIT 1;
WHENEVER OSERROR EXIT 1;

CONNECT / AS SYSDBA

PROMPT dbms_partition_copilot_tab: Tables
@dbms_partition_copilot_tab

PROMPT dbms_partition_copilot_spec: Package Specification
@dbms_partition_copilot_spec

PROMPT dbms_partition_copilot_body: Package Body
@dbms_partition_copilot_body

PROMPT dbms_partition_copilot_sec: Security
@dbms_partition_copilot_sec
