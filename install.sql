WHENEVER SQLERROR EXIT 1;
WHENEVER OSERROR EXIT 1;

CONNECT / AS SYSDBA

PROMPT dbms_partition_ranger_tab: Tables
@dbms_partition_ranger_tab

PROMPT dbms_partition_ranger_spec: Package Specification
@dbms_partition_ranger_spec

PROMPT dbms_partition_ranger_body: Package Body
@dbms_partition_ranger_body

PROMPT dbms_partition_ranger_sec: Security
@dbms_partition_ranger_sec
