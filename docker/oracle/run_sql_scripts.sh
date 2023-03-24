#!/bin/bash

sqlplus sys/oracle@localhost:1521/ORCLCDB as sysdba <<-EOF
  ALTER SESSION SET "_ORACLE_SCRIPT"=true;
  CREATE USER NEOBANK IDENTIFIED BY neobank;
  GRANT CONNECT, RESOURCE, DBA TO NEOBANK;
EOF

for file in /home/oracle/scripts/*.sql
do
  echo "Executing ${file}"
  sqlplus NEOBANK/neobank@localhost:1521/ORCLCDB @${file}
done
