ALTER SESSION SET "_ORACLE_SCRIPT"=true;
CREATE USER NEOBANK IDENTIFIED BY neobank;
GRANT CONNECT, RESOURCE, DBA TO NEOBANK;