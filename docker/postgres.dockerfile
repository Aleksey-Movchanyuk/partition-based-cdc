FROM postgres:13

# Copy the init.sql file into the /docker-entrypoint-initdb.d/ directory
COPY postgres/init.sql /docker-entrypoint-initdb.d/1_init.sql
COPY postgres/ddl/tables.sql /docker-entrypoint-initdb.d/2_tables.sql