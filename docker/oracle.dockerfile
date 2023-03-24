FROM container-registry.oracle.com/database/enterprise:21.3.0.0

USER root

# Create a new directory for custom scripts
RUN mkdir /opt/oracle/custom_scripts && \
    chown oracle:oinstall /opt/oracle/custom_scripts

COPY oracle/ddl/tables.sql /home/oracle/custom_scripts/1_tables.sql
COPY oracle/data/sample_data.sql /home/oracle/custom_scripts/2_sample_data.sql
COPY oracle/subprograms/procedures.sql /home/oracle/custom_scripts/3_procedures.sql
COPY docker/oracle/run_sql_scripts.sh /home/oracle/custom_scripts/run_sql_scripts.sh

RUN chmod +x /home/oracle/custom_scripts/run_sql_scripts.sh

USER oracle