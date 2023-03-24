CREATE OR REPLACE PROCEDURE rename_partitions AS
  v_partition_name VARCHAR2(100);
  v_high_value DATE;
  v_new_name VARCHAR2(100);
  v_sql VARCHAR2(4000);
BEGIN
  FOR r IN (SELECT partition_name, high_value
            FROM user_tab_partitions
            WHERE table_name = 'TRANSACTION') LOOP
    v_partition_name := r.partition_name;
    EXECUTE IMMEDIATE 'SELECT ' || r.high_value || ' FROM DUAL' INTO v_high_value;
    v_new_name := 'TRANSACTION_' || TO_CHAR(v_high_value - INTERVAL '1' DAY, 'YYYY_MM_DD');
    
    -- check if new partition name is different from old partition name
    IF v_new_name != v_partition_name THEN
      v_sql := 'ALTER TABLE transaction RENAME PARTITION ' || v_partition_name || ' TO ' || v_new_name;
      EXECUTE IMMEDIATE v_sql;
    END IF;
  END LOOP;
END;
/
