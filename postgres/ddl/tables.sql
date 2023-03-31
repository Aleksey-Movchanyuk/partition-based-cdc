

CREATE TABLE snapshot_all_tab_modifications (
    snapshot_dt TIMESTAMP NOT NULL, 
    table_owner VARCHAR(128),
    table_name VARCHAR(128),
    partition_name VARCHAR(128),
    subpartition_name VARCHAR(128),
    inserts INTEGER,
    updates INTEGER,
    deletes INTEGER,
    "timestamp" TIMESTAMP,
    truncated VARCHAR(3),
    drop_segments INTEGER
);

-- Add comments for each column
COMMENT ON COLUMN snapshot_all_tab_modifications.snapshot_dt IS 'Snapshot date and time';
COMMENT ON COLUMN snapshot_all_tab_modifications.table_owner IS 'Owner of the modified table';
COMMENT ON COLUMN snapshot_all_tab_modifications.table_name IS 'Name of the modified table';
COMMENT ON COLUMN snapshot_all_tab_modifications.partition_name IS 'Name of the modified partition';
COMMENT ON COLUMN snapshot_all_tab_modifications.subpartition_name IS 'Name of the modified subpartition';
COMMENT ON COLUMN snapshot_all_tab_modifications.inserts IS 'Approximate number of inserts since the last time statistics were gathered';
COMMENT ON COLUMN snapshot_all_tab_modifications.updates IS 'Approximate number of updates since the last time statistics were gathered';
COMMENT ON COLUMN snapshot_all_tab_modifications.deletes IS 'Approximate number of deletes since the last time statistics were gathered';
COMMENT ON COLUMN snapshot_all_tab_modifications."timestamp" IS 'Indicates the last time the table was modified';
COMMENT ON COLUMN snapshot_all_tab_modifications.truncated IS 'Indicates whether the table has been truncated since the last analyze (YES) or not (NO)';
COMMENT ON COLUMN snapshot_all_tab_modifications.drop_segments IS 'Number of partition and subpartition segments dropped since the last analyze';

