# stats of all indexes for a given schema
# naturally this can also be used for stats for all indexes in a table in the schema 
# or stats for a specific index in a table in the schema 

"""
SELECT
    relname,
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch 
FROM
    pg_stat_all_indexes
WHERE
    schemaname = 'college'
ORDER BY
    relname,
    indexrelname;
"""

# stats of all tables in a given schema 
# can be used to get the stats for a a table in the schema 

"""
SELECT
    relname,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_tup_hot_upd,
    n_live_tup,
    n_dead_tup,
    n_mod_since_analyze,
    n_ins_since_vacuum,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    vacuum_count,
    autovacuum_count,
    analyze_count, 
    autoanalyze_count
FROM
    pg_stat_all_tables
WHERE
    schemaname = 'college'
ORDER BY
    relname;
"""

# TODO 
# - use pg_stats? to get how long queries take to execute along with the timestamp
# - use pgstats? to get open transactions
# - use pg_stats to get more possible api endpoints
# - how can we store this efficiently in the file system
# - enforce error checking (API side only??)
