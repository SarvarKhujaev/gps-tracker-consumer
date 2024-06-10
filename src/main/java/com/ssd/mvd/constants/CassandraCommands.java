package com.ssd.mvd.constants;

/*
хранит все команды связанные с БД
*/
public final class CassandraCommands {
    public static String SELECT_ALL = "SELECT * FROM";
    public static String INSERT_INTO = "INSERT INTO";
    public static String DELETE = "DELETE FROM";
    public static String UPDATE = "UPDATE";

    public static String CREATE_KEYSPACE = "CREATE KEYSPACE";
    public static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS";
    public static String CREATE_INDEX = "CREATE INDEX IF NOT EXISTS ON";
    public static String CREATE_TYPE = "CREATE TYPE IF NOT EXISTS";


    public static String IF_EXISTS = "IF EXISTS;";
    public static String IF_NOT_EXISTS = "IF NOT EXISTS;";

    public static String BEGIN_BATCH = "BEGIN BATCH;";
    public static String APPLY_BATCH = "APPLY BATCH;";

    /*
    When upserting data if any columns are missing from the JSON, the value in the missing column is overwritten with null (by default)

    EXAMPLE:
        INSERT INTO cycling.cyclist_category JSON '{
              "category" : "GC",
              "points" : 780,
              "id" : "829aa84a-4bba-411f-a4fb-38167a987cda",
              "lastname" : "SUTHERLAND"
              }';

    https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useInsertJSON.html
     */
    public static final String JSON = "JSON";

    /*
    Use the DEFAULT UNSET option to only overwrite values found in the JSON string:
     */
    public static final String DEFAULT_UNSET = "DEFAULT UNSET";

    /*
    To set the TTL for data, use the USING TTL keywords. The TTL function may be used to retrieve the TTL information.

    The USING TTL keywords can be used to insert data into a table for a specific duration of time.
    To determine the current time-to-live for a record, use the TTL function.

    https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useTTL.html <- link for docs
     */
    public static final String USING_TTL = "USING TTL %d";

    /*
    Create a Change Data Capture (CDC) log on the table.

    cdc = TRUE | FALSE
     */
    public static final String CDC = "cdc = %s";

    public static final String WITH_COMMENT = "comment = '%s'";
    public static final String WITH_COMPACTION = """
            compaction = { 'class' : '%s' }
            """;

    public static final String WITH_CLUSTERING_ORDER = "WITH CLUSTERING ORDER BY ( %s )";

    public static final String WITH_COMPRESSION = """
            COMPRESSION = {
                'class' : '%s',
                'chunk_length_in_kb' : %d
            }
            """;

    public static final String WITH_CACHING = """
            caching = {
                 'keys' : '%s',
                 'rows_per_partition' : '%s'
            }
            """;

    /*
    TTL FUNCTIONS

    https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useTTL.html <- link for docs
    */
    public static final String WITH_TTL = "default_time_to_live = %d";

    public static final String MEM_TABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms = %d";
}
