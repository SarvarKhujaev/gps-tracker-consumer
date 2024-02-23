package com.ssd.mvd.constants;

public class CassandraFunctions {
    public static final String NOW = "now()";
    public static final String UUID = "uuid()";
    public static final String TO_TIMESTAMP = "toTimestamp( %s )";

    public static final String WITH_CLUSTERING_ORDER = "WITH CLUSTERING ORDER BY ( %s );";
}
