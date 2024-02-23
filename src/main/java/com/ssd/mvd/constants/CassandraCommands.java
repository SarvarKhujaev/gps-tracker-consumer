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
}
