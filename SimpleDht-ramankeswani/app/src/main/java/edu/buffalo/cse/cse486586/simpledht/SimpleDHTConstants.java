package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by Keswani on 2/25/2018.
 */

public class SimpleDHTConstants {
    public static final String DB_NAME = "SimpleDhtProvider.db";
    public  static final int DB_VERSION = 1;
    public static final String TABLE_NAME = "provider";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";
    public static final String URI = "edu.buffalo.cse.cse486586.simpledht.provider";
    public static final int MESSAGE_JOIN_RING = 1;
    public static final int MESSAGE_UPDATE_TABLE = 2;
    public static final int MESSAGE_FIND_TO_INSERT = 3;
    public static final int MESSAGE_NOTIFY_PARENT_CHANGED = 4;
    public static final int MESSAGE_FINAL_INSERT = 5;
    public static final int MESSAGE_FIND_KEY = 6;
    public static final int MESSAGE_FIND_KEY_REPLY = 7;
    public static final int MESSAGE_GET_ALL_DATA = 8;
    public static final int MESSAGE_GET_ALL_DATA_REPLY = 9;
    public static final int MESSAGE_DELETE_LOC_FINAL = 10;
    public static final int MESSAGE_DELETE = 11;
    public static final int MESSAGE_DELETE_EVERYTHING = 12;
    public static final String INSERT_ON_SELF = "INSERT_ON_SELF";
    public static final String INSERT_ON_PARENT = "INSERT_ON_PARENT";
    public static final String QUERY_ON_SELF = "QUERY_ON_SELF";
    public static final String QUERY_ON_PARENT = "QUERY_ON_PARENT";
    public static final String DELETE_FROM_SELF = "DELETE_FROM_SELF";
    public static final String DELETE_FROM_PARENT = "DELETE_FROM_PARENT";
    public static final String ownPartition = "@";
    public static final String allNodes = "*";
    public static final String DELETION_PERFORMED_ON_OTHER = "DELETION_PERFORMED_ON_OTHER";
}
