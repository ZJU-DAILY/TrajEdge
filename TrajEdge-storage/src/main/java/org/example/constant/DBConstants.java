package org.example.constant;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * @author Haocheng Wang
 * Created on 2022/10/23
 */
public class DBConstants {
  // Tables
  public static final String META_TABLE_NAME = "trajspark_db_meta";
  public static final String META_TABLE_COLUMN_FAMILY = "meta";
  public static final String META_TABLE_INDEX_META_QUALIFIER = "index_meta";
  public static final String META_TABLE_CORE_INDEX_META_QUALIFIER = "main_table_index_meta";
  public static final String META_TABLE_DESC_QUALIFIER = "desc";

  // INDEX TABLE COLUMNS
  public static final String DATA_TABLE_SUFFIX = "_data";
  public static String INDEX_TABLE_CF = "cf0";
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes(INDEX_TABLE_CF);

  // Bulk load
  public static final String BULK_LOAD_TEMP_FILE_PATH_KEY = "import.file.output.path";
  public static final String BULK_LOAD_INPUT_FILE_PATH_KEY = "import.process.input.path";
  public static final String BULKLOAD_TARGET_INDEX_NAME = "bulkload.target.index.name";
  public static final String BULKLOAD_TEXT_PARSER_CLASS = "bulkload.parser.class";
  public static final String ENABLE_SIMPLE_SECONDARY_INDEX = "enable.simple.secondary.index";

  // Connection
  public static final String OPEN_CONNECTION_FAILED = "Cannot connect to data base.";
  public static final String CLOSE_CONNECTION_FAILED = "Close connection failed.";

  // Initial
  public static final String INITIAL_FAILED = "Initial failed.";

  // Time zone
  public static final ZoneId TIME_ZONE = ZoneOffset.UTC;
}
