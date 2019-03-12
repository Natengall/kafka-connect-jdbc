/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {
  private static final Logger log = LoggerFactory.getLogger(TableQuerier.class);

  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final QueryMode mode;
  protected final String schemaPattern;
  protected final String name;
  protected final String query;
  protected final String topicPrefix;

  // Mutable state
  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected Schema schema;
  protected String partitionColumn;
  protected String keyColumn;
  protected String keyFormat;
  protected String transactionLevel;
  protected String tag;

  static final String TRANSACTION_LEVEL_TEMPLATE = "SET TRANSACTION ISOLATION LEVEL ";

  public TableQuerier(QueryMode mode, String nameOrQuery,
          String topicPrefix, String schemaPattern, JdbcSourceConnectorConfig config) {
    this.mode = mode;
    this.schemaPattern = schemaPattern;
    this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
    this.transactionLevel = config.getString(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_LEVEL_CONFIG);
    this.partitionColumn = config.getString(JdbcSourceConnectorConfig.PARTITION_COLUMN_NAME_CONFIG);
    this.keyColumn = config.getString(JdbcSourceConnectorConfig.KEY_COLUMN_NAME_CONFIG);
    this.keyFormat = config.getString(JdbcSourceConnectorConfig.KEY_FORMAT_CONFIG);
    this.tag = config.getString(JdbcSourceConnectorConfig.TAG_CONFIG);
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    if (stmt != null) {
      return stmt;
    }
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(CachedConnectionProvider cachedConnectionProvider) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(cachedConnectionProvider.getValidConnection());
      resultSet = executeQuery();

      if (resultSet.last()) {
        int rowCount = resultSet.getRow();
        log(topicPrefix + (name == null ? "" : name) + " fetched " + rowCount + " rows.");
        resultSet.beforeFirst(); // not rs.first() because the rs.next() below will move on, missing the first element
      }
      schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  public void reset(long now) {
    closeResultSetQuietly();
    closeStatementQuietly();
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    schema = null;
    lastUpdate = now;
  }

  private void closeStatementQuietly() {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignored) {
      }
    }
    stmt = null;
  }

  private void closeResultSetQuietly() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException ignored) {
      }
    }
    resultSet = null;
  }

  @Override
  public int compareTo(TableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.name.compareTo(other.name);
    }
  }

  protected String getTransactionLevelString() {
    if (transactionLevel == null) {
      return "";
    }
    switch (transactionLevel) {
      case JdbcSourceTaskConfig.TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED:
        return TRANSACTION_LEVEL_TEMPLATE + "READ UNCOMMITTED; ";
      case JdbcSourceTaskConfig.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED:
        return TRANSACTION_LEVEL_TEMPLATE + "READ COMMITTED; ";
      case JdbcSourceTaskConfig.TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ:
        return TRANSACTION_LEVEL_TEMPLATE + "REPEATABLE READ; ";
      case JdbcSourceTaskConfig.TRANSACTION_ISOLATION_LEVEL_READ_SNAPSHOT:
        return TRANSACTION_LEVEL_TEMPLATE + "SNAPSHOT; ";
      case JdbcSourceTaskConfig.TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE:
        return TRANSACTION_LEVEL_TEMPLATE + "SERIALIZABLE; ";
      default:
        return "";
    }
  }

  protected void log(String msg, Object... arg) {
    if (tag != null) {
      log.info("[" + tag + "]" + msg, arg);
    }
  }
}