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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.util.JdbcUtils;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

  public BulkTableQuerier(QueryMode mode, String name, String schemaPattern, String topicPrefix, String transactionLevel, String partitionColumn, String keyColumn) {
    super(mode, name, topicPrefix, schemaPattern, transactionLevel, partitionColumn, keyColumn);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    StringBuilder builder = new StringBuilder(getTransactionLevelString());

    switch (mode) {
      case TABLE:
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);
        builder.append("SELECT * FROM " + JdbcUtils.quoteString(name, quoteString));
        break;
      case QUERY:
        builder.append(query);
        break;
    }
    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = DataConverter.convertRecord(schema, resultSet);
    // TODO: key from primary key? partition?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                             JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }

    int partitionValue = 0;
    if (!partitionColumn.isEmpty()) {
      switch (schema.field(partitionColumn).schema().type()) {
        case INT16:
          partitionValue = record.getInt16(partitionColumn);
          break;
        case INT32:
          partitionValue = record.getInt32(partitionColumn);
          break;
        case INT64:
          partitionValue = record.getInt64(partitionColumn).intValue();
          break;
        case STRING:
          partitionValue = Integer.parseInt(record.getString(partitionColumn));
          break;
      }
    }

    log.info("TableQuerier key: {}, partition: {}, record: {}", keyColumn.isEmpty() ? "null" : keyColumn, (partitionColumn.isEmpty() ? "null" : (partitionColumn + "(" + partitionValue + ")")), record.toString());
    return new SourceRecord(
      partition,
      null,
      topic,
      partitionColumn.isEmpty() ? null : partitionValue,
      keyColumn.isEmpty() ? null : schema.field(keyColumn).schema(),
      keyColumn.isEmpty() ? null : record.get(keyColumn),
      record.schema(),
      record
    );
  }

  @Override
  public String toString() {
    return "BulkTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           '}';
  }

}
