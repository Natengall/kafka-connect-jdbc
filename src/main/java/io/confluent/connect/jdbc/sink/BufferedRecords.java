/*
 * Copyright 2016 Confluent Inc.
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
 */

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final String tableName;
  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private SchemaPair currentSchemaPair;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement preparedStatement;
  private PreparedStatementBinder preparedStatementBinder;

  public BufferedRecords(JdbcSinkConfig config, String tableName, DbDialect dbDialect, DbStructure dbStructure, Connection connection) {
    this.tableName = tableName;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    MDC.put("table", tableName);
    MDC.put("connectionUrl", config.connectionUrl);
    MDC.put("connectionClient", connection.toString());
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

    if (currentSchemaPair == null) {
      try {
        currentSchemaPair = schemaPair;
        // re-initialize everything that depends on the record schema
        fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, currentSchemaPair);
        dbStructure.createOrAmendIfNecessary(config, connection, tableName, fieldsMetadata);
        final String insertSql = getInsertSql();
        log.debug("{} sql: {}\nrecord: {}", config.insertMode, insertSql, record);
        preparedStatement = connection.prepareStatement(insertSql);
        preparedStatementBinder = new PreparedStatementBinder(preparedStatement, config.pkMode, schemaPair, fieldsMetadata);
      } catch (Exception e) {
        log.error("Exception on adding record", e);
        return Collections.emptyList();
      }
    }

    final List<SinkRecord> flushed;
    if (currentSchemaPair.equals(schemaPair)) {
      // Continue with current batch state
      records.add(record);
      if (records.size() >= config.batchSize) {
        flushed = flush();
      } else {
        flushed = Collections.emptyList();
      }
    } else {
      // Each batch needs to have the same SchemaPair, so get the buffered records out, reset state and re-attempt the add
      flushed = flush();
      currentSchemaPair = null;
      flushed.addAll(add(record));
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    for (SinkRecord record : records) {
      try {
        preparedStatementBinder.bindRecord(record);
      } catch (Exception e) { }
    }

    int[] updateCounts;
    String connectorName = config.originals().get("name").toString();

    try {
      updateCounts = preparedStatement.executeBatch();
    } catch (BatchUpdateException bue) {
      updateCounts = bue.getUpdateCounts();
      log.error(connectorName + " reported a DB error: " + bue.getMessage() + ";\n" + preparedStatement);
    } catch (NullPointerException npe) {
      updateCounts = new int[]{0};
    }

    List<SinkRecord> failedRecords = new ArrayList<>();
    for (int i = 0; i < updateCounts.length; i++) {
      if (updateCounts[i] != 1) {
        failedRecords.add(records.get(i));
      }
    }

    if (failedRecords.size() > 0) {
      for (SinkRecord record : failedRecords) {
        log.error(connectorName + " - Failed message: {}", record.value());
      }
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.getInsert(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", tableName
          ));
        }
        return dbDialect.getUpsertQuery(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case DELETE:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
                  "Deleting from table '%s' in DELETE mode requires key field names to be known, check the primary key configuration", tableName
          ));
        }
        return dbDialect.getDeleteQuery(tableName, fieldsMetadata.keyFieldNames);
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }
}
