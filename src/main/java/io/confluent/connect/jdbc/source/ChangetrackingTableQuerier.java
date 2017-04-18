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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.util.JdbcUtils;

/**
 * <p>
 *   ChangetrackingTableQuerier performs incremental loading of changetracking data given a table name
 * </p>
 */
public class ChangetrackingTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(ChangetrackingTableQuerier.class);

  private static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  private String incrementingColumn;
  private ChangetrackingOffset offset;
  private String dynamicSql;

  public ChangetrackingTableQuerier(QueryMode mode, String name, String topicPrefix, String incrementingColumn,
                                           Map<String, Object> offsetMap, Long timestampDelay,
                                           String schemaPattern, Connection connection) {
    super(mode, name, topicPrefix, schemaPattern);
    this.incrementingColumn = incrementingColumn;
    this.offset = ChangetrackingOffset.fromMap(offsetMap);
    Statement dynamicConnectorStatement;
    try {
      dynamicConnectorStatement = connection.createStatement();
      String dynamicConnectorQuery = getDynamicConnectorQuery(name);
      ResultSet rs = dynamicConnectorStatement.executeQuery(dynamicConnectorQuery);
      rs.next();
      dynamicSql = rs.getString(1);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      schema = DataConverter.convertSchema(name, resultSet.getMetaData(), true);
    }
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    // Default when unspecified uses an autoincrementing column
    if (incrementingColumn != null && incrementingColumn.isEmpty()) {
      incrementingColumn = JdbcUtils.getAutoincrementColumn(db, schemaPattern, name);
    }

    String quoteString = JdbcUtils.getIdentifierQuoteString(db);

    StringBuilder builder = new StringBuilder();

    switch (mode) {
      case TABLE:
        builder.append(dynamicSql);
        break;
      case QUERY:
        throw new ConnectException("Unable to define custom query in connector properties when using: " + mode.toString());
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode.toString());
    }
    if (incrementingColumn != null) {
      builder.append(" ORDER BY ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" ASC");
    }
    String connectorSql = builder.toString();
    log.debug("{} prepared SQL query: {}", this, connectorSql);
    stmt = db.prepareStatement(connectorSql);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    if (incrementingColumn != null) {
      Long incOffset = offset.getIncrementingOffset();
      stmt.setLong(1, incOffset);
      log.debug("Executing prepared statement with incrementing value = {}", incOffset);
    }
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    final Struct record = DataConverter.convertRecord(schema, resultSet, true);
    offset = extractOffset(schema, record);
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
    return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
  }

  // Visible for testing
  ChangetrackingOffset extractOffset(Schema schema, Struct record) {
    final Long extractedId;
    if (incrementingColumn != null) {
      final Schema incrementingColumnSchema = schema.field(incrementingColumn).schema();
      final Object incrementingColumnValue = record.get(incrementingColumn);
      if (incrementingColumnValue == null) {
        throw new ConnectException("Null value for incrementing column of type: " + incrementingColumnSchema.type());
      } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
        extractedId = ((Number) incrementingColumnValue).longValue();
      } else if (incrementingColumnSchema.name() != null && incrementingColumnSchema.name().equals(Decimal.LOGICAL_NAME)) {
        final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
        if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
          throw new ConnectException("Decimal value for incrementing column exceeded Long.MAX_VALUE");
        }
        if (decimal.scale() != 0) {
          throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
        }
        extractedId = decimal.longValue();
      } else {
        throw new ConnectException("Invalid type for incrementing column: " + incrementingColumnSchema.type());
      }

      // SYS_CHANGE_VERSION must be incrementing.
      Long incrementingOffset = offset.getIncrementingOffset();
      assert incrementingOffset == -1L || extractedId > incrementingOffset;
    } else {
      extractedId = null;
    }

    return new ChangetrackingOffset(extractedId);
  }

  private boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long
           || incrementingColumnValue instanceof Integer
           || incrementingColumnValue instanceof Short
           || incrementingColumnValue instanceof Byte;
  }

  @Override
  public String toString() {
    return "ChangetrackingTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + dynamicSql + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           ", incrementingColumn='" + incrementingColumn + '\'' +
           '}';
  }

  private String getDynamicConnectorQuery(String tblName) {
    return "IF OBJECT_ID('tempdb..#kc_primary_keys') IS NOT NULL " +
      "DROP TABLE #kc_primary_keys " +
      "DECLARE @db_name nvarchar(128);" +
      "SET @db_name = DB_NAME();" +
      "DECLARE @table_name nvarchar(1000); " +
      "SET @table_name = '" + tblName + "'; " +
      "SELECT K.COLUMN_NAME INTO #kc_primary_keys FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME AND K.TABLE_NAME = @table_name AND CONSTRAINT_TYPE = 'PRIMARY KEY'; " +
      "DECLARE @pk_equals varchar(MAX); " +
      "SET @pk_equals = STUFF(( select ' AND CT.' + COLUMN_NAME + ' = P.' + COLUMN_NAME  from #kc_primary_keys FOR XML PATH('') ), 1, 4, ''); " +
      "DECLARE @columns varchar(MAX); DECLARE @result_columns varchar(MAX); " +
      "SET @columns = STUFF(( select ', '+ 'CT.'+ COLUMN_NAME  from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table_name AND COLUMN_NAME IN (SELECT COLUMN_NAME FROM #kc_primary_keys) FOR XML PATH('') ), 1, 1, '') + " +
      "STUFF(( select ' , '+ 'P.'+ COLUMN_NAME  from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table_name AND COLUMN_NAME NOT IN (SELECT COLUMN_NAME FROM #kc_primary_keys) FOR XML PATH('') ), 1, 1, '') + " +
      "', SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION,' +  " +
      "STUFF(( select ', '+ 'CHANGE_TRACKING_IS_COLUMN_IN_MASK (COLUMNPROPERTY(OBJECT_ID(''' + @db_name + '.dbo.' + @table_name + '''), ''' + COLUMN_NAME + ''', ''ColumnId''), SYS_CHANGE_COLUMNS) AS Change_' + COLUMN_NAME  from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table_name FOR XML PATH('') ), 1, 1, ''); " +
      "SELECT 'SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT' + @columns + ' FROM CHANGETABLE (CHANGES ' + @db_name + '.dbo.' + @table_name + ', ?) AS CT LEFT OUTER JOIN ' + @db_name + '.dbo.' + @table_name + ' AS P ON ' + @pk_equals; ";
  }
}
