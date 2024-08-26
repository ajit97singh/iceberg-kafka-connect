/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.*;

import java.io.IOException;
import java.util.*;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private Map<String, RecordWriter> writers = new HashMap<>();
  private final Map<TopicPartition, Offset> sourceOffsets;
  private static final ObjectMapper objectmapper = new ObjectMapper();

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
  }

  @Override
  public Committable committable() {
    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();

    return new Committable(offsets, writeResults);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));
    if(record.value() instanceof Map && !((Map<?, ?>) record.value()).containsKey("before")) {
      return;
    }
    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName ->
                    config
                        .tableConfig(tableName)
                        .routeRegex()
                        .ifPresent(
                            regex -> {
                              if (regex.matcher(routeValue).matches()) {
                                writerForTable(tableName, record, false).write(record);
                              }
                            }));
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    String table = config.tablesRouteField();
    String db = config.databaseRouteField();
    String dbClusterPrefix = config.dbClusterPrefix();
    String dataLakeName = config.dataLakeName();
    Preconditions.checkNotNull(table, String.format("Table route field cannot be null with dynamic routing at topic: %s, partition: %d, offset: %d", record.topic(), record.kafkaPartition(), record.kafkaOffset()));
    Preconditions.checkNotNull(db, String.format("Database route field cannot be null with dynamic routing at topic: %s, partition: %d, offset: %d", record.topic(), record.kafkaPartition(), record.kafkaOffset()));

    String routeTableValue = extractRouteValue(record.value(), table);
    String routeDbValue = extractRouteValue(record.value(), db);
    String routeValue = dataLakeName + "." + dbClusterPrefix + routeDbValue + "_" + routeTableValue;
    if (routeTableValue != null && !routeTableValue.isEmpty()
            && routeDbValue != null && !routeDbValue.isEmpty()) {
      String tableName = routeValue.toLowerCase();
      writerForTable(tableName, record, true).write(record);

      // Creating an audit record
      if (config.auditTrailEnabled() &&
              config.tablesCdcField() != null &&
              !config.tablesCdcField().isEmpty() &&
              !config.auditTableSuffix().isEmpty()) {
        SinkRecord auditRecord = createAuditRecord(record);
        String auditTableName = tableName + config.auditTableSuffix();
        writerForTable(auditTableName, auditRecord, true).write(auditRecord);
      }
    }
  }


  private SinkRecord createAuditRecord(SinkRecord record) {
    DebeziumPacket debeziumPacket = objectmapper.convertValue(record.value(), DebeziumPacket.class);
    debeziumPacket.setOp(null); // default interpreted operation is insert
      return new SinkRecord(record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            debeziumPacket,
            record.timestamp()
            );
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    if (!writers.containsKey(tableName)) {
      writers.put(tableName, writerFactory.createWriter(tableName, sample, ignoreMissingTable));
    }
    return writers.get(tableName);
  }
}
