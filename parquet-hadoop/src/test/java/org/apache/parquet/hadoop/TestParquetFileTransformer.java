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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.transform.ColumnTransformerBase;
import org.apache.parquet.hadoop.transform.IntegerColumnTransformer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.*;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.*;

public class TestParquetFileTransformer {

  private static final CompressionCodecName CODEC_NAME = GZIP;
  private static final int BLOCK_SIZE = 1024;
  private static final int PAGE_SIZE = 1024;
  private static final int DICTIONARY_PAGE_SIZE = 512;
  private static final boolean ENABLE_DICTIONARY = true;
  private static final ParquetProperties.WriterVersion WRITER_VERSION = PARQUET_1_0;
  private static final int RECORD_COUNT = 50;

  private final Configuration conf = new Configuration();
  private final MessageType schema = Types.buildMessage()
      .required(INT32).named("id")
      .required(BINARY).as(UTF8).named("uuid")
      .optional(INT32).named("timestamp")
      .named("TransformTest");

  private final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
  private final Random random = new Random();
  private Path testDir, inputFile, outputFile;

  private class Record {

    final int id;
    final String uuid;
    final Integer timestamp;

    Record(int id, String uuid, Integer timestamp) {
      this.id = id;
      this.uuid = uuid;
      this.timestamp = timestamp;
    }

    Group toGroup() {
      Group group = groupFactory.newGroup()
          .append("id", id)
          .append("uuid", uuid);
      if (timestamp != null)
        group.append("timestamp", timestamp);
      return group;
    }
  }

  private Record randomRecord(int i) {
    int id = (i * 10) + random.nextInt(10);
    String uuid = UUID.randomUUID().toString();
    Integer timestamp = (random.nextDouble() > 0.5) ?
        1460000000 + random.nextInt(10000000) : null;
    return new Record(id, uuid, timestamp);
  }

  private void populateSourceFile() throws IOException {
    testDir = new Path("target/tests/TestParquetFileTransformer/");
    enforceEmptyDir(conf, testDir);
    inputFile = new Path(testDir, "input.parquet");

    GroupWriteSupport.setSchema(schema, conf);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(
        inputFile, new GroupWriteSupport(), CODEC_NAME, BLOCK_SIZE, PAGE_SIZE,
        DICTIONARY_PAGE_SIZE, ENABLE_DICTIONARY, false, WRITER_VERSION, conf);

    for (int i = 0; i < RECORD_COUNT; i++)
      writer.write(randomRecord(i).toGroup());

    writer.close();
  }

  private void transformFile() throws IOException {
    List<ColumnDescriptor> columnDescriptors = schema.getColumns();
    ColumnDescriptor idColumn = columnDescriptors.get(0),
        uuidColumn = columnDescriptors.get(1);

    HashMap<ColumnDescriptor, ColumnTransformer> transformers =
        new HashMap<ColumnDescriptor, ColumnTransformer>();

    transformers.put(idColumn, new IntegerColumnTransformer(idColumn) {
      @Override
      protected int transformValue(int value) {
        return 2 * value;
      }
    });

    transformers.put(uuidColumn, new ColumnTransformerBase(uuidColumn) {
      @Override
      protected void processValue(int repetitionLevel, int definitionLevel, ColumnReader reader, ColumnWriter writer) {
        Binary value = reader.getBinary();
        Binary transformedValue = Binary.fromString(value.toStringUsingUTF8().substring(0, 8));
        writer.write(transformedValue, repetitionLevel, definitionLevel);
      }
    });

    ParquetMetadata footer = ParquetFileReader.readFooter(conf, inputFile, NO_FILTER);
    List<BlockMetaData> blocks = footer.getBlocks();

    ParquetFileTransformer transformer = new ParquetFileTransformer(
        conf, schema, transformers, PAGE_SIZE, CODEC_NAME, DICTIONARY_PAGE_SIZE,
        ENABLE_DICTIONARY, WRITER_VERSION);
    outputFile = new Path(testDir, "output.parquet");
    transformer.transformFile(inputFile, blocks, outputFile);
  }

  @Before
  public void setUp() throws Exception {
    populateSourceFile();
    transformFile();
  }

  @Test
  public void testMetadata() throws Exception {
    ParquetMetadata inputFooter = ParquetFileReader.readFooter(conf, inputFile, NO_FILTER);
    ParquetMetadata outputFooter = ParquetFileReader.readFooter(conf, outputFile, NO_FILTER);

    List<BlockMetaData> inputBlocks = inputFooter.getBlocks();
    List<BlockMetaData> outputBlocks = outputFooter.getBlocks();
    assertEquals(inputBlocks.size(), outputBlocks.size());

    for (BlockMetaData inputBlock : inputBlocks) {
      BlockMetaData outputBlock = outputBlocks.remove(0);
      assertEquals(inputBlock.getRowCount(), outputBlock.getRowCount());
      assertColumnsEquivalent(inputBlock.getColumns(), outputBlock.getColumns());
    }
  }

  private void assertColumnsEquivalent(List<ColumnChunkMetaData> expected,
                                       List<ColumnChunkMetaData> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i += 1) {
      ColumnChunkMetaData current = actual.get(i);
      if (i != 0) {
        ColumnChunkMetaData previous = actual.get(i - 1);
        long expectedStart = previous.getStartingPos() + previous.getTotalSize();
        assertEquals(expectedStart, current.getStartingPos());
      }

      assertColumnMetadataEquivalent(expected.get(i), current);
    }
  }

  private void assertColumnMetadataEquivalent(ColumnChunkMetaData expected,
                                              ColumnChunkMetaData actual) {
    assertEquals(expected.getPath(), expected.getPath());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getCodec(), actual.getCodec());
    assertEquals(expected.getEncodings(), actual.getEncodings());
    assertEquals(expected.getValueCount(), actual.getValueCount());
  }

  @Test
  public void testTransformedColumns() throws Exception {
    ParquetReader<Group> inputReader = ParquetReader.builder(
        new GroupReadSupport(), inputFile).build();
    ParquetReader<Group> outputReader = ParquetReader.builder(
        new GroupReadSupport(), outputFile).build();

    Group outputGroup;
    while ((outputGroup = outputReader.read()) != null) {
      Group inputGroup = inputReader.read();

      int expectedId = 2 * inputGroup.getInteger("id", 0);
      assertEquals(expectedId, outputGroup.getInteger("id", 0));

      String expectedUuid = inputGroup.getString("uuid", 0).substring(0, 8);
      assertEquals(expectedUuid, outputGroup.getString("uuid", 0));
    }

    assertNull(inputReader.read());
    assertNull(outputReader.read());
  }

  @Test
  public void testCopiedColumns() throws Exception {
    ParquetReader<Group> inputReader = ParquetReader.builder(
        new GroupReadSupport(), inputFile).build();
    ParquetReader<Group> outputReader = ParquetReader.builder(
        new GroupReadSupport(), outputFile).build();

    Group outputGroup;
    while ((outputGroup = outputReader.read()) != null) {
      Group inputGroup = inputReader.read();
      int inputTimestamp = 0, outputTimestamp;
      try {
        inputTimestamp = inputGroup.getInteger("timestamp", 0);
      } catch (RuntimeException e1) {
        try {
          outputGroup.getInteger("timestamp", 0);
          fail("Expected RuntimeException for null group");
        } catch (RuntimeException e2) {
          continue;
        }
      }
      outputTimestamp = outputGroup.getInteger("timestamp", 0);
      assertEquals(inputTimestamp, outputTimestamp);
    }

    assertNull(inputReader.read());
    assertNull(outputReader.read());
  }
}
