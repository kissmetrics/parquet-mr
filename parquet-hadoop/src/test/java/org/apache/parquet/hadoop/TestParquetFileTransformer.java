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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.transform.IntegerColumnTransformer;
import org.apache.parquet.hadoop.transform.LongColumnTransformer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestParquetFileTransformer {

  private static final CompressionCodecName CODEC_NAME = GZIP;
  private static final int BLOCK_SIZE = 1024;
  private static final int PAGE_SIZE = 512;
  private static final int DICTIONARY_PAGE_SIZE = 512;
  private static final boolean ENABLE_DICTIONARY = true;
  private static final ParquetProperties.WriterVersion WRITER_VERSION = PARQUET_1_0;
  private static final int RECORD_COUNT = 250;  // 3 row groups of 100, 100, 50

  private final Configuration conf = new Configuration();

  private final MessageType schema = Types.buildMessage()
      .required(INT32).named("id")
      .optional(INT64).named("timestamp")
      .required(BINARY).as(UTF8).named("name")
      .named("TransformTest");

  private final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
  private final Random random = new Random();
  private Path testDir, inputFile, outputFile;

  private Group newRecord(int i) {
    Group group = groupFactory.newGroup().append("id", i);
    if (i % 2 == 0)
      group.append("timestamp", 1000L * (1400000000L + i));
    group.append("name", String.format("Record %d", i));
    return group;
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
      writer.write(newRecord(i));

    writer.close();
  }

  private void transformFile() throws IOException {
    List<ColumnDescriptor> columnDescriptors = schema.getColumns();
    ColumnDescriptor idColumn = columnDescriptors.get(0),
        timestampColumn = columnDescriptors.get(1);

    HashMap<ColumnDescriptor, ColumnTransformer> transformers =
        new HashMap<ColumnDescriptor, ColumnTransformer>();

    transformers.put(idColumn, new IntegerColumnTransformer(idColumn) {
      @Override
      protected boolean shouldTransform(IntStatistics statistics) {
        return statistics.getMin() >= 100;  // Transform the second and third blocks
      }

      @Override
      protected int transformValue(int value) {
        return 2 * value;
      }
    });

    transformers.put(timestampColumn, new LongColumnTransformer(timestampColumn) {
      @Override
      protected boolean shouldTransform(LongStatistics statistics) {
        return (statistics.getMin() >= 1400000100000L
            && statistics.getMax() < 1400000200000L);  // Transform just the second block
      }

      @Override
      protected long transformValue(long value) {
        return value + 86400L;
      }
    });

    ParquetMetadata footer = ParquetFileReader.readFooter(conf, inputFile, NO_FILTER);
    List<BlockMetaData> blocks = footer.getBlocks();

    ParquetFileTransformer transformer = new ParquetFileTransformer(
        conf, transformers, PAGE_SIZE, CODEC_NAME, DICTIONARY_PAGE_SIZE,
        ENABLE_DICTIONARY, WRITER_VERSION);
    outputFile = new Path(testDir, "output.parquet");
    transformer.transformFile(inputFile, footer, blocks, outputFile);
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
    assertEquals(expected.getPath(), actual.getPath());
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getCodec(), actual.getCodec());
    assertEquals(expected.getEncodings(), actual.getEncodings());
    assertEquals(expected.getValueCount(), actual.getValueCount());
  }

  @Test
  public void testTransformation() throws Exception {
    ParquetReader<Group> inputReader = ParquetReader.builder(
        new GroupReadSupport(), inputFile).build();
    ParquetReader<Group> outputReader = ParquetReader.builder(
        new GroupReadSupport(), outputFile).build();

    Group outputGroup;
    while ((outputGroup = outputReader.read()) != null) {
      Group inputGroup = inputReader.read();

      // Note that the first block should be copied verbatim since no
      // transformations apply.
      int inputId = inputGroup.getInteger("id", 0);
      int expectedId = (inputId >= 100) ? (2 * inputId) : inputId;
      assertEquals(expectedId, outputGroup.getInteger("id", 0));

      if (inputGroup.getFieldRepetitionCount("timestamp") == 0)
        assertEquals(0, outputGroup.getFieldRepetitionCount("timestamp"));
      else {
        long inputTimestamp = inputGroup.getLong("timestamp", 0);
        long expectedTimestamp =
            (inputTimestamp >= 1400000100000L && inputTimestamp < 1400000200000L)
                ? inputTimestamp + 86400L : inputTimestamp;
        assertEquals(expectedTimestamp, outputGroup.getLong("timestamp", 0));
      }

      String expectedName = inputGroup.getString("name", 0);
      assertEquals(expectedName, outputGroup.getString("name", 0));
    }

    assertNull(inputReader.read());
    assertNull(outputReader.read());
  }
}
