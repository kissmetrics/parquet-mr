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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.CopyPageVisitor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.hadoop.ParquetFileReader.Chunk;
import org.apache.parquet.hadoop.ParquetFileReader.ChunkPageSet;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Appender of pages from blocks into combined output
 */
class ParquetFilePageAppender implements Closeable {

  private final MessageType schema;
  private final long rowGroupSize;
  private final int pageSize;
  private final CompressionCodecName codecName;
  private final CodecFactory codecFactory;

  ParquetFilePageAppender(Configuration conf, MessageType schema,
                          long rowGroupSize, int pageSize,
                          CompressionCodecName codecName) {
    this.schema = schema;
    this.rowGroupSize = rowGroupSize;
    this.pageSize = pageSize;
    this.codecName = codecName;
    this.codecFactory = new CodecFactory(conf);
  }

  @Override
  public void close() throws IOException {
    codecFactory.release();
  }

  /**
   * Appends raw pages from the given column chunks.
   *
   * @param fileWriter File writer to which to append pages
   * @param fileReaders File readers from which to read chunks for blocks
   * @param blockLists Blocks for each file whose pages to append, which must
   *                   not make use of dictionary pages
   * @throws IllegalArgumentException if any column chunks include dictionary
   *                                  pages
   */
  void appendPages(ParquetFileWriter fileWriter,
                   Iterable<ParquetFileReader> fileReaders,
                   Iterable<List<BlockMetaData>> blockLists)
      throws IOException {
    final CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(codecName, pageSize);
    final Iterator<List<BlockMetaData>> blockListsIterator = blockLists.iterator();

    ColumnChunkPageWriteStore pageWriteStore = newWriteStore(compressor);
    Map<ColumnDescriptor, CopyPageVisitor> copyVisitors = newCopyVisitors(pageWriteStore);
    long bufferedSize = 0L, rowCount = 0L;

    for (ParquetFileReader fileReader : fileReaders) {
      final List<BlockMetaData> blocks = blockListsIterator.next();

      for (BlockMetaData block : blocks) {
        bufferedSize += block.getTotalByteSize();
        rowCount += block.getRowCount();

        final List<Chunk> chunks = fileReader.readChunks(block);
        for (Chunk chunk : chunks) {
          final ChunkPageSet pageSet = chunk.readRawPages();
          if (pageSet.hasDictionaryPage())
            throw new IllegalArgumentException("Chunks may not have dictionary pages");

          final ColumnDescriptor column = chunk.getColumnDescriptor();
          final CopyPageVisitor copyVisitor = copyVisitors.get(column);
          for (DataPage dataPage : pageSet.getDataPages())
            try {
              dataPage.accept(copyVisitor);
            } catch (CopyPageVisitor.PageWriteException e) {
              throw (IOException)e.getCause();
            }
        }

        if (bufferedSize > rowGroupSize) {
          flush(fileWriter, rowCount, pageWriteStore);
          pageWriteStore = newWriteStore(compressor);
          copyVisitors = newCopyVisitors(pageWriteStore);
          bufferedSize = rowCount = 0L;
        }
      }
    }

    if (bufferedSize > 0L)
      flush(fileWriter, rowCount, pageWriteStore);
  }

  private ColumnChunkPageWriteStore newWriteStore(CodecFactory.BytesCompressor compressor) {
    return new ColumnChunkPageWriteStore(compressor, schema, pageSize);
  }

  private void flush(ParquetFileWriter fileWriter, long recordCount,
                     ColumnChunkPageWriteStore pageWriteStore) throws IOException {
    fileWriter.startBlock(recordCount);
    pageWriteStore.flushToFileWriter(fileWriter);
    fileWriter.endBlock();
  }

  private Map<ColumnDescriptor, CopyPageVisitor> newCopyVisitors(
      PageWriteStore pageWriteStore) {
    final HashMap<ColumnDescriptor, CopyPageVisitor> pageWriters =
        new HashMap<ColumnDescriptor, CopyPageVisitor>();

    for (ColumnDescriptor column : schema.getColumns()) {
      final PageWriter pageWriter = pageWriteStore.getPageWriter(column);
      pageWriters.put(column, new CopyPageVisitor(pageWriter));
    }

    return pageWriters;
  }
}
