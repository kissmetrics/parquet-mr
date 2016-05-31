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
import org.apache.parquet.Closeables;
import org.apache.parquet.column.*;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.CopyPageVisitor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.hadoop.ParquetFileReader.Chunk;
import org.apache.parquet.hadoop.ParquetFileReader.ChunkPageSet;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Transformer of column values from a Parquet file
 */
public class ParquetFileTransformer implements Closeable {

  private final Configuration conf;
  private final Map<ColumnDescriptor, ? extends ColumnTransformer> transformers;
  private final int pageSize;
  private final CompressionCodecName codecName;
  private final ParquetProperties parquetProperties;
  private final CodecFactory codecFactory;
  private final NopGroupConverter recordConverter = new NopGroupConverter();

  public ParquetFileTransformer(Configuration conf,
                                Map<ColumnDescriptor, ? extends ColumnTransformer> transformers,
                                int pageSize, CompressionCodecName codecName,
                                int dictionaryPageSize, boolean enableDictionary,
                                ParquetProperties.WriterVersion writerVersion) {
    this.conf = conf;
    this.transformers = transformers;
    this.pageSize = pageSize;
    this.codecName = codecName;
    this.parquetProperties = new ParquetProperties(dictionaryPageSize, writerVersion, enableDictionary);
    this.codecFactory = new CodecFactory(conf);
  }

  @Override
  public void close() throws IOException {
    codecFactory.release();
  }

  /**
   * Transforms the specified file.
   *
   * @param inputFile Input file to transform
   * @param footer Footer metadata block for the file
   * @param blocks Blocks from the input file to transform
   * @param outputFile Target file for transformed output
   */
  public void transformFile(Path inputFile, ParquetMetadata footer,
                            List<BlockMetaData> blocks, Path outputFile)
      throws IOException {
    final FileMetaData metaData = footer.getFileMetaData();
    final MessageType schema = metaData.getSchema();

    final CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(codecName, pageSize);

    final ParquetFileReader fileReader =
        new ParquetFileReader(conf, inputFile, blocks, schema.getColumns());

    try {
      final ParquetFileWriter fileWriter =
          new ParquetFileWriter(conf, schema, outputFile);
      fileWriter.start();

      BlockMetaData block;
      while ((block = fileReader.getCurrentBlock()) != null) {
        writeBlock(schema, compressor, fileReader, fileWriter, block);
        fileReader.advanceBlock();
      }

      fileWriter.end(Collections.<String, String>emptyMap());
    } finally {
      Closeables.closeAndSwallowIOExceptions(fileReader);
    }
  }

  private void writeBlock(MessageType schema,
                          CodecFactory.BytesCompressor compressor,
                          ParquetFileReader fileReader,
                          ParquetFileWriter fileWriter,
                          BlockMetaData block) throws IOException {
    fileWriter.startBlock(block.getRowCount());

    final ColumnChunkPageWriteStore pageWriteStore =
        new ColumnChunkPageWriteStore(compressor, schema, pageSize);
    final ColumnWriteStore columnWriteStore =
        parquetProperties.newColumnWriteStore(schema, pageWriteStore, pageSize);

    final List<Chunk> chunks = fileReader.readChunks(block);
    for (Chunk chunk : chunks) {
      final ColumnDescriptor column = chunk.getColumnDescriptor();
      final ColumnTransformer transformer = transformers.get(column);
      if (transformer != null)
        transformChunk(schema, block, columnWriteStore, chunk, column,
            transformer);
      else {
        final PageWriter pageWriter = pageWriteStore.getPageWriter(column);
        copyChunk(chunk, pageWriter);
      }
    }

    columnWriteStore.flush();
    pageWriteStore.flushToFileWriter(fileWriter);

    fileWriter.endBlock();
  }

  private void transformChunk(MessageType schema, BlockMetaData block,
                              ColumnWriteStore columnWriteStore,
                              Chunk chunk, ColumnDescriptor column,
                              ColumnTransformer transformer)
      throws IOException {
    final ColumnChunkPageReader pageReader = chunk.readAllPages();

    final ColumnChunkPageReadStore pageReadStore =
        new ColumnChunkPageReadStore(block.getRowCount());
    pageReadStore.addColumn(column, pageReader);

    final ColumnReadStoreImpl columnReadStore =
        new ColumnReadStoreImpl(pageReadStore, recordConverter, schema);
    final ColumnReader columnReader = columnReadStore.getColumnReader(column);

    final ColumnWriter columnWriter = columnWriteStore.getColumnWriter(column);

    transformer.transform(columnReader, columnWriter);
  }

  private void copyChunk(Chunk chunk, PageWriter pageWriter) throws IOException {
    final ChunkPageSet chunkPageSet = chunk.readRawPages();
    final CopyPageVisitor visitor = new CopyPageVisitor(pageWriter);

    final DictionaryPage dictionaryPage = chunkPageSet.getDictionaryPage();
    if (dictionaryPage != null)
      pageWriter.writeDictionaryPage(dictionaryPage);

    for (DataPage dataPage : chunkPageSet.getDataPages())
      try {
        dataPage.accept(visitor);
      } catch (CopyPageVisitor.PageWriteException e) {
        throw (IOException)e.getCause();
      }
  }

  private static class NopConverter extends PrimitiveConverter {}

  private static class NopGroupConverter extends GroupConverter {

    private final NopConverter converter = new NopConverter();

    @Override public Converter getConverter(int fieldIndex) { return converter; }
    @Override public void start() {}
    @Override public void end() {}
  }
}
