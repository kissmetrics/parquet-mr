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
import org.apache.parquet.column.*;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
  private final MessageType schema;
  private final Map<ColumnDescriptor, ColumnTransformer> transformers;
  private ParquetProperties parquetProperties;
  private final CodecFactory codecFactory;
  private final NopGroupConverter recordConverter = new NopGroupConverter();

  public ParquetFileTransformer(Configuration conf, MessageType schema,
                                Map<ColumnDescriptor, ColumnTransformer> transformers,
                                ParquetProperties parquetProperties) {
    this.conf = conf;
    this.schema = schema;
    this.transformers = transformers;
    this.parquetProperties = parquetProperties;
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
   * @param blocks Blocks for the input file
   * @param outputFile Target file for transformed output
   * @param codecName Codec to compress transformed columns
   * @param pageSize Size of transformed pages
   */
  public void transformFile(Path inputFile, List<BlockMetaData> blocks,
                            Path outputFile, CompressionCodecName codecName,
                            int pageSize) throws IOException {
    ParquetFileReader fileReader =
        new ParquetFileReader(conf, inputFile, blocks, schema.getColumns());

    CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(codecName, pageSize);

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, outputFile);
    fileWriter.start();

    BlockMetaData block;
    while ((block = fileReader.getCurrentBlock()) != null) {
      fileWriter.startBlock(block.getRowCount());

      ColumnChunkPageWriteStore pageWriteStore =
          new ColumnChunkPageWriteStore(compressor, schema, pageSize);
      ColumnWriteStore columnWriteStore =
          parquetProperties.newColumnWriteStore(schema, pageWriteStore, pageSize);

      List<ParquetFileReader.Chunk> chunks = fileReader.readChunks(block);
      for (ParquetFileReader.Chunk chunk : chunks) {
        ColumnDescriptor column = chunk.getColumnDescriptor();
        ColumnTransformer transformer = transformers.get(column);
        if (transformer != null)
          transformChunk(block, columnWriteStore, chunk, column, transformer);
        else {
          PageWriter pageWriter = pageWriteStore.getPageWriter(column);
          copyChunk(chunk, pageWriter);
        }
      }

      columnWriteStore.flush();
      pageWriteStore.flushToFileWriter(fileWriter);

      fileWriter.endBlock();
      fileReader.advanceBlock();
    }

    fileWriter.end(Collections.<String, String>emptyMap());
  }

  private void transformChunk(BlockMetaData block, ColumnWriteStore columnWriteStore,
                              ParquetFileReader.Chunk chunk, ColumnDescriptor column,
                              ColumnTransformer transformer) throws IOException {
    ColumnChunkPageReadStore.ColumnChunkPageReader pageReader = chunk.readAllPages();

    ColumnChunkPageReadStore pageReadStore =
        new ColumnChunkPageReadStore(block.getRowCount());
    pageReadStore.addColumn(column, pageReader);

    ColumnReadStoreImpl columnReadStore =
        new ColumnReadStoreImpl(pageReadStore, recordConverter, schema);
    ColumnReader columnReader = columnReadStore.getColumnReader(column);

    ColumnWriter columnWriter = columnWriteStore.getColumnWriter(column);

    transformer.transform(columnReader, columnWriter);
  }

  private void copyChunk(ParquetFileReader.Chunk chunk, PageWriter pageWriter) throws IOException {
    ParquetFileReader.ChunkPageSet chunkPageSet = chunk.readRawPages();
    CopyPageVisitor visitor = new CopyPageVisitor(pageWriter);

    DictionaryPage dictionaryPage = chunkPageSet.getDictionaryPage();
    if (dictionaryPage != null)
      pageWriter.writeDictionaryPage(dictionaryPage);

    for (DataPage dataPage : chunkPageSet.getDataPages())
      try {
        dataPage.accept(visitor);
      } catch (PageWriteException e) {
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

  private static class CopyPageVisitor implements DataPage.Visitor<Void> {

    private final PageWriter pageWriter;

    public CopyPageVisitor(PageWriter pageWriter) {
      this.pageWriter = pageWriter;
    }

    @Override
    public Void visit(DataPageV1 page) {
      try {
        pageWriter.writeCompressedPage(
            page.getBytes(), page.getUncompressedSize(), page.getValueCount(),
            page.getStatistics(), page.getRlEncoding(), page.getDlEncoding(),
            page.getValueEncoding());
      } catch (IOException e) {
        throw new PageWriteException(e);
      }
      return null;
    }

    @Override
    public Void visit(DataPageV2 page) {
      try {
        pageWriter.writeCompressedPageV2(
            page.getRowCount(), page.getNullCount(), page.getValueCount(),
            page.getRepetitionLevels(), page.getDefinitionLevels(),
            page.getDataEncoding(), page.getData(), page.getUncompressedSize(),
            page.getStatistics());
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  // Wrapper for checked IOExceptions thrown by visitors
  private static class PageWriteException extends RuntimeException {

    public PageWriteException(IOException cause) {
      super(cause);
    }
  }
}
