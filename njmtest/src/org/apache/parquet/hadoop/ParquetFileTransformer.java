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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transformer of column values from a Parquet file
 */
public class ParquetFileTransformer implements Closeable {

  private final Configuration conf;
  private final Path inputFile;
  private final MessageType schema;
  private final List<BlockMetaData> blocks;
  private final Map<ColumnDescriptor, ColumnTransformer> transformers;
  private final CodecFactory codecFactory;
  private final List<ColumnDescriptor> columns;
  private final Map<ColumnPath, ColumnDescriptor> pathColumns = new HashMap<ColumnPath, ColumnDescriptor>();

  public ParquetFileTransformer(Configuration conf, Path inputFile,
                                MessageType schema, List<BlockMetaData> blocks,
                                Map<ColumnDescriptor, ColumnTransformer> transformers) {
    this.conf = conf;
    this.inputFile = inputFile;
    this.schema = schema;
    this.blocks = blocks;
    this.transformers = transformers;
    this.codecFactory = new CodecFactory(conf);
    columns = schema.getColumns();
    for (ColumnDescriptor column : columns)
      pathColumns.put(ColumnPath.get(column.getPath()), column);
  }

  @Override
  public void close() throws IOException {
    codecFactory.release();
  }

  private FSDataInputStream openInputStream() throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);
    return fileSystem.open(inputFile);
  }

  /**
   * Transforms all pathColumns from the inputFile.
   *
   * @param outputFile Target inputFile for transformed output
   * @param codecName Codec to compress transformed columns
   * @param pageSize Size of transformed pages
   */
  public void transformFile(Path outputFile, CompressionCodecName codecName, int pageSize) throws IOException {
    ParquetFileReader fileReader = new ParquetFileReader(conf, inputFile, blocks, columns);

    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(codecName, pageSize);

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, outputFile);
    fileWriter.start();

    BlockMetaData block;
    while ((block = fileReader.getCurrentBlock()) != null) {
      List<ParquetFileReader.Chunk> chunks = fileReader.readChunks(block);
      fileWriter.startBlock(block.getRowCount());

      ColumnChunkPageWriteStore pageWriteStore = new ColumnChunkPageWriteStore(compressor, schema, pageSize);

      for (ParquetFileReader.Chunk chunk : chunks) {
        ColumnDescriptor column = chunk.getColumnDescriptor();

        PageWriter pageWriter = pageWriteStore.getPageWriter(column);
        PageWriterVisitor pageWriterVisitor = new PageWriterVisitor(pageWriter);

        ParquetFileReader.ChunkPageSet chunkPageSet = chunk.readRawPages();

        DictionaryPage dictionaryPage = chunkPageSet.getDictionaryPage();
        if (dictionaryPage != null)
          pageWriter.writeDictionaryPage(dictionaryPage);

        for (DataPage dataPage : chunkPageSet.getDataPages())
          try {
            dataPage.accept(pageWriterVisitor);
          } catch (PageWriteException e) {
            throw (IOException)e.getCause();
          }
      }

      pageWriteStore.flushToFileWriter(fileWriter);

      fileWriter.endBlock();
      fileReader.advanceBlock();
    }

    fileWriter.end(Collections.<String, String>emptyMap());
  }

  private static class PageWriterVisitor implements DataPage.Visitor<Void> {

    private final PageWriter pageWriter;

    public PageWriterVisitor(PageWriter pageWriter) {
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

  // Wrapper for checked IOException of writeDataPage in visitor
  private static class PageWriteException extends RuntimeException {

    public PageWriteException(IOException cause) {
      super(cause);
    }
  }
}
