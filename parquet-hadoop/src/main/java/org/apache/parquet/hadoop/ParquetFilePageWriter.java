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
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Writer of pages from column chunks to combined output
 */
class ParquetFilePageWriter {

  private final MessageType schema;
  private final long rowGroupSize;
  private final int pageSize;
  private final CompressionCodecName codecName;
  private final CodecFactory codecFactory;

  ParquetFilePageWriter(Configuration conf, MessageType schema,
                        long rowGroupSize, int pageSize,
                        CompressionCodecName codecName) {
    this.schema = schema;
    this.rowGroupSize = rowGroupSize;
    this.pageSize = pageSize;
    this.codecName = codecName;
    this.codecFactory = new CodecFactory(conf);
  }

  /**
   * Writes raw pages from the given column chunks.
   *
   * @param fileWriter File writer to which to write pages
   * @param fileReaders File readers from which to read chunks for blocks
   * @param blockLists Blocks for each file whose pages to append, which must
   *                   not make use of dictionary pages
   * @throws IllegalArgumentException if any column chunks include dictionary
   *                                  pages
   */
  void writePages(ParquetFileWriter fileWriter,
                  Iterable<ParquetFileReader> fileReaders,
                  Iterable<List<BlockMetaData>> blockLists)
      throws IOException {
    CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(codecName, pageSize);
    ColumnChunkPageWriteStore pageWriteStore =
        new ColumnChunkPageWriteStore(compressor, schema, pageSize);
    Map<ColumnDescriptor, CopyPageVisitor> copyVisitors
        = newCopyVisitors(pageWriteStore);

    final Iterator<List<BlockMetaData>> blockListsIterator
        = blockLists.iterator();
    long bufferedSize = 0L, rowCount = 0L;

    for (ParquetFileReader fileReader : fileReaders) {
      List<BlockMetaData> blocks = blockListsIterator.next();

      for (BlockMetaData block : blocks) {
        final long blockSize = block.getTotalByteSize();
        if (bufferedSize + blockSize > rowGroupSize) {
          flush(fileWriter, rowCount, pageWriteStore);
          bufferedSize = rowCount = 0L;
        }

        final List<Chunk> chunks = fileReader.readChunks(block);
        for (Chunk chunk : chunks) {
          ChunkPageSet pageSet = chunk.readRawPages();
          if (pageSet.hasDictionaryPage())
            throw new IllegalArgumentException("Chunks may not have dictionary pages");

          final ColumnDescriptor column = chunk.getColumnDescriptor();
          final CopyPageVisitor copyVisitor = copyVisitors.get(column);

          for (DataPage dataPage : pageSet.getDataPages()) {
            try {
              dataPage.accept(copyVisitor);
            } catch (CopyPageVisitor.PageWriteException e) {
              throw (IOException)e.getCause();
            }
          }
        }

        bufferedSize += blockSize;
        rowCount += block.getRowCount();
      }
    }

    if (bufferedSize > 0L)
      flush(fileWriter, rowCount, pageWriteStore);
  }

  private void flush(ParquetFileWriter fileWriter, long recordCount,
                     ColumnChunkPageWriteStore pageWriteStore) throws IOException {
    fileWriter.startBlock(recordCount);
    pageWriteStore.flushToFileWriter(fileWriter);
    fileWriter.endBlock();
  }

  private Map<ColumnDescriptor, CopyPageVisitor> newCopyVisitors(
      PageWriteStore pageWriteStore) {
    HashMap<ColumnDescriptor, CopyPageVisitor> pageWriters =
        new HashMap<ColumnDescriptor, CopyPageVisitor>();

    for (ColumnDescriptor column : schema.getColumns()) {
      final PageWriter pageWriter = pageWriteStore.getPageWriter(column);
      pageWriters.put(column, new CopyPageVisitor(pageWriter));
    }

    return pageWriters;
  }
}
