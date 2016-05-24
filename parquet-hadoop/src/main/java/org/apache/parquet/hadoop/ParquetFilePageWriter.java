package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.CopyPageVisitor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.hadoop.ParquetFileReader.Chunk;
import org.apache.parquet.hadoop.ParquetFileReader.ChunkPageSet;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.util.HashMap;
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
   * @param fileWriter File writer to which to append
   * @param chunks Column chunks to append, which must not contain dictionary
   *               pages
   * @throws IllegalArgumentException if any column chunks include dictionary
   *                                  pages
   */
  void writePages(ParquetFileWriter fileWriter,
                  Iterable<Chunk> chunks) throws IOException {
    CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(codecName, pageSize);
    ColumnChunkPageWriteStore pageWriteStore =
        new ColumnChunkPageWriteStore(compressor, schema, pageSize);

    long bufferedSize = 0L;
    Map<ColumnDescriptor, CopyPageVisitor> copyVisitors =
        newCopyVisitors(pageWriteStore);

    for (Chunk chunk : chunks) {
      ChunkPageSet pageSet = chunk.readRawPages();
      if (pageSet.hasDictionaryPage())
        throw new IllegalArgumentException("Chunks may not have dictionary pages");

      final ColumnDescriptor column = chunk.getColumnDescriptor();
      final CopyPageVisitor copyVisitor = copyVisitors.get(column);

      for (DataPage dataPage : pageSet.getDataPages()) {
        final int uncompressedSize = dataPage.getUncompressedSize();
        if (bufferedSize + uncompressedSize > rowGroupSize) {
          pageWriteStore.flushToFileWriter(fileWriter);
          copyVisitors = newCopyVisitors(pageWriteStore);
          bufferedSize = 0L;
        }

        try {
          dataPage.accept(copyVisitor);
        } catch (CopyPageVisitor.PageWriteException e) {
          throw (IOException)e.getCause();
        }
        bufferedSize += uncompressedSize;
      }
    }

    if (bufferedSize > 0L)
      pageWriteStore.flushToFileWriter(fileWriter);
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
