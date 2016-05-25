package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

/**
 * Coalescer of Parquet files with optimal block boundaries
 */
public class ParquetFileCoalescer {

  private final Configuration conf;
  private final ParquetFilePageAppender pageAppender;

  public ParquetFileCoalescer(Configuration conf,
                              ParquetFilePageAppender pageAppender) {
    this.conf = conf;
    this.pageAppender = pageAppender;
  }

  /**
   * Coalesces two files, writing their combined data to the given file writer.
   *
   * @param fileWriter File writer for coalesced output
   * @param file1 First file whose content to write
   * @param file2 Second file whose content to write, which should be smaller
   *              than the first
   * @throws IOException
   */
  public void coalesceFiles(ParquetFileWriter fileWriter, Path file1,
                            Path file2) throws IOException {
    final ParquetMetadata file1Footer = readFooter(file1);
    final List<BlockMetaData> file1Blocks = file1Footer.getBlocks();

    final ParquetMetadata file2Footer = readFooter(file2);
    final List<BlockMetaData> file2Blocks = file2Footer.getBlocks();

    // Cleave the final block of the first file to read independently.  This
    // is the boundary at which we drop from block- to page-level appends to
    // avoid block fragments.  This strategy assumes the second file is
    // relatively small compared to the first.
    final BlockMetaData lastBlock = file1Blocks.remove(file1Blocks.size() - 1);
    final List<BlockMetaData> file1LastBlockList =
        Collections.singletonList(lastBlock);

    // Write blocks from the first file.
    final ParquetFileReader file1Reader =
        openReader(file1, file1Footer, file1Blocks);
    file1Reader.appendTo(fileWriter);

    // Write remaining pages from the first and second files.
    final ParquetFileReader file1LastBlockReader =
        openReader(file1, file1Footer, file1LastBlockList);
    final ParquetFileReader file2Reader =
        openReader(file2, file2Footer, file2Blocks);

    final List<ParquetFileReader> fileReaders =
        Arrays.asList(file1LastBlockReader, file2Reader);

    final List<List<BlockMetaData>> blockLists = new ArrayList<List<BlockMetaData>>(2);
    blockLists.add(file1LastBlockList);
    blockLists.add(file2Blocks);

    pageAppender.appendPages(fileWriter, fileReaders, blockLists);
  }

  private ParquetMetadata readFooter(Path file1) throws IOException {
    return ParquetFileReader.readFooter(conf, file1, NO_FILTER);
  }

  private ParquetFileReader openReader(Path file, ParquetMetadata footer,
                                       List<BlockMetaData> blocks)
      throws IOException {
    final List<ColumnDescriptor> columns =
        footer.getFileMetaData().getSchema().getColumns();
    return new ParquetFileReader(conf, file, blocks, columns);
  }
}
