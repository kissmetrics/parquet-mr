package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
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

public class TestParquetFilePageAppender {

  private static final CompressionCodecName CODEC_NAME = GZIP;
  private static final int BLOCK_SIZE = 2048;
  private static final int PAGE_SIZE = 512;

  private final Configuration conf = new Configuration();

  private final MessageType schema = Types.buildMessage()
      .required(INT32).named("id")
      .required(BINARY).as(UTF8).named("name")
      .named("AppendTest");

  private final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
  private Path testDir;
  private ParquetFilePageAppender pageAppender;

  private Group makeRecord(int id, String name) {
    return groupFactory.newGroup().append("id", id).append("name", name);
  }

  private ParquetWriter<Group> writerFor(Path file, boolean enableDictionary)
      throws IOException {
    return new ParquetWriter<Group>(file, new GroupWriteSupport(), CODEC_NAME,
        BLOCK_SIZE, PAGE_SIZE, PAGE_SIZE, enableDictionary, false, PARQUET_1_0, conf);
  }

  private ParquetWriter<Group> writerFor(Path file) throws IOException {
    return writerFor(file, false);
  }

  private List<BlockMetaData> blocksFor(Path file) throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    return footer.getBlocks();
  }

  private Path appendPages(List<ParquetFileReader> fileReaders,
                           List<List<BlockMetaData>> blockLists)
      throws IOException {
    Path outputFile = new Path(testDir, "output.parquet");
    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, outputFile);

    fileWriter.start();
    pageAppender.appendPages(fileWriter, fileReaders, blockLists);
    fileWriter.end(Collections.<String, String>emptyMap());

    return outputFile;
  }

  private ParquetFileReader fileReaderFor(Path file, List<BlockMetaData> blocks)
      throws IOException {
    return new ParquetFileReader(conf, file, blocks, schema.getColumns());
  }

  private List<List<BlockMetaData>> blockListsFor(List<Path> files)
      throws IOException {
    ArrayList<List<BlockMetaData>> blockLists = new ArrayList<List<BlockMetaData>>();
    for (Path file : files)
      blockLists.add(blocksFor(file));
    return blockLists;
  }

  private List<ParquetFileReader> fileReadersFor(List<Path> files,
                                                 List<List<BlockMetaData>> blockLists)
      throws IOException {
    List<ParquetFileReader> fileReaders = new ArrayList<ParquetFileReader>();
    Iterator<List<BlockMetaData>> blockListsIterator = blockLists.iterator();
    for (Path file : files)
      fileReaders.add(fileReaderFor(file, blockListsIterator.next()));
    return fileReaders;
  }

  private ParquetReader<Group> groupReaderFor(Path file) throws IOException {
    return ParquetReader.builder(new GroupReadSupport(), file).build();
  }

  @Before
  public void setUp() throws Exception {
    testDir = new Path("target/tests/TestParquetFileAppender/");
    enforceEmptyDir(conf, testDir);

    GroupWriteSupport.setSchema(schema, conf);

    pageAppender = new ParquetFilePageAppender(conf, schema, 2 * BLOCK_SIZE,
        PAGE_SIZE, CODEC_NAME);
  }

  @Test
  public void testPageWriting() throws IOException {
    Path fragment1 = new Path(testDir, "fragment1.parquet");
    ParquetWriter<Group> recordWriter = writerFor(fragment1);
    recordWriter.write(makeRecord(1, "foo"));
    recordWriter.write(makeRecord(2, "bar"));
    recordWriter.close();

    Path fragment2 = new Path(testDir, "fragment2.parquet");
    recordWriter = writerFor(fragment2);
    recordWriter.write(makeRecord(3, "baz"));
    recordWriter.close();

    Path fragment3 = new Path(testDir, "fragment3.parquet");
    recordWriter = writerFor(fragment3);
    recordWriter.write(makeRecord(4, "quux"));
    recordWriter.write(makeRecord(5, "xy"));
    recordWriter.write(makeRecord(6, "zzy"));
    recordWriter.close();

    List<Path> files = Arrays.asList(fragment1, fragment2, fragment3);
    List<List<BlockMetaData>> blockLists = blockListsFor(files);
    List<ParquetFileReader> fileReaders = fileReadersFor(files, blockLists);

    Path outputFile = appendPages(fileReaders, blockLists);

    List<BlockMetaData> blocks = blocksFor(outputFile);
    assertEquals(1, blocks.size());

    BlockMetaData block = blocks.get(0);
    assertEquals(6, block.getRowCount());

    ParquetReader<Group> reader = groupReaderFor(outputFile);
    assertNextRecord(reader, 1, "foo");
    assertNextRecord(reader, 2, "bar");
    assertNextRecord(reader, 3, "baz");
    assertNextRecord(reader, 4, "quux");
    assertNextRecord(reader, 5, "xy");
    assertNextRecord(reader, 6, "zzy");
  }

  private void assertNextRecord(ParquetReader<Group> reader, int expectedId,
                                String expectedName) throws IOException {
    Group group = reader.read();
    assertEquals(expectedId, group.getInteger("id", 0));
    assertEquals(expectedName, group.getString("name", 0));
  }

  @Test
  public void testSpannedBlocks() throws Exception {
    Path fragment1 = new Path(testDir, "fragment1.parquet");
    ParquetWriter<Group> recordWriter = writerFor(fragment1);
    for (int i = 0; i < 1000; i++) {
      String name = String.format("Record %d", i);
      recordWriter.write(makeRecord(i, name));
    }
    recordWriter.close();

    Path fragment2 = new Path(testDir, "fragment2.parquet");
    recordWriter = writerFor(fragment2);
    for (int i = 0; i < 100; i++) {
      String name = String.format("Another record %d", i);
      recordWriter.write(makeRecord(1000 + i, name));
    }
    recordWriter.close();

    List<Path> files = Arrays.asList(fragment1, fragment2);
    List<List<BlockMetaData>> blockLists = blockListsFor(files);
    List<ParquetFileReader> fileReaders = fileReadersFor(files, blockLists);

    Path outputFile = appendPages(fileReaders, blockLists);

    List<BlockMetaData> blocks = blocksFor(outputFile);
    assertEquals(4, blocks.size());

    ParquetReader<Group> reader = groupReaderFor(outputFile);
    for (int i = 0; i < 1000; i++) {
      String name = String.format("Record %d", i);
      assertNextRecord(reader, i, name);
    }
    for (int i = 0; i < 100; i++) {
      String name = String.format("Another record %d", i);
      assertNextRecord(reader, 1000 + i, name);
    }
    assertNull(reader.read());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDictionaryChunks() throws Exception {
    Path dictionaryFile = new Path(testDir, "foo.parquet");
    ParquetWriter<Group> recordWriter = writerFor(dictionaryFile, true);
    recordWriter.write(makeRecord(123, "abc"));
    recordWriter.write(makeRecord(456, "abc"));
    recordWriter.close();

    List<Path> files = Collections.singletonList(dictionaryFile);
    List<List<BlockMetaData>> blockLists = blockListsFor(files);
    List<ParquetFileReader> fileReaders = fileReadersFor(files, blockLists);

    appendPages(fileReaders, blockLists);
  }
}