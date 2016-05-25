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
import java.util.Collections;
import java.util.List;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.*;

public class ParquetFileCoalescerTest {

  private static final CompressionCodecName CODEC_NAME = GZIP;
  private static final int BLOCK_SIZE = 2048;
  private static final int PAGE_SIZE = 512;

  private final Configuration conf = new Configuration();

  private final MessageType schema = Types.buildMessage()
      .required(INT32).named("id")
      .required(BINARY).as(UTF8).named("name")
      .named("CoalesceTest");

  private final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
  private Path testDir, file1, file2;
  private ParquetFileCoalescer parquetFileCoalescer;
  private ParquetFileWriter fileWriter;
  private Path outputFile;

  private Group makeRecord(int id, String name) {
    return groupFactory.newGroup().append("id", id).append("name", name);
  }

  private ParquetWriter<Group> writerFor(Path file) throws IOException {
    return new ParquetWriter<Group>(file, new GroupWriteSupport(), CODEC_NAME,
        BLOCK_SIZE, PAGE_SIZE, PAGE_SIZE, false, false, PARQUET_1_0, conf);
  }

  @Before
  public void setUp() throws Exception {
    testDir = new Path("target/tests/TestParquetFileCoalescer/");
    enforceEmptyDir(conf, testDir);

    GroupWriteSupport.setSchema(schema, conf);

    parquetFileCoalescer = new ParquetFileCoalescer(conf, schema,
        BLOCK_SIZE, PAGE_SIZE, CODEC_NAME);

    // Span two blocks, but leave the final block as a fragment.
    file1 = new Path(testDir, "file1.parquet");
    ParquetWriter<Group> writer = writerFor(file1);
    for (int i = 0; i < 400; i++)
      writer.write(makeRecord(i, String.format("Record %d", i)));
    writer.close();

    // Write just one small block, which should be coalesced with the final
    // block of the first file.
    file2 = new Path(testDir, "file2.parquet");
    writer = writerFor(file2);
    for (int i = 0; i < 30; i++)
      writer.write(makeRecord(400 + i, String.format("Other record %d", 400 + i)));
    writer.close();

    outputFile = new Path(testDir, "output.parquet");
    fileWriter = new ParquetFileWriter(conf, schema, outputFile);
  }

  private List<BlockMetaData> blocksFor(Path file) throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    return footer.getBlocks();
  }

  @Test
  public void testCoalescing() throws Exception {
    fileWriter.start();
    parquetFileCoalescer.coalesceFiles(fileWriter, file1, file2);
    fileWriter.end(Collections.<String, String>emptyMap());

    assertEquals(2, blocksFor(file1).size());
    assertEquals(1, blocksFor(file2).size());
    assertEquals(2, blocksFor(outputFile).size());

    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), outputFile).build();

    for (int i = 0; i < 400; i++) {
      Group group = reader.read();
      assertEquals(i, group.getInteger("id", 0));
      String name = String.format("Record %d", i);
      assertEquals(name, group.getString("name", 0));
    }

    for (int i = 0; i < 30; i++) {
      Group group = reader.read();
      assertEquals(400 + i, group.getInteger("id", 0));
      String name = String.format("Other record %d", 400 + i);
      assertEquals(name, group.getString("name", 0));
    }

    assertNull(reader.read());
  }
}