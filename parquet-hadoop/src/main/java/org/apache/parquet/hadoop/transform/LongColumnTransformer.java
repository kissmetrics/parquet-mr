package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public abstract class LongColumnTransformer extends ColumnTransformerBase {

  public LongColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.INT64)
      throw new IllegalArgumentException("Column must contain long integers");
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    long value = transformValue(reader.getLong());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected long transformValue(long value);
}
