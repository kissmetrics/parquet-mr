package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public abstract class IntegerColumnTransformer extends ColumnTransformerBase {

  public IntegerColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.INT32)
      throw new IllegalArgumentException("Column must contain integers");
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    int value = transformValue(reader.getInteger());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected int transformValue(int value);
}
