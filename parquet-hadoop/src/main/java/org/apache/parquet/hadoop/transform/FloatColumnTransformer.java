package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public abstract class FloatColumnTransformer extends ColumnTransformerBase {

  public FloatColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.INT64)
      throw new IllegalArgumentException("Column must contain floats");
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    float value = transformValue(reader.getFloat());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected float transformValue(float value);
}
