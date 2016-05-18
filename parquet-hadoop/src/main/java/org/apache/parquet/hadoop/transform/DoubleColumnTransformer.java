package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public abstract class DoubleColumnTransformer extends ColumnTransformerBase {

  public DoubleColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.INT64)
      throw new IllegalArgumentException("Column must contain doubles");
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    double value = transformValue(reader.getDouble());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected double transformValue(double value);
}
