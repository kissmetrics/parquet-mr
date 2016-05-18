package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public abstract class BooleanColumnTransformer extends ColumnTransformerBase {

  public BooleanColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.BOOLEAN)
      throw new IllegalArgumentException("Column must contain Booleans");
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    boolean value = transformValue(reader.getBoolean());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected boolean transformValue(boolean value);
}
