/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.transform;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

public abstract class BooleanColumnTransformer extends ColumnTransformerBase {

  public BooleanColumnTransformer(ColumnDescriptor column) {
    super(column);
    if (column.getType() != PrimitiveType.PrimitiveTypeName.BOOLEAN)
      throw new IllegalArgumentException("Column must contain Booleans");
  }


  @Override
  public boolean shouldTransform(Statistics statistics) {
    return shouldTransform((BooleanStatistics)statistics);
  }

  protected boolean shouldTransform(BooleanStatistics statistics) {
    return true;
  }

  @Override
  protected void processValue(int repetitionLevel, int definitionLevel,
                              ColumnReader reader, ColumnWriter writer) {
    boolean value = transformValue(reader.getBoolean());
    writer.write(value, repetitionLevel, definitionLevel);
  }

  abstract protected boolean transformValue(boolean value);
}
