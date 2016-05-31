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
import org.apache.parquet.hadoop.ColumnTransformer;

/**
 * Base class for standard column transformers
 */
public abstract class ColumnTransformerBase implements ColumnTransformer {

  private final ColumnDescriptor column;

  public ColumnTransformerBase(ColumnDescriptor column) {
    this.column = column;
  }

  @Override
  public void transform(ColumnReader reader, ColumnWriter writer) {
    final int dlMax = column.getMaxDefinitionLevel();
    final long valueCount = reader.getTotalValueCount();
    for (long i = 0; i < valueCount; i++) {
      int rl = reader.getCurrentRepetitionLevel();
      int dl = reader.getCurrentDefinitionLevel();
      if (dl == dlMax)
        processValue(rl, dl, reader, writer);
      else
        writer.writeNull(rl, dl);
      reader.consume();
    }
  }

  /**
   * Transforms and writes the given value to the writer.
   **/
  abstract protected void processValue(int repetitionLevel, int definitionLevel,
                                       ColumnReader reader, ColumnWriter writer);
}
