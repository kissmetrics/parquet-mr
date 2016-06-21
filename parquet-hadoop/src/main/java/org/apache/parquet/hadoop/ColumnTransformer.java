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
package org.apache.parquet.hadoop;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * Transformer of column values
 */
public interface ColumnTransformer {

  /**
   * Checks whether a column should be transformed given its statistics.
   *
   * @param statistics Statistics of the column to transform
   * @return Whether to transform the column
   */
  boolean shouldTransform(Statistics statistics);

  /**
   * Writes a transformed column value.
   *
   * @param reader Reader from which to obtain the current value
   * @param writer Writer for the transformed value
   */
  void transform(ColumnReader reader, ColumnWriter writer);
}
