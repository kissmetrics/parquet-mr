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
package org.apache.parquet.column.page;

import java.io.IOException;

/**
 * Visitor to copy data pages to a given writer
 */
public class CopyPageVisitor implements DataPage.Visitor<Void> {

  private final PageWriter pageWriter;

  /**
   * @param pageWriter Target for writing data pages
   */
  public CopyPageVisitor(PageWriter pageWriter) {
    this.pageWriter = pageWriter;
  }

  @Override
  public Void visit(DataPageV1 page) {
    try {
      pageWriter.writeCompressedPage(
          page.getBytes(), page.getUncompressedSize(), page.getValueCount(),
          page.getStatistics(), page.getRlEncoding(), page.getDlEncoding(),
          page.getValueEncoding());
    } catch (IOException e) {
      throw new PageWriteException(e);
    }
    return null;
  }

  @Override
  public Void visit(DataPageV2 page) {
    try {
      pageWriter.writeCompressedPageV2(
          page.getRowCount(), page.getNullCount(), page.getValueCount(),
          page.getRepetitionLevels(), page.getDefinitionLevels(),
          page.getDataEncoding(), page.getData(), page.getUncompressedSize(),
          page.getStatistics());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Wrapper for checked `IOException` instances thrown by visitors
   */
  public static class PageWriteException extends RuntimeException {

    PageWriteException(IOException cause) {
      super(cause);
    }
  }
}
