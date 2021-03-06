/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dryangkun.hbase.tidx.hive;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class DataInputInputStream extends InputStream {

  private final DataInput dataInput;
  public DataInputInputStream(DataInput dataInput) {
    this.dataInput = dataInput;
  }
  @Override
  public int read() throws IOException {
    try {
      return dataInput.readUnsignedByte();
    } catch (EOFException e) {
      // contract on EOF differs between DataInput and InputStream
      return -1;
    }
  }

  public static InputStream from(DataInput dataInput) {
    if(dataInput instanceof InputStream) {
      return (InputStream)dataInput;
    }
    return new DataInputInputStream(dataInput);
  }
}
