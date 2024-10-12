/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.util.Arrays;
import java.util.Objects;

/**
 * IOContext holds additional details on the merge/search context. A IOContext object can never be
 * initialized as null as passed as a parameter to either {@link
 * org.apache.lucene.store.Directory#openInput(String, IOContext)} or {@link
 * org.apache.lucene.store.Directory#createOutput(String, IOContext)}
 */
public class IOContext {
  /**
   * Context is a enumerator which specifies the context in which the Directory is being used for.
   */
  public enum Context {
    MERGE,
    READ,
    FLUSH,
    DEFAULT
  };

  /** An object of a enumerator Context type */
  public final Context context;

  public final MergeInfo mergeInfo;

  public final FlushInfo flushInfo;

  public final ReadAdvice readAdvice;

  public static final IOContext DEFAULT =
      new IOContext(Context.DEFAULT, null, null, ReadAdvice.NORMAL);

  public static final IOContext READONCE = new IOContext(ReadAdvice.SEQUENTIAL);

  public static final IOContext READ = new IOContext(ReadAdvice.NORMAL);

  public static final IOContext PRELOAD = new IOContext(ReadAdvice.RANDOM_PRELOAD);

  public static final IOContext RANDOM = new IOContext(ReadAdvice.RANDOM);

  public IOContext(
      Context context, MergeInfo mergeInfo, FlushInfo flushInfo, ReadAdvice readAdvice) {
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(readAdvice, "readAdvice must not be null");
    switch (context) {
      case MERGE:
        Objects.requireNonNull(mergeInfo, "mergeInfo must not be null if context is MERGE");
        break;
      case FLUSH:
        Objects.requireNonNull(flushInfo, "flushInfo must not be null if context is FLUSH");
        break;
      case READ:
      case DEFAULT:
    }
    if (context == Context.MERGE && readAdvice != ReadAdvice.SEQUENTIAL) {
      throw new IllegalArgumentException(
          "The MERGE context must use the SEQUENTIAL read access advice");
    }
    if ((context == Context.FLUSH || context == Context.DEFAULT)
        && readAdvice != ReadAdvice.NORMAL) {
      throw new IllegalArgumentException(
          "The FLUSH and DEFAULT contexts must use the NORMAL read access advice");
    }
    this.context = context;
    this.mergeInfo = mergeInfo;
    this.flushInfo = flushInfo;
    this.readAdvice = readAdvice;
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, flushInfo, mergeInfo, readAdvice);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    IOContext other = (IOContext) obj;
    if (context != other.context) return false;
    if (!Objects.equals(flushInfo, other.flushInfo)) return false;
    if (!Objects.equals(mergeInfo, other.mergeInfo)) return false;
    if (readAdvice != other.readAdvice) return false;
    return true;
  }

  @Override
  public String toString() {
    return "IOContext [context="
        + context
        + ", mergeInfo="
        + mergeInfo
        + ", flushInfo="
        + flushInfo
        + ", readAdvice="
        + readAdvice;
  }

  private IOContext(ReadAdvice accessAdvice) {
    this(Context.READ, null, null, accessAdvice);
  }

  /** Creates an IOContext for flushing. */
  public IOContext(FlushInfo flushInfo) {
    this(Context.FLUSH, null, flushInfo, ReadAdvice.NORMAL);
  }

  /** Creates an IOContext for merging. */
  public IOContext(MergeInfo mergeInfo) {
    // Merges read input segments sequentially.
    this(Context.MERGE, mergeInfo, null, ReadAdvice.SEQUENTIAL);
  }

  private static final IOContext[] READADVICE_TO_IOCONTEXT =
      Arrays.stream(ReadAdvice.values()).map(IOContext::new).toArray(IOContext[]::new);

  /**
   * Return an updated {@link IOContext} that has the provided {@link ReadAdvice} if the {@link
   * Context} is a {@link Context#DEFAULT} context, otherwise return this existing instance. This
   * helps preserve a {@link ReadAdvice#SEQUENTIAL} advice for merging, which is always the right
   * choice, while allowing {@link IndexInput}s open for searching to use arbitrary {@link
   * ReadAdvice}s.
   */
  public IOContext withReadAdvice(ReadAdvice advice) {
    if (context == Context.DEFAULT) {
      return READADVICE_TO_IOCONTEXT[advice.ordinal()];
    } else {
      return this;
    }
  }
}
