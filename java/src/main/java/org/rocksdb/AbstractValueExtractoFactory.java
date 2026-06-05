// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Each compaction will create a new {@link AbstractCompactionFilter}
 * allowing the application to know about different compactions
 *
 * @param <T> The concrete type of the compaction filter
 */
public abstract class AbstractValueExtractorFactory<T extends AbstractSlice<?>>
    extends RocksObject {

  public AbstractValueExtractorFactory(final long nativeHandle) {
    super(nativeHandle);
  }


  /**
   * A name which identifies this compaction filter
   *
   * The name will be printed to the LOG file on start up for diagnosis
   */
  public abstract String name();

  /**
   * We override {@link RocksCallbackObject#disposeInternal()}
   * as disposing of a TERARKDB_NAMESPACE::AbstractCompactionFilterFactory requires
   * a slightly different approach as it is a std::shared_ptr
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }
  
  private native void disposeInternal(final long handle);
}
