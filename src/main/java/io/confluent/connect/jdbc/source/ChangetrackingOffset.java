/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import java.util.HashMap;
import java.util.Map;

public class ChangetrackingOffset {
  private static final String INCREMENTING_FIELD = "incrementing";

  private final Long incrementingOffset;
  /**
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return 0.
   */
  public ChangetrackingOffset(Long incrementingOffset) {
    this.incrementingOffset = incrementingOffset;
  }

  public long getIncrementingOffset() {
    return incrementingOffset == null ? 0 : incrementingOffset;
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(3);
    if (incrementingOffset != null) {
      map.put(INCREMENTING_FIELD, incrementingOffset);
    }
    return map;
  }

  public static ChangetrackingOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return new ChangetrackingOffset(null);
    }

    Long incr = (Long) map.get(INCREMENTING_FIELD);
    return new ChangetrackingOffset(incr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ChangetrackingOffset that = (ChangetrackingOffset) o;

    return incrementingOffset != null ? incrementingOffset.equals(that.incrementingOffset) : that.incrementingOffset == null;

  }

  @Override
  public int hashCode() {
    return incrementingOffset != null ? incrementingOffset.hashCode() : 0;
  }
}
