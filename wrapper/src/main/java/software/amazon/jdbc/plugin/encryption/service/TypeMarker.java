/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.encryption.service;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public enum TypeMarker {
  STRING(1, String.class),
  INTEGER(2, Integer.class),
  LONG(3, Long.class),
  DOUBLE(4, Double.class),
  FLOAT(5, Float.class),
  BOOLEAN(6, Boolean.class),
  BIG_DECIMAL(7, BigDecimal.class),
  DATE(8, Date.class),
  TIME(9, Time.class),
  TIMESTAMP(10, Timestamp.class),
  LOCAL_DATE(11, LocalDate.class),
  LOCAL_TIME(12, LocalTime.class),
  LOCAL_DATE_TIME(13, LocalDateTime.class),
  BYTE_ARRAY(14, byte[].class),
  GENERIC(99, Object.class);

  private final byte value;
  private final Class<?> type;

  TypeMarker(int value, Class<?> type) {
    this.value = (byte) value;
    this.type = type;
  }

  public byte getValue() {
    return value;
  }

  public Class<?> getType() {
    return type;
  }

  public static TypeMarker fromValue(byte value) {
    for (TypeMarker marker : values()) {
      if (marker.value == value) {
        return marker;
      }
    }
    throw new IllegalArgumentException("Unknown type marker: " + value);
  }

  public static TypeMarker fromObject(Object obj) {
    if (obj instanceof String) return STRING;
    if (obj instanceof Integer) return INTEGER;
    if (obj instanceof Long) return LONG;
    if (obj instanceof Double) return DOUBLE;
    if (obj instanceof Float) return FLOAT;
    if (obj instanceof Boolean) return BOOLEAN;
    if (obj instanceof BigDecimal) return BIG_DECIMAL;
    if (obj instanceof Date) return DATE;
    if (obj instanceof Time) return TIME;
    if (obj instanceof Timestamp) return TIMESTAMP;
    if (obj instanceof LocalDate) return LOCAL_DATE;
    if (obj instanceof LocalTime) return LOCAL_TIME;
    if (obj instanceof LocalDateTime) return LOCAL_DATE_TIME;
    if (obj instanceof byte[]) return BYTE_ARRAY;
    return GENERIC;
  }
}
