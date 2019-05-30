/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeUtils {

  private static final ThreadLocal<Map<TimeZone, Calendar>> TIMEZONE_CALENDARS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_DATE_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIME_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIMESTAMP_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  public static Calendar getTimeZoneCalendar(final TimeZone timeZone) {
    return TIMEZONE_CALENDARS.get().computeIfAbsent(timeZone, GregorianCalendar::new);
  }

  public static String formatDate(Date date, TimeZone timeZone) {
    return TIMEZONE_DATE_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  public static String formatTime(Date date, TimeZone timeZone) {
    return TIMEZONE_TIME_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  public static String formatTimestamp(Date date, TimeZone timeZone) {
    return TIMEZONE_TIMESTAMP_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  /**
   * Format the given date per the specified timezone with nano seconds in a format supported by SQL.
   * @param date the date to convert to a String
   * @param timezone timezone to apply
   * @return the formatted string
   */

  public static String formatDateTimeOffset(java.sql.Timestamp date, TimeZone timeZone) {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS XXX");
    ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), timeZone.toZoneId());
    return zdt.format(dtf);
  }

  private DateTimeUtils() {
  }
}
