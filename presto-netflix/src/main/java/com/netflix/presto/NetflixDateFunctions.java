/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.presto;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.operator.scalar.DateTimeFunctions.addFieldValueDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.addFieldValueTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.addFieldValueTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.dayFromDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.dayFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.dayFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.diffDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.diffTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.diffTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.formatDatetime;
import static io.prestosql.operator.scalar.DateTimeFunctions.formatDatetimeWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.hourFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.hourFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.minuteFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.minuteFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.monthFromDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.monthFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.monthFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.quarterFromDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.quarterFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.quarterFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.secondFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.secondFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.toISO8601FromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.truncateDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.truncateTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.truncateTimestampWithTimezone;
import static io.prestosql.operator.scalar.DateTimeFunctions.weekFromDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.weekFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.weekFromTimestampWithTimeZone;
import static io.prestosql.operator.scalar.DateTimeFunctions.yearFromDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.yearFromTimestamp;
import static io.prestosql.operator.scalar.DateTimeFunctions.yearFromTimestampWithTimeZone;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

public final class NetflixDateFunctions
{
    private static final Slice DATE_INT_FORMAT = utf8Slice("yyyyMMdd");
    private static final long DATE_INT_MAX_THRESHOLD = 100000000L;
    private static final long DATE_INT_MIN_THRESHOLD = 10000000L;
    private static ConcurrentHashMap computedTimeZones = new ConcurrentHashMap<String, TimeZone>();

    private static TimeZone getTimeZone(String timeZoneId)
    {
        return (TimeZone) computedTimeZones.computeIfAbsent(timeZoneId, timezoneId -> TimeZone.getTimeZone(timeZoneId));
    }

    private NetflixDateFunctions()
    {
    }

    // Convert a dateint in the format 'yyyyMMdd' to Java local date
    private static LocalDate toLocalDate(int dateInt)
    {
        if (dateInt >= DATE_INT_MIN_THRESHOLD && dateInt < DATE_INT_MAX_THRESHOLD) {
            return LocalDate.of(dateInt / 10000, dateInt / 100 % 100, dateInt % 100);
        }
        else {
            throw new IllegalArgumentException("Input must have eight digits in the format 'yyyyMMdd'");
        }
    }

    protected static long toDateInt(LocalDate localDate)
    {
        return localDate.getYear() * 10000 + localDate.getMonthValue() * 100 + localDate.getDayOfMonth();
    }

    protected static LocalDate toLocalDate(long epochMs, TimeZoneKey timeZoneKey)
    {
        return Instant.ofEpochMilli(epochMs).atZone(ZoneId.of(timeZoneKey.getId())).toLocalDate();
    }

    private static LocalDate toLocalDate(Slice dateStr, Slice format)
    {
        java.time.format.DateTimeFormatter dateTimeFormatter =
                java.time.format.DateTimeFormatter.ofPattern(format.toStringUtf8());
        return LocalDate.parse(dateStr.toStringUtf8(), dateTimeFormatter);
    }

    private static LocalDateTime toLocalDateTime(Slice dateStr, Slice format)
    {
        java.time.format.DateTimeFormatter dateTimeFormatter =
                java.time.format.DateTimeFormatter.ofPattern(format.toStringUtf8());
        return LocalDateTime.parse(dateStr.toStringUtf8(), dateTimeFormatter);
    }

    private static boolean isInvalidDate(int year, int month, int day)
    {
        try {
            LocalDate.of(year, month, day);
            return false;
        }
        catch (Exception e) {
            return true;
        }
    }

    /**
     * Parses a given string to the corresponding a corresponding epoch micros.
     * The following formats are allowed:
     *
     * `yyyy`
     * `yyyy-[m]m`
     * `yyyy-[m]m-[d]d`
     * `yyyy-[m]m-[d]d `
     * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
     * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
     * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
     * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
     * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
     * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
     * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
     * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
     * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
     * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
     * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
     * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
     * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
     * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
     * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
     * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
     */
    private static long stringToTimestamp(String s, TimeZone timeZone)
    {
        if (s == null) {
            throw new NullPointerException("Cannot convert null to timestamp");
        }
        Byte tz = null;
        int[] segments = {1, 1, 1, 0, 0, 0, 0, 0, 0};
        int i = 0;
        int currentSegmentValue = 0;
        byte[] bytes = s.getBytes();
        int j = 0;
        int digitsMilli = 0;
        boolean justTime = false;
        while (j < bytes.length) {
            byte b = bytes[j];
            byte parsedValue = (byte) (b - (byte) '0');
            if (parsedValue < 0 || parsedValue > 9) {
                if (j == 0 && b == 'T') {
                    justTime = true;
                    i += 3;
                }
                else if (i < 2) {
                    if (b == '-') {
                        if (i == 0 && j != 4) {
                            throw new IllegalArgumentException("Year should have exact four digits");
                        }
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                    }
                    else if (i == 0 && b == ':') {
                        justTime = true;
                        segments[3] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i = 4;
                    }
                    else {
                        throw new IllegalArgumentException("Could not convert string to timestamp");
                    }
                }
                else if (i == 2) {
                    if (b == ' ' || b == 'T') {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                    }
                    else {
                        throw new IllegalArgumentException("Could not convert string to timestamp");
                    }
                }
                else if (i == 3 || i == 4) {
                    if (b == ':') {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                    }
                    else {
                        throw new IllegalArgumentException("Could not convert string to timestamp");
                    }
                }
                else if (i == 5 || i == 6) {
                    if (b == 'Z') {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                        tz = 43;
                    }
                    else if (b == '-' || b == '+') {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                        tz = b;
                    }
                    else if (b == '.' && i == 5) {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                    }
                    else {
                        throw new IllegalArgumentException("Could not convert string to timestamp");
                    }
                    if (i == 6 && b != '.') {
                        i += 1;
                    }
                }
                else {
                    if (b == ':' || b == ' ') {
                        segments[i] = currentSegmentValue;
                        currentSegmentValue = 0;
                        i += 1;
                    }
                    else {
                        throw new IllegalArgumentException("Could not convert string to timestamp");
                    }
                }
            }
            else {
                if (i == 6) {
                    digitsMilli += 1;
                }
                currentSegmentValue = currentSegmentValue * 10 + parsedValue;
            }
            j += 1;
        }

        segments[i] = currentSegmentValue;
        if (!justTime && i == 0 && j != 4) {
            throw new IllegalArgumentException("Year should have exact four digits");
        }

        while (digitsMilli < 6) {
            segments[6] *= 10;
            digitsMilli += 1;
        }

        if (!justTime && isInvalidDate(segments[0], segments[1], segments[2])) {
            throw new IllegalArgumentException("Could not convert string to timestamp");
        }

        // Instead of return None, we truncate the fractional seconds to prevent inserting NULL
        if (segments[6] > 999999) {
            segments[6] = Integer.valueOf(String.valueOf(segments[6]).substring(0, 6));
        }

        if (segments[3] < 0 || segments[3] > 23 || segments[4] < 0 || segments[4] > 59 ||
                segments[5] < 0 || segments[5] > 59 || segments[6] < 0 || segments[6] > 999999 ||
                segments[7] < 0 || segments[7] > 23 || segments[8] < 0 || segments[8] > 59) {
            throw new IllegalArgumentException("Could not convert string to timestamp");
        }

        Calendar c;
        if (tz == null) {
            c = Calendar.getInstance(timeZone);
        }
        else {
            c = Calendar.getInstance(getTimeZone(String.format("GMT%s%02d:%02d", timeZone, segments[7], segments[8])));
        }

        c.set(Calendar.MILLISECOND, 0);

        if (justTime) {
            c.set(Calendar.HOUR_OF_DAY, segments[3]);
            c.set(Calendar.MINUTE, segments[4]);
            c.set(Calendar.SECOND, segments[5]);
        }
        else {
            c.set(segments[0], segments[1] - 1, segments[2], segments[3], segments[4], segments[5]);
        }
        return c.getTimeInMillis() * 1000 + segments[6];
    }

    private static long getEpochMs(long value)
    {
        int length = (int) (Math.log10(value) + 1);
        if (length == 10) {
            // Epoch seconds
            return value * 1000L;
        }
        else if (length == 13) {
            // Epoch milliseconds
            return value;
        }
        else {
            throw new IllegalArgumentException("Only 10 (epoch) or 13 (epochMs) digit numbers are accepted.");
        }
    }

    private static <T> T handleExceptions(Callable<T> callable, T defaultvalue)
    {
        try {
            return callable.call();
        }
        catch (Exception ex) {
            return defaultvalue;
        }
    }

    @ScalarFunction("nf_dateint")
    @Description("Returns the input value if it s a valid dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateInt(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return toDateInt(toLocalDate(getEpochMs(dateInt), session.getTimeZoneKey()));
            }

            toLocalDate((int) dateInt);
            return dateInt;
        }, null);
    }

    @ScalarFunction("nf_dateint")
    @Description("Convert a date string in the format 'yyyy-MM-dd'  or 'yyyyMMdd' to dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateInt(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return toDateInt(toLocalDate(dateString, DATE_INT_FORMAT));
            }
            else if (dateString.length() == 10) {
                return toDateInt(LocalDate.parse(dateString.toStringUtf8()));
            }
            else {
                long epochMillis = stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return nfTimestampToDateInt(session, epochMillis);
            }
        }, null);
    }

    @ScalarFunction("nf_dateint")
    @Description("Convert a date string in a given format to dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateInt(@SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            return toDateInt(toLocalDate(dateString, format));
        }, null);
    }

    @ScalarFunction("nf_dateint")
    @Description("Convert a date to dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateToDateInt(@SqlType(DATE) long epochDays)
    {
        return handleExceptions(() -> {
            return toDateInt(LocalDate.ofEpochDay(epochDays));
        }, null);
    }

    @ScalarFunction("nf_dateint")
    @Description("Convert a timestamp to dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfTimestampToDateInt(ConnectorSession session, @SqlType(TIMESTAMP) long epochMs)
    {
        return handleExceptions(() -> {
            return toDateInt(toLocalDate(epochMs, session.getTimeZoneKey()));
        }, null);
    }

    @ScalarFunction("nf_dateint")
    @Description("Convert a timestamp with timezone to dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfTimestampWithTimezoneToDateInt(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            LocalDate date = Instant.ofEpochMilli(unpackMillisUtc(epochMs))
                    .atZone(ZoneId.of(unpackZoneKey(epochMs).getId())).toLocalDate();
            return toDateInt(date);
        }, null);
    }

    @ScalarFunction("nf_dateint_today")
    @Description("Returns dateint value at the start of the query session")
    @SqlType(INTEGER)
    public static long nfDateIntToday(ConnectorSession session)
    {
        return toDateInt(toLocalDate(session.getStartTime(), session.getTimeZoneKey()));
    }

    @ScalarFunction("nf_unixtime_now")
    @Description("Returns unix time / epoch seconds at the start of the session")
    @SqlType(BIGINT)
    public static long nfUnixtimeNow(ConnectorSession session)
    {
        return (session.getStartTime() / 1000);
    }

    @ScalarFunction("nf_unixtime_now_ms")
    @Description("Returns unix time / epoch milliseconds at the start of the session")
    @SqlType(BIGINT)
    public static long nfUnixtimeNowMs(ConnectorSession session)
    {
        return session.getStartTime();
    }

    private static long toUnixTime(LocalDate localDate, TimeZoneKey timeZoneKey)
    {
        return localDate.atStartOfDay(ZoneId.of(timeZoneKey.getId())).toEpochSecond();
    }

    private static long toUnixTimeMs(LocalDateTime localDateTime, TimeZoneKey timeZoneKey)
    {
        return localDateTime.atZone(ZoneId.of(timeZoneKey.getId())).toInstant().toEpochMilli();
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert dateint to epoch seconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateIntToUnixTime(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return getEpochMs(dateInt) / 1000;
            }
            return toUnixTime(toLocalDate((int) dateInt), session.getTimeZoneKey());
        }, null);
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert date string in the format 'yyyy-MM-dd'  or 'yyyyMMdd' to epoch seconds. ")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateStrToUnixTime(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT), session.getTimeZoneKey());
            }
            else if (dateString.length() == 10) {
                return toUnixTime(LocalDate.parse(dateString.toStringUtf8()), session.getTimeZoneKey());
            }
            else {
                return stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000000L;
            }
        }, null);
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert date string in the specified format to epoch seconds. ")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateStrToUnixTime(ConnectorSession session, @SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            try {
                return toUnixTimeMs(toLocalDateTime(dateString, format), session.getTimeZoneKey()) / 1000L;
            }
            catch (Exception e) {
                if (e instanceof DateTimeParseException) {
                    return toUnixTime(toLocalDate(dateString, format), session.getTimeZoneKey());
                }
                else {
                    throw e;
                }
            }
        }, null);
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert date to epoch seconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateToUnixTime(ConnectorSession session, @SqlType(DATE) long epochDays)
    {
        return handleExceptions(() -> {
            return toUnixTime(LocalDate.ofEpochDay(epochDays), session.getTimeZoneKey());
        }, null);
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert timestamp to epoch seconds.")
    @SqlType(BIGINT)
    public static long nfTimestampToUnixTime(@SqlType(TIMESTAMP) long epochMs)
    {
        return epochMs / 1000L;
    }

    @ScalarFunction("nf_to_unixtime")
    @Description("Convert timestamp with timezone to epoch seconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneToUnixTime(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            return unpackMillisUtc(epochMs) / 1000L;
        }, null);
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert dateint to epoch ms.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateIntToUnixTimeMs(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return getEpochMs(dateInt);
            }
            return toUnixTime(toLocalDate((int) dateInt), session.getTimeZoneKey()) * 1000L;
        }, null);
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert date string in 'yyyy-MM-dd' format to milliseconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateStrToUnixTimeMs(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT), session.getTimeZoneKey()) * 1000L;
            }
            else if (dateString.length() == 10) {
                return toUnixTime(LocalDate.parse(dateString.toStringUtf8()), session.getTimeZoneKey()) * 1000L;
            }
            else {
                return stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
            }
        }, null);
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert date string in a given format to epoch milliseconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateStrToUnixTimeMs(ConnectorSession session, @SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            try {
                return toUnixTimeMs(toLocalDateTime(dateString, format), session.getTimeZoneKey());
            }
            catch (Exception e) {
                if (e instanceof DateTimeParseException) {
                    return toUnixTime(toLocalDate(dateString, format), session.getTimeZoneKey()) * 1000L;
                }
                else {
                    throw e;
                }
            }
        }, null);
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert date to epoch milliseconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateToUnixTimeMs(ConnectorSession session, @SqlType(DATE) long epochDays)
    {
        return handleExceptions(() -> {
            return toUnixTime(LocalDate.ofEpochDay(epochDays), session.getTimeZoneKey()) * 1000L;
        }, null);
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert timestamp to epoch milliseconds.")
    @SqlType(BIGINT)
    public static long nfToUnixTimeMs(@SqlType(TIMESTAMP) long epochMs)
    {
        return epochMs;
    }

    @ScalarFunction("nf_to_unixtime_ms")
    @Description("Convert timestamp to epoch seconds.")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneToUnixTimeMs(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            return unpackMillisUtc(epochMs);
        }, null);
    }

    @ScalarFunction("nf_from_unixtime")
    @Description("Convert epoch seconds to timestamp. Currently this will return the timestamp in the session " +
            "time zone. After an upstream patch, this will always return the UTC timestamp ")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfFromUnixTime(@SqlType(BIGINT) long epoch)
    {
        return handleExceptions(() -> {
            return getEpochMs(epoch);
        }, null);
    }

    @ScalarFunction("nf_from_unixtime")
    @Description("Convert epoch seconds to timestamp in a given format")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfFromUnixTime(ConnectorSession session, @SqlType(BIGINT) long epoch, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            return nfFromUnixTimeMs(session, getEpochMs(epoch), format);
        }, null);
    }

    @ScalarFunction("nf_from_unixtime_ms")
    @Description("Convert epoch milliseconds to timestamp.")
    @SqlType(TIMESTAMP)
    public static long nfFromUnixTimeMs(@SqlType(BIGINT) long epochMilliSeconds)
    {
        return epochMilliSeconds;
    }

    @ScalarFunction("nf_from_unixtime_ms")
    @Description("Convert epoch milliseconds to timestamp in a given format")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfFromUnixTimeMs(ConnectorSession session, @SqlType(BIGINT) long epochMilliSeconds,
                                         @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            DateTimeFormatter df = DateTimeFormat.forPattern(format.toStringUtf8())
                    .withChronology(getChronology(session.getTimeZoneKey()));
            return utf8Slice(df.print(epochMilliSeconds));
        }, null);
    }

    @ScalarFunction("nf_from_unixtime_tz")
    @Description("Convert epoch seconds to timestamp in a given timezone")
    @SqlType(TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfFromUnixTimeTz(@SqlType(BIGINT) long epoch,
                                        @SqlType(VARCHAR) Slice timezone)
    {
        return handleExceptions(() -> {
            return packDateTimeWithZone(getEpochMs(epoch), timezone.toStringUtf8());
        }, null);
    }

    @ScalarFunction("nf_from_unixtime_ms_tz")
    @Description("Convert epoch milli seconds to timestamp in a given timezone")
    @SqlType(TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfFromUnixTimeMsTz(@SqlType(BIGINT) long epochMilliSeconds,
                                          @SqlType(VARCHAR) Slice timezone)
    {
        return handleExceptions(() -> {
            return packDateTimeWithZone(epochMilliSeconds, timezone.toStringUtf8());
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Returns the string value if it is a valid date in ‘yyyy-MM-dd’ or 'yyyyMMdd' format")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateString(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return utf8Slice(toLocalDate(dateString, DATE_INT_FORMAT).toString());
            }
            else if (dateString.length() == 10) {
                LocalDate.parse(dateString.toStringUtf8());
                return dateString;
            }
            else {
                long epochMillis = stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return nfTimestampToDateStr(session, epochMillis);
            }
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Parses a date string in a given format and returns a date string in 'yyyy-MM-dd'")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateString(@SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            return utf8Slice(toLocalDate(dateString, format).toString());
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Convert a dateint in ‘yyyyMMdd’ format to date string")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateString(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return utf8Slice(toLocalDate(getEpochMs(dateInt), session.getTimeZoneKey()).toString());
            }
            return utf8Slice(toLocalDate((int) dateInt).toString());
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Convert a date to date string")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateToDateStr(@SqlType(DATE) long epochDays)
    {
        return handleExceptions(() -> {
            return utf8Slice(LocalDate.ofEpochDay(epochDays).toString());
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Convert a timestamp to date string")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfTimestampToDateStr(ConnectorSession session, @SqlType(TIMESTAMP) long epochMs)
    {
        return handleExceptions(() -> {
            return utf8Slice(toLocalDate(epochMs, session.getTimeZoneKey()).toString());
        }, null);
    }

    @ScalarFunction("nf_datestr")
    @Description("Convert a timestamp with timezone to date string")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfTimestampWithTimeZoneToDateStr(ConnectorSession session, @SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            LocalDate date = Instant.ofEpochMilli(unpackMillisUtc(epochMs))
                    .atZone(ZoneId.of(unpackZoneKey(epochMs).getId())).toLocalDate();
            return utf8Slice(date.toString());
        }, null);
    }

    @ScalarFunction("nf_datestr_today")
    @Description("Returns datestr value at the start of the query session")
    @SqlType(VARCHAR)
    public static Slice nfDateStrToday(ConnectorSession session)
    {
        return utf8Slice(toLocalDate(session.getStartTime(), session.getTimeZoneKey()).toString());
    }

    @ScalarFunction("nf_date")
    @Description("Converts a date string in the format 'yyyy-MM-dd'  or 'yyyyMMdd' to date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfDate(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay();
            }
            else if (dateString.length() == 10) {
                return LocalDate.parse(dateString.toStringUtf8()).toEpochDay();
            }
            else {
                long epochMillis = stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return nfTimestampToDate(session, epochMillis);
            }
        }, null);
    }

    @ScalarFunction("nf_date")
    @Description("Converts a date string in a given format to date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfDate(@SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            return toLocalDate(dateString, format).toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_date")
    @Description("Convert a dateint in ‘yyyyMMdd’ format to date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfDateInttoDate(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return toLocalDate(getEpochMs(dateInt), session.getTimeZoneKey()).toEpochDay();
            }
            return toLocalDate((int) dateInt).toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_date")
    @Description("Return the date value")
    @SqlType(DATE)
    public static long nfDate(@SqlType(DATE) long epochDays)
    {
        return epochDays;
    }

    @ScalarFunction("nf_date")
    @Description("Convert timestamp to date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfTimestampToDate(ConnectorSession session, @SqlType(TIMESTAMP) long epochMs)
    {
        return handleExceptions(() -> {
            return toLocalDate(epochMs, session.getTimeZoneKey()).toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_date")
    @Description("Convert timestamp with timezone to date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneToDate(ConnectorSession session, @SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            LocalDate date = Instant.ofEpochMilli(unpackMillisUtc(epochMs))
                    .atZone(ZoneId.of(unpackZoneKey(epochMs).getId())).toLocalDate();
            return date.toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_date_today")
    @Description("Returns date value at the start of the query session")
    @SqlType(DATE)
    public static long nfDateToday(ConnectorSession session)
    {
        return handleExceptions(() -> {
            return toLocalDate(session.getStartTime(), session.getTimeZoneKey()).toEpochDay();
        }, null);
    }

    private static long toUnixTimeMs(Slice dateStr, Slice format, TimeZoneKey timeZoneKey)
    {
        return handleExceptions(() -> {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(format.toStringUtf8())
                    .withChronology(getChronology(timeZoneKey))
                    .withOffsetParsed();
            return DateTime.parse(dateStr.toStringUtf8(), formatter).toDateTime().getMillis();
        }, null);
    }

    @ScalarFunction("nf_timestamp")
    @Description("Converts date string in the format 'yyyy-MM-dd'  or 'yyyyMMdd' to timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfTimestamp(ConnectorSession session, @SqlType(VARCHAR) Slice dateString)
    {
        return handleExceptions(() -> {
            if (dateString.length() == 8) {
                return toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT), session.getTimeZoneKey()) * 1000L;
            }
            else if (dateString.length() == 10) {
                return toUnixTime(LocalDate.parse(dateString.toStringUtf8()), session.getTimeZoneKey()) * 1000L;
            }
            else {
                long epochMillis = stringToTimestamp(dateString.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return epochMillis;
            }
        }, null);
    }

    @ScalarFunction("nf_timestamp")
    @Description("Converts date string in the given format")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfTimestamp(ConnectorSession session, @SqlType(VARCHAR) Slice dateString, @SqlType(VARCHAR) Slice format)
    {
        return handleExceptions(() -> {
            try {
                return toUnixTimeMs(toLocalDateTime(dateString, format), session.getTimeZoneKey());
            }
            catch (Exception e) {
                if (e instanceof DateTimeParseException) {
                    return toUnixTime(toLocalDate(dateString, format), session.getTimeZoneKey()) * 1000L;
                }
                else {
                    throw e;
                }
            }
        }, null);
    }

    @ScalarFunction("nf_timestamp")
    @Description("Convert a dateint in ‘yyyyMMdd’ format to timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfDateintToTimestamp(ConnectorSession session, @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return getEpochMs(dateInt);
            }
            else {
                return toUnixTime(toLocalDate((int) dateInt), session.getTimeZoneKey()) * 1000L;
            }
        }, null);
    }

    @ScalarFunction("nf_timestamp")
    @Description("Convert a date to timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfDateToTimestamp(ConnectorSession session, @SqlType(DATE) long epochDays)
    {
        return handleExceptions(() -> {
            return toUnixTime(LocalDate.ofEpochDay(epochDays), session.getTimeZoneKey()) * 1000L;
        }, null);
    }

    @ScalarFunction("nf_timestamp")
    @Description("Returns the timestamp")
    @SqlType(TIMESTAMP)
    public static long nfTimestamp(@SqlType(TIMESTAMP) long epochMs)
    {
        return epochMs;
    }

    @ScalarFunction("nf_timestamp")
    @Description("Returns the timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfTimestampWithTimeZone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long epochMs)
    {
        return handleExceptions(() -> {
            return unpackMillisUtc(epochMs);
        }, null);
    }

    @ScalarFunction("nf_timestamp_now")
    @Description("Returns the timestamp at the start of the query session")
    @SqlType(TIMESTAMP)
    public static long nfTimestampNow(ConnectorSession session)
    {
        return session.getStartTime();
    }

    private static LocalDate addOffsetExpr(LocalDate startDate, Slice offsetExpr)
    {
        Pattern offsetPattern = Pattern.compile("([+-]?\\d+)(['yMd'])");
        Matcher matcher = offsetPattern.matcher(offsetExpr.toStringUtf8());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid offset expression " + offsetExpr);
        }

        String unit = matcher.group(2);
        long offset = Long.valueOf(matcher.group(1));
        switch (unit) {
            case "y":
                return startDate.plusYears(offset);
            case "M":
                return startDate.plusMonths(offset);
            case "d":
                return startDate.plusDays(offset);
            default:
                throw new IllegalArgumentException("Invalid offset expression " + offsetExpr);
        }
    }

    private static long addOffsetExpr(ConnectorSession session, long timestamp, Slice offsetExpr)
    {
        Pattern offsetPattern = Pattern.compile("([+-]?\\d+)(['yMd'])");
        Matcher matcher = offsetPattern.matcher(offsetExpr.toStringUtf8());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid offset expression " + offsetExpr);
        }

        String unitExpr = matcher.group(2);
        Slice unit = null;
        long offset = Long.valueOf(matcher.group(1));
        switch (unitExpr) {
            case "y":
                unit = utf8Slice("year");
            case "M":
                unit = utf8Slice("month");
            case "d":
                unit = utf8Slice("day");
        }
        if (unit == null) {
            throw new IllegalArgumentException("Invalid offset expression " + offsetExpr);
        }
        return dateIntAddEpoch(session, timestamp, offset, unit);
    }

    private static long addOffsetExprWithTz(long timestamp, Slice offsetExpr)
    {
        Pattern offsetPattern = Pattern.compile("([+-]?\\d+)(['yMd'])");
        Matcher matcher = offsetPattern.matcher(offsetExpr.toStringUtf8());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid offset expression " + offsetExpr);
        }

        String unit = matcher.group(2);
        long offset = Long.valueOf(matcher.group(1));
        switch (unit) {
            case "y":
                return addFieldValueTimestampWithTimeZone(utf8Slice("year"), offset, timestamp);
            case "M":
                return addFieldValueTimestampWithTimeZone(utf8Slice("month"), offset, timestamp);
            case "d":
                return addFieldValueTimestampWithTimeZone(utf8Slice("day"), offset, timestamp);
            default:
                throw new IllegalArgumentException("Invalid offset expression " + offsetExpr);
        }
    }

    private static long dateIntAddEpoch(ConnectorSession session, long startDate, long numDays, Slice unit)
    {
        long epochMs = getEpochMs(startDate);
        boolean isEpoch = false;
        if (epochMs != startDate) {
            isEpoch = true;
        }
        long result = addFieldValueTimestamp(session, unit, numDays, epochMs);
        if (isEpoch) {
            result = result / 1000L;
        }
        return result;
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input dateint and return a dateint")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateIntAdd(ConnectorSession session, @SqlType(INTEGER) long startDate,
                                    @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            if (startDate > DATE_INT_MAX_THRESHOLD) {
                return dateIntAddEpoch(session, startDate, numDays, utf8Slice("day"));
            }
            return toDateInt(toLocalDate((int) startDate).plusDays(numDays));
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input dateint and return a dateint")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateBigIntAdd(ConnectorSession session, @SqlType(BIGINT) long startDate,
                                    @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            if (startDate > DATE_INT_MAX_THRESHOLD) {
                return dateIntAddEpoch(session, startDate, numDays, utf8Slice("day"));
            }
            return toDateInt(toLocalDate((int) startDate).plusDays(numDays));
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input dateint and return a dateint" +
            "Offset expression is in the format (+/-)(0-9)(y/M/d). Eg: 3y, -2M, 5d")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateIntAdd(ConnectorSession session, @SqlType(INTEGER) long startDate,
                                    @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            if (startDate > DATE_INT_MAX_THRESHOLD) {
                return addOffsetExpr(session, startDate, offsetExpression);
            }
            return toDateInt(addOffsetExpr(toLocalDate((int) startDate), offsetExpression));
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input dateint and return a dateint" +
            "Offset expression is in the format (+/-)(0-9)(y/M/d). Eg: 3y, -2M, 5d")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateBigIntAdd(ConnectorSession session, @SqlType(BIGINT) long startDate,
                                 @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            if (startDate > DATE_INT_MAX_THRESHOLD) {
                return addOffsetExpr(session, startDate, offsetExpression);
            }
            return toDateInt(addOffsetExpr(toLocalDate((int) startDate), offsetExpression));
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input dateint and return the result as a dateint." +
            "Supported units are 'year', 'month','day', 'week', 'quarter'")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long nfDateIntAdd(ConnectorSession session, @SqlType(VARCHAR) Slice unit, @SqlType(BIGINT) long value,
                                    @SqlType(INTEGER) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return dateIntAddEpoch(session, dateInt, value, unit);
            }
            long epochDay = addFieldValueDate(unit, value, nfDateInttoDate(session, dateInt));
            return nfDateToDateInt(epochDay);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input dateint and return the result as a dateint." +
            "Supported units are 'year', 'month','day', 'week', 'quarter'")
    @SqlType(BIGINT)
    @SqlNullable
    public static Long nfDateBigIntAdd(ConnectorSession session, @SqlType(VARCHAR) Slice unit, @SqlType(BIGINT) long value,
                                    @SqlType(BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return dateIntAddEpoch(session, dateInt, value, unit);
            }
            long epochDay = addFieldValueDate(unit, value, nfDateInttoDate(session, dateInt));
            return nfDateToDateInt(epochDay);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input datestring and return a datestring")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateStrAdd(ConnectorSession session, @SqlType(VARCHAR) Slice startDate,
                                     @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            if (startDate.length() == 8) {
                return utf8Slice(String.valueOf(nfDateIntAdd(session, nfDateInt(session, startDate), numDays)));
            }
            else if (startDate.length() == 10) {
                LocalDate resultDate = LocalDate.parse(startDate.toStringUtf8()).plusDays(numDays);
                return utf8Slice(resultDate.toString());
            }
            else {
                //Assume it is a ISO format timestamp string
                long epochMillis = stringToTimestamp(startDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return toISO8601FromTimestampWithTimeZone(addFieldValueTimestampWithTimeZone(utf8Slice("day"),
                        numDays, nfFromUnixTimeMsTz(epochMillis, utf8Slice(session.getTimeZoneKey().getId()))));
            }
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input datestring and return a datestring")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateStrAdd(ConnectorSession session, @SqlType(VARCHAR) Slice startDate,
                                     @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            if (startDate.length() == 8) {
                return utf8Slice(String.valueOf(nfDateIntAdd(session, nfDateInt(session, startDate), offsetExpression)));
            }
            else if (startDate.length() == 10) {
                LocalDate resultDate = addOffsetExpr(LocalDate.parse(startDate.toStringUtf8()), offsetExpression);
                return utf8Slice(resultDate.toString());
            }
            else {
                long epochMillis = stringToTimestamp(startDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return toISO8601FromTimestampWithTimeZone(addOffsetExprWithTz(nfFromUnixTimeMsTz(epochMillis,
                        utf8Slice(session.getTimeZoneKey().getId())), offsetExpression));
            }
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input datestring and return the result as a datestring")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice nfDateStrAdd(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                     @SqlType(BIGINT) long value, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8) {
                return utf8Slice(String.valueOf(nfDateIntAdd(session, unit, value, nfDateInt(session, dateStr))));
            }
            else if (dateStr.length() == 10) {
                long epochDay = addFieldValueDate(unit, value, LocalDate.parse(dateStr.toStringUtf8()).toEpochDay());
                return nfDateToDateStr(epochDay);
            }
            else {
                long epochMillis = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return toISO8601FromTimestampWithTimeZone(addFieldValueTimestampWithTimeZone(unit,
                        value, nfFromUnixTimeMsTz(epochMillis, utf8Slice(session.getTimeZoneKey().getId()))));
            }
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input date and return a date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfDateAdd(@SqlType(DATE) long startDate,
                              @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            LocalDate resultDate = LocalDate.ofEpochDay(startDate).plusDays(numDays);
            return resultDate.toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input date and return a date")
    @SqlType(DATE)
    @SqlNullable
    public static Long nfDateAdd(@SqlType(DATE) long startDate,
                              @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            LocalDate resultDate = addOffsetExpr(LocalDate.ofEpochDay(startDate), offsetExpression);
            return resultDate.toEpochDay();
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input date and return the result as a date")
    @SqlType(StandardTypes.DATE)
    @SqlNullable
    public static Long nfDateAdd(@SqlType(VARCHAR) Slice unit,
                                 @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DATE) long date)
    {
        return handleExceptions(() -> {
            return addFieldValueDate(unit, value, date);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input timestamp and return a timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfTimestampAdd(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp,
                                 @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            return addFieldValueTimestamp(session, utf8Slice("day"), numDays, timestamp);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input timestamp and return a timestamp")
    @SqlType(TIMESTAMP)
    @SqlNullable
    public static Long nfTimestampAdd(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp,
                                 @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            return addOffsetExpr(session, timestamp, offsetExpression);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input timestamp and return the result as a timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    @SqlNullable
    public static Long nfTimestampAdd(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                      @SqlType(StandardTypes.BIGINT) long value,
                                      @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return addFieldValueTimestamp(session, unit, value, timestamp);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add numDays to the input timestamp with timezone  and return a timestamp with timezone ")
    @SqlType(TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneAdd(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp,
                                                  @SqlType(BIGINT) long numDays)
    {
        return handleExceptions(() -> {
            return addFieldValueTimestampWithTimeZone(utf8Slice("day"), numDays, timestamp);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the offset to the input timestamp with timezone and return a timestamp with timezone ")
    @SqlType(TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneAdd(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp,
                                 @SqlType(VARCHAR) Slice offsetExpression)
    {
        return handleExceptions(() -> {
            return addOffsetExprWithTz(timestamp, offsetExpression);
        }, null);
    }

    @ScalarFunction("nf_dateadd")
    @Description("Add the value in terms of unit to the input timestamp with timezone and return the result " +
            "as a timestamp with timezone")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneAdd(@SqlType(VARCHAR) Slice unit,
                                                  @SqlType(StandardTypes.BIGINT) long value,
                                                  @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return addFieldValueTimestampWithTimeZone(unit, value, timestamp);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in the number of days between two given dateints")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateIntDiff(ConnectorSession session, @SqlType(StandardTypes.BIGINT) long startDate,
                                     @SqlType(StandardTypes.BIGINT) long endDate)
    {
        return handleExceptions(() -> {
            if ((startDate > DATE_INT_MAX_THRESHOLD) || (endDate > DATE_INT_MAX_THRESHOLD)) {
                if (!((endDate > DATE_INT_MAX_THRESHOLD) && (startDate > DATE_INT_MAX_THRESHOLD))) {
                    throw new IllegalArgumentException("Both inputs must either be dateints or epochs");
                }
                else {
                    return diffTimestamp(session, utf8Slice("day"), getEpochMs(startDate), getEpochMs(endDate));
                }
            }
            return diffDate(utf8Slice("day"), nfDateInttoDate(session, startDate), nfDateInttoDate(session, endDate));
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in the number of days between two given datesrings")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateStrDiff(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice startDate,
                                     @SqlType(StandardTypes.VARCHAR) Slice endDate)
    {
        return handleExceptions(() -> {
            if (startDate.length() == 8) {
                if (endDate.length() != 8) {
                    throw new IllegalArgumentException("Both start and end date must be in the same type and format");
                }
                return diffDate(utf8Slice("day"), nfDate(session, startDate), nfDate(session, endDate));
            }
            if (startDate.length() == 10) {
                if (endDate.length() != 10) {
                    throw new IllegalArgumentException("Both start and end date must be in the same type and format");
                }
                return diffDate(utf8Slice("day"), nfDate(session, startDate), nfDate(session, endDate));
            }

            long startTimestamp = stringToTimestamp(startDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
            long endTimestamp = stringToTimestamp(endDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
            return diffTimestamp(session, utf8Slice("day"), startTimestamp, endTimestamp);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in the number of days between two given dates")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateDiff(@SqlType(StandardTypes.DATE) long startDate,
                                  @SqlType(StandardTypes.DATE) long endDate)
    {
        return handleExceptions(() -> {
            return diffDate(utf8Slice("day"), startDate, endDate);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in the number of days between two given timestamps")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfTimestampDiff(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp,
                                       @SqlType(StandardTypes.TIMESTAMP) long endTimestamp)
    {
        return handleExceptions(() -> {
            return diffTimestamp(session, utf8Slice("day"), startTimestamp, endTimestamp);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in the number of days between two given timestamps with timezone")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfTimestampWithTimezoneDiff(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp,
                                                   @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long endTimestamp)
    {
        return handleExceptions(() -> {
            return diffTimestampWithTimeZone(utf8Slice("day"), startTimestamp, endTimestamp);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in terms of the unit between two given dateints")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateIntDiff(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                     @SqlType(StandardTypes.BIGINT) long startDate,
                                     @SqlType(StandardTypes.BIGINT) long endDate)
    {
        return handleExceptions(() -> {
            if (startDate > DATE_INT_MAX_THRESHOLD) {
                return diffTimestamp(session, unit, startDate, endDate);
            }
            return diffDate(unit, nfDateInttoDate(session, startDate), nfDateInttoDate(session, endDate));
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in terms of the unit between two given datesrings")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateStrDiff(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                     @SqlType(StandardTypes.VARCHAR) Slice startDate,
                                     @SqlType(StandardTypes.VARCHAR) Slice endDate)
    {
        return handleExceptions(() -> {
            if (startDate.length() == 8 || startDate.length() == 10) {
                if (startDate.length() != endDate.length()) {
                    throw new IllegalArgumentException("Both start and end date must be in the same type and format");
                }
                return diffDate(unit, nfDate(session, startDate), nfDate(session, endDate));
            }
            else {
                long startTimestamp = stringToTimestamp(startDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                long endTimestamp = stringToTimestamp(endDate.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return diffTimestamp(session, unit, startTimestamp, endTimestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in terms of the unit  between two given dates")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateDiff(@SqlType(VARCHAR) Slice unit,
                                  @SqlType(StandardTypes.DATE) long startDate,
                                  @SqlType(StandardTypes.DATE) long endDate)
    {
        return handleExceptions(() -> {
            return diffDate(unit, startDate, endDate);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in terms of the unit  between two given timestamps")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfTimestampDiff(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                       @SqlType(StandardTypes.TIMESTAMP) long startTimestamp,
                                       @SqlType(StandardTypes.TIMESTAMP) long endTimestamp)
    {
        return handleExceptions(() -> {
            return diffTimestamp(session, unit, startTimestamp, endTimestamp);
        }, null);
    }

    @ScalarFunction("nf_datediff")
    @Description("Difference in terms of the unit  between two given timestamps with timezone")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfTimestampWithTimezoneDiff(@SqlType(VARCHAR) Slice unit,
                                                   @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp,
                                                   @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long endTimestamp)
    {
        return handleExceptions(() -> {
            return diffTimestampWithTimeZone(unit, startTimestamp, endTimestamp);
        }, null);
    }

    private static long dateIntEpochTrunc(ConnectorSession session, Slice unit, long dateInt)
    {
        long epochMs = getEpochMs(dateInt);
        boolean isEpoch = false;
        if (epochMs != dateInt) {
            isEpoch = true;
        }
        long result = truncateTimestamp(session, unit, epochMs);
        if (isEpoch) {
            result = result / 1000L;
        }
        return result;
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a dateint where the value is truncated to the specified unit")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDateIntTrunc(ConnectorSession session, @SqlType(VARCHAR) Slice unit, @SqlType(StandardTypes.INTEGER) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return dateIntEpochTrunc(session, unit, dateInt);
            }
            return nfDateToDateInt(truncateDate(unit, nfDateInttoDate(session, dateInt)));
        }, null);
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a dateint where the value is truncated to the specified unit")
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long nfDateBigIntTrunc(ConnectorSession session, @SqlType(VARCHAR) Slice unit, @SqlType(StandardTypes.BIGINT) long dateInt)
    {
        return handleExceptions(() -> {
            if (dateInt > DATE_INT_MAX_THRESHOLD) {
                return dateIntEpochTrunc(session, unit, dateInt);
            }
            return nfDateToDateInt(truncateDate(unit, nfDateInttoDate(session, dateInt)));
        }, null);
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a datestr where the value is truncated to the specified unit")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfDateStrTrunc(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                      @SqlType(StandardTypes.VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8) {
                return utf8Slice(String.valueOf(nfDateIntTrunc(session, unit, nfDateInt(session, dateStr))));
            }
            else if (dateStr.length() == 10) {
                return nfDateToDateStr(truncateDate(unit, nfDate(session, dateStr)));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return toISO8601FromTimestampWithTimeZone(truncateTimestampWithTimezone(unit, nfFromUnixTimeMsTz(timestamp, utf8Slice(session.getTimeZoneKey().getId()))));
            }
        }, null);
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a date where the value is truncated to the specified unit")
    @SqlType(StandardTypes.DATE)
    @SqlNullable
    public static Long nfDateTrunc(@SqlType(VARCHAR) Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        return handleExceptions(() -> {
            return truncateDate(unit, date);
        }, null);
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a timestamp where the value is truncated to the specified unit")
    @SqlType(StandardTypes.TIMESTAMP)
    @SqlNullable
    public static Long nfTimestampTrunc(ConnectorSession session, @SqlType(VARCHAR) Slice unit,
                                        @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return truncateTimestamp(session, unit, timestamp);
        }, null);
    }

    @ScalarFunction("nf_datetrunc")
    @Description("Returns a timestamp with timezone where the value is truncated to the specified unit")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    @SqlNullable
    public static Long nfTimestampWithTimeZoneTrunc(@SqlType(VARCHAR) Slice unit,
                                        @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return truncateTimestampWithTimezone(unit, timestamp);
        }, null);
    }

    @ScalarFunction("nf_dateformat")
    @Description("Formats the given dateint in the given format and returns a string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfDateIntFormat(ConnectorSession session, @SqlType(BIGINT) long dateint,
                                        @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return handleExceptions(() -> {
            return formatDatetime(session, nfDateintToTimestamp(session, dateint), formatString);
        }, null);
    }

    @ScalarFunction("nf_dateformat")
    @Description("Formats the given datestr in the given format and returns a string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfDateStrFormat(ConnectorSession session, @SqlType(VARCHAR) Slice datestr,
                                        @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return handleExceptions(() -> {
            return formatDatetime(session, nfTimestamp(session, datestr), formatString);
        }, null);
    }

    @ScalarFunction("nf_dateformat")
    @Description("Formats the given date in the given format and returns a string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfDateFormat(ConnectorSession session, @SqlType(DATE) long date,
                                        @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return handleExceptions(() -> {
            return formatDatetime(session, nfDateToTimestamp(session, date), formatString);
        }, null);
    }

    @ScalarFunction("nf_dateformat")
    @Description("Formats the given timestamp in the given format and returns a string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfTimestampFormat(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp,
                                     @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return handleExceptions(() -> {
            return formatDatetime(session, timestamp, formatString);
        }, null);
    }

    @ScalarFunction("nf_dateformat")
    @Description("Formats the given timestamp with timezone in the given format and returns a string")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice nfTimestampWithTimeZoneFormat(ConnectorSession session,
                                                      @SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp,
                                                      @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return handleExceptions(() -> {
            return formatDatetimeWithTimeZone(session, timestamp, formatString);
        }, null);
    }

    @ScalarFunction("nf_year")
    @Description("Extracts year as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfYearDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return yearFromTimestamp(session, getEpochMs(dateint));
            }
            return yearFromDate(nfDateInttoDate(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_year")
    @Description("Extracts year as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfYearDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return yearFromDate(nfDate(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return yearFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_year")
    @Description("Extracts year as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfYearDate(@SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return yearFromDate(date);
        }, null);
    }

    @ScalarFunction("nf_year")
    @Description("Extracts year as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfYearTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return yearFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_year")
    @Description("Extracts year as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfYearTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return yearFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_month")
    @Description("Extracts month as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMonthDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return monthFromTimestamp(session, getEpochMs(dateint));
            }
            return monthFromDate(nfDateInttoDate(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_month")
    @Description("Extracts month as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMonthDateSTr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return monthFromDate(nfDate(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return monthFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_month")
    @Description("Extracts month as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMonthDate(@SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return monthFromDate(date);
        }, null);
    }

    @ScalarFunction("nf_month")
    @Description("Extracts month as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMonthTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return monthFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_month")
    @Description("Extracts month as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMonthTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return monthFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_day")
    @Description("Extracts day as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDayDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return dayFromTimestamp(session, getEpochMs(dateint));
            }
            return dayFromDate(nfDateInttoDate(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_day")
    @Description("Extracts day as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDayDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return dayFromDate(nfDate(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return dayFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_day")
    @Description("Extracts day as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDatDate(@SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return dayFromDate(date);
        }, null);
    }

    @ScalarFunction("nf_day")
    @Description("Extracts day as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDayTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return dayFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_day")
    @Description("Extracts day as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfDayTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return dayFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_quarter")
    @Description("Extracts quarter as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfQuarterDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                dayFromTimestamp(session, getEpochMs(dateint));
            }
            return quarterFromDate(nfDateInttoDate(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_quarter")
    @Description("Extracts quarter as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfQuarterDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return quarterFromDate(nfDate(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return quarterFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_quarter")
    @Description("Extracts quarter as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfQuarterDate(@SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return quarterFromDate(date);
        }, null);
    }

    @ScalarFunction("nf_quarter")
    @Description("Extracts quarter as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfQuarterTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return quarterFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_quarter")
    @Description("Extracts quarter as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfQuarterTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return quarterFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_week")
    @Description("Extracts week as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfWeekDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return weekFromTimestamp(session, getEpochMs(dateint));
            }
            return weekFromDate(nfDateInttoDate(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_week")
    @Description("Extracts week as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfWeekDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return weekFromDate(nfDate(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return weekFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_week")
    @Description("Extracts week as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfWeekDate(@SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return weekFromDate(date);
        }, null);
    }

    @ScalarFunction("nf_week")
    @Description("Extracts week as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfWeekTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return weekFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_week")
    @Description("Extracts week as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfWeekTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return weekFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_hour")
    @Description("Extracts hour as an integer from dateint. Will return 0")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfHourDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return hourFromTimestamp(session, getEpochMs(dateint));
            }
            return hourFromTimestamp(session, nfDateintToTimestamp(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_hour")
    @Description("Extracts hour as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfHourDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return hourFromTimestamp(session, nfTimestamp(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return hourFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_hour")
    @Description("Extracts hour as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfHourDate(ConnectorSession session, @SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return hourFromTimestamp(session, nfDateToTimestamp(session, date));
        }, null);
    }

    @ScalarFunction("nf_hour")
    @Description("Extracts hour as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfHourTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return hourFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_hour")
    @Description("Extracts hour as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfHourTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return hourFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_minute")
    @Description("Extracts minute as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMinuteDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return minuteFromTimestamp(session, getEpochMs(dateint));
            }

            return minuteFromTimestamp(session, nfDateintToTimestamp(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_minute")
    @Description("Extracts minute as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMinuteDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return minuteFromTimestamp(session, nfTimestamp(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return minuteFromTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_minute")
    @Description("Extracts minute as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMinuteDate(ConnectorSession session, @SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return minuteFromTimestamp(session, nfDateToTimestamp(session, date));
        }, null);
    }

    @ScalarFunction("nf_minute")
    @Description("Extracts minute as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMinuteTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return minuteFromTimestamp(session, timestamp);
        }, null);
    }

    @ScalarFunction("nf_minute")
    @Description("Extracts minute as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMinuteTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return minuteFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_second")
    @Description("Extracts second as an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfSecondDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            if (dateint > DATE_INT_MAX_THRESHOLD) {
                return secondFromTimestamp(getEpochMs(dateint));
            }
            return secondFromTimestamp(nfDateintToTimestamp(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_second")
    @Description("Extracts second as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfSecondDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return secondFromTimestamp(nfTimestamp(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return secondFromTimestamp(timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_second")
    @Description("Extracts second as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfSecondDate(ConnectorSession session, @SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return secondFromTimestamp(nfDateToTimestamp(session, date));
        }, null);
    }

    @ScalarFunction("nf_second")
    @Description("Extracts second as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfSecondTimestamp(@SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return secondFromTimestamp(timestamp);
        }, null);
    }

    @ScalarFunction("nf_second")
    @Description("Extracts second as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfSecondTimestampWithTimezone(@SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return secondFromTimestampWithTimeZone(timestamp);
        }, null);
    }

    @ScalarFunction("nf_millisecond")
    @Description("Extracts millisecond  an integer from dateint")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMilliSecondDateInt(ConnectorSession session, @SqlType(BIGINT) long dateint)
    {
        return handleExceptions(() -> {
            return nfMilliSecondTimestamp(session, nfDateintToTimestamp(session, dateint));
        }, null);
    }

    @ScalarFunction("nf_millisecond")
    @Description("Extracts millisecond as an integer from datestr")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMilliSecondDateStr(ConnectorSession session, @SqlType(VARCHAR) Slice dateStr)
    {
        return handleExceptions(() -> {
            if (dateStr.length() == 8 || dateStr.length() == 10) {
                return nfMilliSecondTimestamp(session, nfTimestamp(session, dateStr));
            }
            else {
                long timestamp = stringToTimestamp(dateStr.toStringUtf8(), TimeZone.getTimeZone(session.getTimeZoneKey().getId())) / 1000L;
                return nfMilliSecondTimestamp(session, timestamp);
            }
        }, null);
    }

    @ScalarFunction("nf_millisecond")
    @Description("Extracts millisecond as an integer from date")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMilliSecondDate(ConnectorSession session, @SqlType(DATE) long date)
    {
        return handleExceptions(() -> {
            return nfMilliSecondTimestamp(session, nfDateToTimestamp(session, date));
        }, null);
    }

    @ScalarFunction("nf_millisecond")
    @Description("Extracts millisecond as an integer from timestamp")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMilliSecondTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long timestamp)
    {
        return handleExceptions(() -> {
            return Long.valueOf(getChronology(session.getTimeZoneKey()).millisOfSecond().get(timestamp));
        }, null);
    }

    @ScalarFunction("nf_millisecond")
    @Description("Extracts millisecond as an integer from timestamp with timezone")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long nfMilliSecondTimestampWithTimezone(ConnectorSession session, @SqlType(TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return handleExceptions(() -> {
            return Long.valueOf(getChronology(session.getTimeZoneKey()).millisOfSecond().get(unpackMillisUtc(timestamp)));
        }, null);
    }
}
