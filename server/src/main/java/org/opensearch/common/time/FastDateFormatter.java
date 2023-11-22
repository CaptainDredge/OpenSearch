/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.text.ParsePosition;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

public class FastDateFormatter extends CustomDateTimeFormatter {

    private ZoneId zone;

    public FastDateFormatter(String pattern) {
        super(pattern);
    }

    public FastDateFormatter(String pattern, ZoneId zone) {
        super(pattern);
        this.zone = zone;
    }

    public FastDateFormatter(DateTimeFormatter formatter) {
        super(formatter);
    }

    public FastDateFormatter(DateTimeFormatter formatter, ZoneId zone) {
        super(formatter);
        this.zone = zone;
    }

    public static FastDateFormatter ofPattern(String pattern) {
        return new FastDateFormatter(pattern);
    }

    @Override
    public TemporalAccessor parse(String date) {
        int len = date.length();
        char[] tmp = date.toCharArray();
        OffsetDateTime parsed;
        if (len > 19
            && len < 31
            && tmp[len - 1] == 'Z'
            && tmp[4] == '-'
            && tmp[7] == '-'
            && (tmp[10] == 'T' || tmp[10] == 't' || tmp[10] == ' ')
            && tmp[13] == ':'
            && tmp[16] == ':'
            && DateUtils.allDigits(tmp, 20, len - 1)) {
            final int year = DateUtils.NumberConverter.read4(tmp, 0);
            final int month = DateUtils.NumberConverter.read2(tmp, 5);
            final int day = DateUtils.NumberConverter.read2(tmp, 8);
            final int hour = DateUtils.NumberConverter.read2(tmp, 11);
            final int min = DateUtils.NumberConverter.read2(tmp, 14);
            final int sec = DateUtils.NumberConverter.read2(tmp, 17);
            if (tmp[19] == '.') {
                final int nanos = DateUtils.readNanos(tmp, len - 1, 20);
                parsed = OffsetDateTime.of(year, month, day, hour, min, sec, nanos, ZoneOffset.UTC);
            }
            parsed = OffsetDateTime.of(year, month, day, hour, min, sec, 0, ZoneOffset.UTC);

            return (zone == null ? parsed : parsed.atZoneSameInstant(zone));
        } else if (len > 22
            && len < 36
            && tmp[len - 3] == ':'
            && (tmp[len - 6] == '+' || tmp[len - 6] == '-')
            && tmp[4] == '-'
            && tmp[7] == '-'
            && (tmp[10] == 'T' || tmp[10] == 't' || tmp[10] == ' ')
            && tmp[13] == ':'
            && tmp[16] == ':'
            && DateUtils.allDigits(tmp, len - 2, len)
            && DateUtils.allDigits(tmp, len - 5, len - 3)) {
                final int year = DateUtils.NumberConverter.read4(tmp, 0);
                final int month = DateUtils.NumberConverter.read2(tmp, 5);
                final int day = DateUtils.NumberConverter.read2(tmp, 8);
                final int hour = DateUtils.NumberConverter.read2(tmp, 11);
                final int min = DateUtils.NumberConverter.read2(tmp, 14);
                final int sec = DateUtils.NumberConverter.read2(tmp, 17);
                final int offHour = DateUtils.NumberConverter.read2(tmp, len - 5);
                final int offMin = DateUtils.NumberConverter.read2(tmp, len - 2);
                final boolean isPositiveOffset = tmp[len - 6] == '+';
                final ZoneOffset offset = ZoneOffset.ofHoursMinutes(
                    isPositiveOffset ? offHour : -offHour,
                    isPositiveOffset ? offMin : -offMin
                );
                if (tmp[19] == '.') {
                    final int nanos = DateUtils.readNanos(tmp, len - 6, 20);
                    OffsetDateTime dt;
                    return OffsetDateTime.of(year, month, day, hour, min, sec, nanos, offset);
                }
                return OffsetDateTime.of(year, month, day, hour, min, sec, 0, offset);
            } else {
                return getFormatter().parse(date, new ParsePosition(0));
            }
    }

    @Override
    public CustomDateTimeFormatter withZone(ZoneId zoneId) {
        return new FastDateFormatter(getFormatter().withZone(zoneId), zoneId);
    }

    @Override
    public CustomDateTimeFormatter withLocale(Locale locale) {
        return new FastDateFormatter(getFormatter().withLocale(locale));
    }

    @Override
    public Object parseObject(String text, ParsePosition pos) {
        return super.parseObject(text, pos);
    }
}
