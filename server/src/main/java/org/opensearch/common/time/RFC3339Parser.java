/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Locale;

public final class RFC3339Parser extends CustomDateTimeFormatter {
    public static final char DATE_SEPARATOR = '-';
    public static final char TIME_SEPARATOR = ':';
    public static final char SEPARATOR_UPPER = 'T';
    private static final char PLUS = '+';
    private static final char MINUS = '-';
    private static final char SEPARATOR_LOWER = 't';
    private static final char SEPARATOR_SPACE = ' ';
    private static final char FRACTION_SEPARATOR = '.';
    private static final char ZULU_UPPER = 'Z';
    private static final char ZULU_LOWER = 'z';

    private ZoneId zone;

    public RFC3339Parser(String pattern) {
        super(pattern);
    }

    public RFC3339Parser(String pattern, ZoneId zone) {
        super(pattern);
        this.zone = zone;
    }

    public RFC3339Parser(DateTimeFormatter formatter) {
        super(formatter);
    }

    public RFC3339Parser(DateTimeFormatter formatter, ZoneId zone) {
        super(formatter);
        this.zone = zone;
    }

    public static RFC3339Parser ofPattern(String pattern) {
        return new RFC3339Parser(pattern);
    }

    @Override
    public CustomDateTimeFormatter withZone(ZoneId zoneId) {
        return new RFC3339Parser(getFormatter().withZone(zoneId), zoneId);
    }

    @Override
    public CustomDateTimeFormatter withLocale(Locale locale) {
        return new RFC3339Parser(getFormatter().withLocale(locale));
    }

    @Override
    public Object parseObject(String text, ParsePosition pos) {
        return parse(text, pos);
    }

    @Override
    public TemporalAccessor parse(final String dateTime) {
        OffsetDateTime parsedDatetime = parse(dateTime, new ParsePosition(0)).toOffsetDatetime();
        return zone == null ? parsedDatetime : parsedDatetime.atZoneSameInstant(zone);
    }

    public DateTime parse(String date, ParsePosition pos) {
        if (date == null) {
            throw new NullPointerException("date cannot be null");
        }

        final int len = date.length();
        final char[] chars = date.toCharArray();

        // Date portion

        // YEAR
        final int years = getYear(chars, pos);
        if (4 == len) {
            return DateTime.ofYear(years);
        }

        // MONTH
        consumeChar(chars, pos, DATE_SEPARATOR);
        final int months = getMonth(chars, pos);
        if (7 == len) {
            return DateTime.ofYearMonth(years, months);
        }

        // DAY
        consumeChar(chars, pos, DATE_SEPARATOR);
        final int days = getDay(chars, pos);
        if (10 == len) {
            return DateTime.ofDate(years, months, days);
        }

        // HOURS
        consumeChar(chars, pos, SEPARATOR_UPPER, SEPARATOR_LOWER, SEPARATOR_SPACE);
        final int hours = getHour(chars, pos);

        // MINUTES
        consumeChar(chars, pos, TIME_SEPARATOR);
        final int minutes = getMinute(chars, pos);
        if (16 == len) {
            return DateTime.of(years, months, days, hours, minutes, null);
        }

        // SECONDS or TIMEZONE
        return handleTime(chars, pos, years, months, days, hours, minutes);
    }

    private static boolean isDigit(char c) {
        return (c >= '0' && c <= '9');
    }

    private static int digit(char c) {
        return c - '0';
    }

    private static int readInt(final char[] strNum, ParsePosition pos, int n) {
        int start = pos.getIndex(), end = start + n;
        if (end > strNum.length) {
            pos.setErrorIndex(end);
            throw new DateTimeException("Unexpected end of expression at position " + strNum.length + ": '" + new String(strNum) + "'");
        }

        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = strNum[i];
            if (isDigit(c) == false) {
                pos.setErrorIndex(i);
                throw new DateTimeException("Character " + c + " is not a digit");
            }
            int digit = digit(c);
            result = result * 10 + digit;
        }
        pos.setIndex(end);
        return result;
    }

    private static int readIntUnchecked(final char[] strNum, ParsePosition pos, int n) {
        int start = pos.getIndex(), end = start + n;
        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = strNum[i];
            int digit = digit(c);
            result = result * 10 + digit;
        }
        pos.setIndex(end);
        return result;
    }

    private static int getHour(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getMinute(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getDay(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static boolean isValidOffset(char[] chars, int offset) {
        if (offset >= chars.length) {
            return false;
        }
        return true;
    }

    private static void consumeChar(char[] chars, ParsePosition pos, char expected) {
        int offset = pos.getIndex();
        if (isValidOffset(chars, offset) == false) {
            raiseDateTimeException(chars, "Unexpected end of input");
        }

        if (chars[offset] != expected) {
            throw new DateTimeException("Expected character " + expected + " at position " + (offset + 1) + " '" + new String(chars) + "'");
        }
        pos.setIndex(offset + 1);
    }

    private static void consumeNextChar(char[] chars, ParsePosition pos) {
        int offset = pos.getIndex();
        if (isValidOffset(chars, offset) == false) {
            raiseDateTimeException(chars, "Unexpected end of input");
        }
        pos.setIndex(offset + 1);
    }

    private static boolean checkPositionContains(char[] chars, ParsePosition pos, char... expected) {
        int offset = pos.getIndex();
        if (offset >= chars.length) {
            raiseDateTimeException(chars, "Unexpected end of input");
        }

        boolean found = false;
        for (char e : expected) {
            if (chars[offset] == e) {
                found = true;
                break;
            }
        }
        return found;
    }

    private static void consumeChar(char[] chars, ParsePosition pos, char... expected) {
        int offset = pos.getIndex();
        if (offset >= chars.length) {
            raiseDateTimeException(chars, "Unexpected end of input");
        }

        boolean found = false;
        for (char e : expected) {
            if (chars[offset] == e) {
                found = true;
                pos.setIndex(offset + 1);
                break;
            }
        }
        if (!found) {
            throw new DateTimeException(
                "Expected character " + Arrays.toString(expected) + " at position " + (offset + 1) + " '" + new String(chars) + "'"
            );
        }
    }

    private static void assertNoMoreChars(char[] chars, ParsePosition pos) {
        if (chars.length > pos.getIndex()) {
            throw new DateTimeException("Trailing junk data after position " + (pos.getIndex() + 1));
        }
    }

    private static ZoneOffset parseTimezone(char[] chars, ParsePosition pos) {
        int offset = pos.getIndex();
        final int left = chars.length - offset;
        if (checkPositionContains(chars, pos, ZULU_LOWER, ZULU_UPPER)) {
            consumeNextChar(chars, pos);
            assertNoMoreChars(chars, pos);
            return ZoneOffset.UTC;
        }

        if (left != 6) {
            throw new DateTimeException("Invalid timezone offset: " + new String(chars, offset, left));
        }

        final char sign = chars[offset];
        consumeNextChar(chars, pos);
        int hours = getHour(chars, pos);
        int minutes = getMinute(chars, pos);
        if (sign == MINUS) {
            if (hours == 0 && minutes == 0) {
                throw new DateTimeException("Unknown 'Local Offset Convention' date-time not allowed");
            }
            hours = -hours;
            minutes = -minutes;
        } else if (sign != PLUS) {
            throw new DateTimeException("Invalid character starting at position " + offset + 1);
        }

        return ZoneOffset.ofHoursMinutes(hours, minutes);
    }

    private static DateTime handleTime(char[] chars, ParsePosition pos, int year, int month, int day, int hour, int minute) {
        switch (chars[pos.getIndex()]) {
            case TIME_SEPARATOR:
                consumeChar(chars, pos, TIME_SEPARATOR);
                return handleSeconds(year, month, day, hour, minute, chars, pos);

            case PLUS:
            case MINUS:
            case ZULU_UPPER:
            case ZULU_LOWER:
                final ZoneOffset zoneOffset = parseTimezone(chars, pos);
                return DateTime.of(year, month, day, hour, minute, zoneOffset);
        }
        throw new DateTimeException("Unexpected character " + " at position " + pos.getIndex());
    }

    private static int getMonth(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getYear(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 4);
    }

    private static int getSeconds(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getFractions(final char[] chars, final ParsePosition pos, final int len) {
        final int fractions;
        fractions = readIntUnchecked(chars, pos, len);
        switch (len) {
            case 0:
                throw new DateTimeException("Must have at least 1 fraction digit");
            case 1:
                return fractions * 100_000_000;
            case 2:
                return fractions * 10_000_000;
            case 3:
                return fractions * 1_000_000;
            case 4:
                return fractions * 100_000;
            case 5:
                return fractions * 10_000;
            case 6:
                return fractions * 1_000;
            case 7:
                return fractions * 100;
            case 8:
                return fractions * 10;
            default:
                return fractions;
        }
    }

    public static int indexOfNonDigit(final char[] text, int offset) {
        for (int i = offset; i < text.length; i++) {
            if (isDigit(text[i]) == false) {
                return i;
            }
        }
        return -1;
    }

    public static void consumeDigits(final char[] text, ParsePosition pos) {
        final int idx = indexOfNonDigit(text, pos.getIndex());
        if (idx == -1) {
            pos.setErrorIndex(text.length);
        } else {
            pos.setIndex(idx);
        }
    }

    private static DateTime handleSeconds(int year, int month, int day, int hour, int minute, char[] chars, ParsePosition pos) {
        // From here the specification is more lenient
        final int seconds = getSeconds(chars, pos);
        int currPos = pos.getIndex();
        final int remaining = chars.length - currPos;
        if (remaining == 0) {
            return DateTime.of(year, month, day, hour, minute, seconds, 0, null, 0);
        }

        ZoneOffset offset = null;
        int fractions = 0;
        int fractionDigits = 0;
        if (remaining == 1 && checkPositionContains(chars, pos, ZULU_LOWER, ZULU_UPPER)) {
            consumeNextChar(chars, pos);
            // Do nothing we are done
            offset = ZoneOffset.UTC;
            assertNoMoreChars(chars, pos);
        } else if (remaining >= 1 && checkPositionContains(chars, pos, FRACTION_SEPARATOR)) {
            // We have fractional seconds;
            consumeNextChar(chars, pos);
            ParsePosition initPosition = new ParsePosition(pos.getIndex());
            consumeDigits(chars, pos);
            if (pos.getErrorIndex() == -1) {
                // We have an end of fractions
                final int len = pos.getIndex() - initPosition.getIndex();
                fractions = getFractions(chars, initPosition, len);
                fractionDigits = len;
                offset = parseTimezone(chars, pos);
            } else {
                raiseDateTimeException(chars, "No timezone information");
            }
        } else if (remaining >= 1 && checkPositionContains(chars, pos, PLUS, MINUS)) {
            // No fractional sections
            offset = parseTimezone(chars, pos);
        } else {
            raiseDateTimeException(chars, "Unexpected character at position " + (pos.getIndex()));
        }

        return fractionDigits > 0
            ? DateTime.of(year, month, day, hour, minute, seconds, fractions, offset, fractionDigits)
            : DateTime.of(year, month, day, hour, minute, seconds, offset);
    }

    private static void raiseDateTimeException(char[] chars, String message) {
        throw new DateTimeException(message + ": " + new String(chars));
    }
}
