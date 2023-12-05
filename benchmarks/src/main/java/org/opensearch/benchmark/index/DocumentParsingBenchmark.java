/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index;

import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.time.DateFormatters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import com.ethlo.time.ITU;
import org.opensearch.common.time.RFC3339Parser;

@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class DocumentParsingBenchmark {

    @Setup
    public void setup() {
        LogConfigurator.setNodeName("test");
    }

    @Param({ "2023-01-01T23:38:34.000Z", "1970-01-01T00:16:12.675Z", "5050-01-01T12:02:01.123Z" })
    public String dateString;

    @Benchmark
    public void strictDateOptionalTimeFormatter() {
        DateFormatters.STRICT_DATE_OPTIONAL_TIME_FORMATTER.parse(dateString);
    }
    @Benchmark
    public void isoOffsetDateFormatter() throws Exception {
        // STRICT_FORMATTER.parse(nowEpoch);
        DateFormatters.ISO_OFFSET_DATE_FORMATTER.parse(dateString);
    }

    @Benchmark
    public void charDateFormatter() throws Exception {
        DateFormatters.CHAR_DATE_FORMATTER.parse(dateString);
    }

    @Benchmark
    public void benchITUParser() {
        ITU.parseDateTime(dateString);
    }

    @Benchmark
    public void benchRFC3339Parser() {
        DateFormatters.RFC3339_OFFSET_DATE_FORMATTER.parse(dateString);
    }
}
