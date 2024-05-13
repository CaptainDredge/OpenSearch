/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class SplitShardRange extends AbstractDiffable<SplitShardRange>  implements Comparable<SplitShardRange> {

    private final int primaryShardId;
    private final int shardId;

    private final int min;
    private final int max;

    private final TreeSet<SplitShardRange> childRanges = new TreeSet<>();

    List<Integer>childShardIds = new ArrayList<>();

    public SplitShardRange(int shardId, int primaryShardId, int min, int max) {
        this.shardId = shardId;
        this.primaryShardId = primaryShardId;
        this.min = min;
        this.max = max;
    }

    public SplitShardRange(int shardId, int primaryShardId) {
        this(shardId, primaryShardId, 0, Integer.MAX_VALUE);
    }

    public SplitShardRange(StreamInput in) throws IOException {
        shardId = in.readVInt();
        primaryShardId = in.readVInt();
        min = in.readVInt();
        max = in.readVInt();
        int numChildRanges = in.readVInt();
        for (int i = 0; i < numChildRanges; i++) {
            childRanges.add(new SplitShardRange(in));
        }
    }

    public int getShardId() {
        return shardId;
    }

    public int getPrimaryShardId() {
        return primaryShardId;
    }

    public TreeSet<SplitShardRange> getChildRanges() {
        return childRanges;
    }

    public List<Integer> getChildShardIds() {
        return childShardIds;
    }

    private void addChild(SplitShardRange range) {
        childRanges.add(range);
        childShardIds.add(range.getShardId());
    }

    public boolean contains(int hash) {
        return hash >= min && hash <= max;
    }

    @Override
    public int compareTo(SplitShardRange o) {
        return Integer.compare(min, o.min);
    }

    public void splitRange(int numShards, int maxShardId) {
        int rangeSize = (max - min) / numShards;

        if(rangeSize <= 0) {
            throw new IllegalArgumentException("Cannot split further");
        }
        int start = min;
        for (int i = 0; i < numShards; ++i) {
            int end = i == numShards - 1 ? max : start + rangeSize;
            SplitShardRange child = new SplitShardRange(++maxShardId, primaryShardId, start, end);
            addChild(child);
            start = end + 1;
        }
    }

    public static Diff<SplitShardRange> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardRange::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeVInt(primaryShardId);
        out.writeVInt(min);
        out.writeVInt(max);
        out.writeVInt(childRanges.size());
        for (SplitShardRange range : childRanges) {
            range.writeTo(out);
        }
    }

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("shard_id", shardId);
        builder.field("primary_shard_id", primaryShardId);
        builder.field("min", min);
        builder.field("max", max);
        builder.startArray("child_ranges");
        for (SplitShardRange range : childRanges) {
            range.toXContent(builder);
        }
        builder.endArray();
        builder.endObject();
    }

    public static SplitShardRange parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected START_OBJECT token but got " + token);
        }
        int shardId = -1;
        int primaryShardId = -1;
        int min = -1;
        int max = -1;
        List<SplitShardRange> childRanges = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                if ("shard_id".equals(fieldName)) {
                    shardId = parser.intValue();
                } else if ("primary_shard_id".equals(fieldName)) {
                    primaryShardId = parser.intValue();
                } else if ("min".equals(fieldName)) {
                    min = parser.intValue();
                } else if ("max".equals(fieldName)) {
                    max = parser.intValue();
                } else if ("child_ranges".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        childRanges.add(parse(parser));
                    }
                }
            }
        }
        SplitShardRange range = new SplitShardRange(shardId, primaryShardId, min, max);
        for (SplitShardRange child : childRanges) {
            range.addChild(child);
        }
        return range;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardRange)) return false;
        SplitShardRange that = (SplitShardRange) o;

        return shardId == that.shardId &&
            primaryShardId == that.primaryShardId &&
            min == that.min &&
            max == that.max &&
            childRanges.equals(that.childRanges);
    }

    @Override
    public int hashCode() {
        int result = shardId;
        result = 31 * result + primaryShardId;
        result = 31 * result + min;
        result = 31 * result + max;
        result = 31 * result + childRanges.hashCode();
        return result;
    }
}
