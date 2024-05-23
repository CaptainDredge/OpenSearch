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

public class SplitShardMetadata extends AbstractDiffable<SplitShardMetadata>  implements Comparable<SplitShardMetadata> {

    private final int seedShardId;
    private final int shardId;

    private final int min;
    private final int max;

    private final TreeSet<SplitShardMetadata> ephemeralChildShardMetadata = new TreeSet<>();

    List<Integer>childShardIds = new ArrayList<>();

    public SplitShardMetadata(int shardId, int seedShardId, int min, int max) {
        this.shardId = shardId;
        this.seedShardId = seedShardId;
        this.min = min;
        this.max = max;
    }

    public SplitShardMetadata(int shardId, int seedShardId, int min) {
        this.shardId = shardId;
        this.seedShardId = seedShardId;
        this.min = min;
        this.max = Integer.MAX_VALUE;
    }
    public SplitShardMetadata(int shardId, int seedShardId) {
        this(shardId, seedShardId, 0);
    }

    public SplitShardMetadata(StreamInput in) throws IOException {
        shardId = in.readVInt();
        seedShardId = in.readVInt();
        min = in.readVInt();
        max = in.readVInt();
        int numChildRanges = in.readVInt();
        for (int i = 0; i < numChildRanges; i++) {
            ephemeralChildShardMetadata.add(new SplitShardMetadata(in));
        }
    }

    public int getShardId() {
        return shardId;
    }

    public int getSeedShardId() {
        return seedShardId;
    }

    public TreeSet<SplitShardMetadata> getEphemeralChildShardMetadata() {
        return ephemeralChildShardMetadata;
    }

    public List<Integer> getChildShardIds() {
        return childShardIds;
    }

    private void addChild(SplitShardMetadata range) {
        ephemeralChildShardMetadata.add(range);
        childShardIds.add(range.getShardId());
    }

    public SplitShardMetadata findChildShardRange(int hash) {
        return ephemeralChildShardMetadata.floor(new SplitShardMetadata(shardId, shardId, hash));
    }
    public boolean contains(int hash) {
        return hash >= min && hash <= max;
    }

    @Override
    public int compareTo(SplitShardMetadata o) {
        return Integer.compare(min, o.min);
    }

    public void splitRange(int numOfSplits, int maxShardId) {
        int rangeSize = (max - min) / numOfSplits;

        if(rangeSize <= 1000) {
            throw new IllegalArgumentException("Cannot split further");
        }
        int start = min;
        for (int i = 0; i < numOfSplits; ++i) {
            int end = i == numOfSplits - 1 ? max : start + rangeSize;
            SplitShardMetadata child = new SplitShardMetadata(++maxShardId, seedShardId, start, end);
            addChild(child);
            start = end + 1;
        }
    }

    public static Diff<SplitShardMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardMetadata::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeVInt(seedShardId);
        out.writeVInt(min);
        out.writeVInt(max);
        out.writeVInt(ephemeralChildShardMetadata.size());
        for (SplitShardMetadata range : ephemeralChildShardMetadata) {
            range.writeTo(out);
        }
    }

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.startObject(Integer.toString(shardId));
        builder.field("shard_id", shardId);
        builder.field("primary_shard_id", seedShardId);
        builder.field("min", min);
        builder.field("max", max);
        builder.startObject("child_shard_metadata");
        for (SplitShardMetadata range : ephemeralChildShardMetadata) {
            range.toXContent(builder);
        }
        builder.endObject();
        builder.endObject();
    }

    public static SplitShardMetadata parse(XContentParser parser) throws IOException {
        XContentParser.Token token;
        int shardId = -1;
        int primaryShardId = -1;
        int min = -1;
        int max = -1;
        String fieldName = null;
        List<SplitShardMetadata> childRanges = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            }
            else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("shard_id".equals(fieldName)) {
                    shardId = parser.intValue();
                } else if ("primary_shard_id".equals(fieldName)) {
                    primaryShardId = parser.intValue();
                } else if ("min".equals(fieldName)) {
                    min = parser.intValue();
                } else if ("max".equals(fieldName)) {
                    max = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("child_shard_metadata".equals(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        childRanges.add(parse(parser));
                    }
                }
            }
        }
        SplitShardMetadata range = new SplitShardMetadata(shardId, primaryShardId, min, max);
        for (SplitShardMetadata child : childRanges) {
            range.addChild(child);
        }
        return range;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardMetadata)) return false;
        SplitShardMetadata that = (SplitShardMetadata) o;

        return shardId == that.shardId &&
            seedShardId == that.seedShardId &&
            min == that.min &&
            max == that.max &&
            ephemeralChildShardMetadata.equals(that.ephemeralChildShardMetadata);
    }

    @Override
    public int hashCode() {
        int result = shardId;
        result = 31 * result + seedShardId;
        result = 31 * result + min;
        result = 31 * result + max;
        result = 31 * result + ephemeralChildShardMetadata.hashCode();
        return result;
    }
}
