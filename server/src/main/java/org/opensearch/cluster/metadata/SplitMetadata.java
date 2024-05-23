/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class SplitMetadata implements ToXContentFragment, Writeable {
    static final String SPLIT_SHARD_METADATA_MAP = "split_shard_metadata_map";
    static final String SPLIT_SEED_SHARD_METADATA_MAP = "split_seed_shard_metadata_map";
    private final Map<Integer, SplitShardMetadata> shardIdToRangeMap;
    private final Map<Integer, TreeSet<SplitShardMetadata>> shardRanges;

    public SplitMetadata(SplitMetadata splitMetadata) {
        this.shardIdToRangeMap = new HashMap<>(splitMetadata.shardIdToRangeMap);
        this.shardRanges = new HashMap<>();
        for (Map.Entry<Integer, TreeSet<SplitShardMetadata>> entry : splitMetadata.shardRanges.entrySet()) {
            this.shardRanges.put(entry.getKey(), new TreeSet<>(entry.getValue()));
        }
    }

    public SplitMetadata() {
        this.shardIdToRangeMap = new HashMap<>();
        this.shardRanges = new HashMap<>();
    }

    public SplitMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        this.shardIdToRangeMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            SplitShardMetadata splitShardMetadata = new SplitShardMetadata(in);
            shardIdToRangeMap.put(splitShardMetadata.getShardId(), splitShardMetadata);
        }
        int shardRangesSize = in.readVInt();
        this.shardRanges = new HashMap<>(shardRangesSize);
        for (int i = 0; i < shardRangesSize; i++) {
            int shardId = in.readInt();
            int splitShardMetadataSize = in.readVInt();
            TreeSet<SplitShardMetadata> splitShardMetadata = new TreeSet<>();
            for (int j = 0; j < splitShardMetadataSize; j++) {
                splitShardMetadata.add(new SplitShardMetadata(in));
            }
            shardRanges.put(shardId, splitShardMetadata);
        }
    }

    public SplitShardMetadata getSplitShardMetadata(int shardId) {
        return shardIdToRangeMap.getOrDefault(shardId, new SplitShardMetadata(shardId, shardId));
    }

    public List<Integer> getSplitShardIds() {
        return List.copyOf(shardIdToRangeMap.keySet());
    }
    public void putSplitShardMetadata(SplitShardMetadata splitShardMetadata) {
        shardIdToRangeMap.put(splitShardMetadata.getShardId(), splitShardMetadata);
    }

    public void putSplitSeedShardMetadata(int shardId, TreeSet<SplitShardMetadata> splitShardMetadata) {
        shardRanges.put(shardId, splitShardMetadata);
    }

    public void putSplitSeedShardMetadata(SplitShardMetadata splitShardMetadata) {
        if (shardRanges.containsKey(splitShardMetadata.getSeedShardId())) {
            shardRanges.get(splitShardMetadata.getSeedShardId()).add(splitShardMetadata);
        } else {
            TreeSet<SplitShardMetadata> splitSeedShardMetadata = new TreeSet<>();
            splitSeedShardMetadata.add(splitShardMetadata);
            shardRanges.put(splitShardMetadata.getSeedShardId(), splitSeedShardMetadata);
        }
    }

    public SplitShardMetadata findShardRange(int shardId, int hash) {
        return shardRanges.get(shardId).floor(new SplitShardMetadata(shardId, shardId, hash));
    }
    public boolean isParentShard(Integer shardId) {
        return shardIdToRangeMap.containsKey(shardId) && shardIdToRangeMap.get(shardId).getChildShardIds().size() > 0;
    }

    public boolean isEmpty() {
        return shardIdToRangeMap.isEmpty();
    }

    public void updateSplitMetadataForChildShards(int sourceShardId) {
        SplitShardMetadata sourceShardRange = this.shardIdToRangeMap.get(sourceShardId);
        // Remove parent shard range
        this.shardRanges.get(sourceShardRange.getSeedShardId()).
            remove(sourceShardRange);

        // Add child shard ranges
        for(SplitShardMetadata childRange: sourceShardRange.getEphemeralChildShardMetadata()) {
            this.shardRanges.get(childRange.getSeedShardId()).add(childRange);
        }
        // Clear child shard ranges
        sourceShardRange.getEphemeralChildShardMetadata().clear();
    }

    public void removeSplitShardMetadata(int sourceShardId) {
        SplitShardMetadata splitShardMetadata = this.shardIdToRangeMap.get(sourceShardId);
        if (splitShardMetadata != null) {
            splitShardMetadata.getEphemeralChildShardMetadata().clear();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SplitMetadata that = (SplitMetadata) o;

        if (!shardIdToRangeMap.equals(that.shardIdToRangeMap)) return false;
        return shardRanges.equals(that.shardRanges);
    }

    @Override
    public int hashCode() {
        int result = shardIdToRangeMap.hashCode();
        result = 31 * result + shardRanges.hashCode();
        return result;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardIdToRangeMap.size());
        for(final SplitShardMetadata splitRange : shardIdToRangeMap.values()) {
            splitRange.writeTo(out);
        }
        out.writeVInt(shardRanges.size());
        for (final Map.Entry<Integer, TreeSet<SplitShardMetadata>> cursor : shardRanges.entrySet()) {
            out.writeInt(cursor.getKey());
            out.writeVInt(cursor.getValue().size());
            for (final SplitShardMetadata splitRange : cursor.getValue()) {
                splitRange.writeTo(out);
            }
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SPLIT_SHARD_METADATA_MAP);
        for (final SplitShardMetadata cursor : shardIdToRangeMap.values()) {
            cursor.toXContent(builder);
        }
        builder.endObject();

        builder.startObject(SPLIT_SEED_SHARD_METADATA_MAP);
        for (final TreeSet<SplitShardMetadata> seedShardRanges : shardRanges.values()) {
            for (final SplitShardMetadata cursor : seedShardRanges) {
                cursor.toXContent(builder);
            }
        }
        builder.endObject();
        return builder;
    }

    private void parse(XContentParser parser, String currentFieldName) throws IOException {
        if (SPLIT_SHARD_METADATA_MAP.equals(currentFieldName)) {
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                SplitShardMetadata splitShardMetadata = SplitShardMetadata.parse(parser);
                shardIdToRangeMap.put(splitShardMetadata.getShardId(), splitShardMetadata);
            }
        } else if (SPLIT_SEED_SHARD_METADATA_MAP.equals(currentFieldName)) {
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                SplitShardMetadata splitShardMetadata = SplitShardMetadata.parse(parser);
                if(shardRanges.containsKey(splitShardMetadata.getSeedShardId())) {
                    shardRanges.get(splitShardMetadata.getSeedShardId()).add(splitShardMetadata);
                } else {
                    TreeSet<SplitShardMetadata> splitSeedShardMetadata = new TreeSet<>();
                    splitSeedShardMetadata.add(splitShardMetadata);
                    shardRanges.put(splitShardMetadata.getSeedShardId(), splitSeedShardMetadata);
                }
            }
        }
    }

    public static SplitMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        SplitMetadata splitMetadata = new SplitMetadata();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                splitMetadata.parse(parser, currentFieldName);
            } else {
                throw new IllegalArgumentException("Unexpected token: " + token);
            }
        }
        return splitMetadata;
    }

    public static class SplitMetadataDiff implements Diff<SplitMetadata> {
        public final Diff<Map<Integer, SplitShardMetadata>> shardIdToRangeMap;
        public final Diff<Map<Integer, TreeSet<SplitShardMetadata>>> shardRanges;

        private static final DiffableUtils.DiffableValueReader<Integer, SplitShardMetadata> SPLIT_SHARD_METADATA_DIFFABLE_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(SplitShardMetadata::new, SplitShardMetadata::readDiffFrom);

        public SplitMetadataDiff(SplitMetadata before, SplitMetadata after) {
            this.shardIdToRangeMap = DiffableUtils.diff(before.shardIdToRangeMap, after.shardIdToRangeMap, DiffableUtils.getVIntKeySerializer());
            this.shardRanges = DiffableUtils.diff(before.shardRanges, after.shardRanges, DiffableUtils.getVIntKeySerializer(), SplitShardMetadataSetValueSerializer.getInstance());
        }

        @Override
        public SplitMetadata apply(SplitMetadata part) {
            SplitMetadata splitMetadata = new SplitMetadata();
            splitMetadata.shardIdToRangeMap.putAll(shardIdToRangeMap.apply(part.shardIdToRangeMap));
            splitMetadata.shardRanges.putAll(shardRanges.apply(part.shardRanges));
            return splitMetadata;
        }

        public static class SplitShardMetadataSetValueSerializer<K> extends DiffableUtils.NonDiffableValueSerializer<K, TreeSet<SplitShardMetadata>> {
            private static final SplitShardMetadataSetValueSerializer INSTANCE = new SplitShardMetadataSetValueSerializer();

            public static <K> SplitShardMetadataSetValueSerializer<K> getInstance() {
                return INSTANCE;
            }

            @Override
            public void write(TreeSet<SplitShardMetadata> value, StreamOutput out) throws IOException {
                out.writeVInt(value.size());
                for (SplitShardMetadata range : value) {
                    range.writeTo(out);
                }
            }

            @Override
            public TreeSet<SplitShardMetadata> read(StreamInput in, K key) throws IOException {
                int size = in.readVInt();
                TreeSet<SplitShardMetadata> ranges = new TreeSet<>();
                for (int i = 0; i < size; i++) {
                    ranges.add(new SplitShardMetadata(in));
                }
                return ranges;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardIdToRangeMap.writeTo(out);
            shardRanges.writeTo(out);
        }

        public SplitMetadataDiff(StreamInput in) throws IOException {
            this.shardIdToRangeMap = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getVIntKeySerializer(), SPLIT_SHARD_METADATA_DIFFABLE_VALUE_READER);
            this.shardRanges = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getVIntKeySerializer(), SplitShardMetadataSetValueSerializer.getInstance());
        }
    }
}
