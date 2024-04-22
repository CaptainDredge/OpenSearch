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
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class SplitPartition extends AbstractDiffable<SplitPartition> {

    private final Integer parentShardId;
    private final Set<Integer> childShardIds;

    private final int routingFactor;
    private final int routingShard;

    public SplitPartition(Integer parentShardId, SplitPartition parentPartition, Set<Integer> childShardIds) {
        this.parentShardId = parentShardId;
        this.childShardIds = childShardIds;
        int numChildShards = childShardIds.size();
        this.routingShard = calculateNumRoutingShards(parentPartition.routingFactor, numChildShards);
        this.routingFactor = this.routingShard / numChildShards;
    }

    public SplitPartition(Integer parentShardId, Set<Integer> childShardIds, Integer routingFactor, Integer routingShard) {
        this.parentShardId = parentShardId;
        this.childShardIds = childShardIds;
        this.routingFactor = routingFactor;
        this.routingShard = routingShard;
    }

    public SplitPartition(StreamInput in) throws IOException {
        parentShardId = in.readVInt();
        childShardIds = in.readSet(StreamInput::readVInt);
        routingFactor = in.readVInt();
        routingShard = in.readVInt();
    }

    /**
     * Calculate the number of routing shards for a given parent shard
     * @param parentRoutingFactor
     * @param numChildShards
     * @return the number of routing shards
     * @throws IllegalArgumentException if the number of routing shards is less than 1 i.e. parent shard cannot be split further
     *
     * The number of routing shards is calculated as follows:
     * newRoutingFactor = 2 ^ floor(log2(parentRoutingFactor / numChildShards))
     * numRoutingShards = numChildShards * newRoutingFactor
     */
    public static int calculateNumRoutingShards(int parentRoutingFactor, int numChildShards) {
        int numRoutingShardsPerChildShard = parentRoutingFactor / numChildShards;
        int log2numRoutingShardsPerChildShard = 32 - Integer.numberOfLeadingZeros(numRoutingShardsPerChildShard - 1); // ceil(log2(x)) = 32 - leadingZeros(x - 1)

        int numAllowedSplits = (numRoutingShardsPerChildShard & (numRoutingShardsPerChildShard - 1)) == 0     // floor(log2(x)) = ceil(log2(x)) - 1 if x is a power of 2
                ? log2numRoutingShardsPerChildShard : log2numRoutingShardsPerChildShard - 1;
        if(numAllowedSplits <= 0)
            throw new IllegalArgumentException("Cannot split further");
        return numChildShards * (1 << numAllowedSplits);
    }

    public int getRoutingFactor() {
        return routingFactor;
    }

    public int getRoutingShard() {
        return routingShard;
    }

    public ArrayList<Integer> getChildShards() {
        return new ArrayList<>(childShardIds);
    }

    public Integer getParentShardId() {
        return parentShardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(parentShardId);
        out.writeCollection(childShardIds, StreamOutput::writeVInt);
        out.writeVInt(routingFactor);
        out.writeVInt(routingShard);
    }

    public static Diff<SplitPartition> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitPartition::new, in);
    }

    public int hashCode() {
        int result = childShardIds.hashCode();
        result = 31 * result + parentShardId;
        result = 31 * result + routingFactor;
        result = 31 * result + routingShard;
        return result;
    }

    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(parentShardId.toString());
        builder.startArray("child_shard_ids");
        for (final Integer childShard : childShardIds) {

            builder.value(childShard);
        }
        builder.endArray();
        builder.field("routing_factor", routingFactor);
        builder.field("routing_shard", routingShard);
        builder.endObject();
    }

    public static SplitPartition parse(XContentParser parser, String currentFieldName) throws IOException {
        Integer parentShardId = Integer.parseInt(currentFieldName);
        Set<Integer> childShardIds = new HashSet<>();
        int routingFactor = 0;
        int routingShard = 0;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                if (fieldName.equals("child_shard_ids")) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            childShardIds.add(parser.intValue());
                        }
                    }
                } else if (fieldName.equals("routing_factor")) {
                    routingFactor = parser.intValue();
                } else if (fieldName.equals("routing_shard")) {
                    routingShard = parser.intValue();
                }
            }
        }
        return new SplitPartition(parentShardId, childShardIds, routingFactor, routingShard);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SplitPartition that = (SplitPartition) o;

        if (routingFactor != that.routingFactor) {
            return false;
        }
        if (routingShard != that.routingShard) {
            return false;
        }
        return childShardIds.equals(that.childShardIds);
    }
}
