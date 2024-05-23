/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.DiffableUtils;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SplitMetadataTests extends AbstractSerializingTestCase<SplitMetadata>  {
    @Override
    protected SplitMetadata doParseInstance(XContentParser parser) throws IOException {
       try {
            return SplitMetadata.fromXContent(parser);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected Writeable.Reader<SplitMetadata> instanceReader() {
        return SplitMetadata::new;
    }

    @Override
    protected SplitMetadata createTestInstance() {
        SplitMetadata splitMetadata = new SplitMetadata();
        int seedShards = randomIntBetween(3, 10);
        int totalShard = seedShards;
        for (int i = 0; i < seedShards; i++) {
            SplitShardMetadata splitShardMetadata = new SplitShardMetadata(i, i);
            splitMetadata.putSplitShardMetadata(splitShardMetadata);
            splitMetadata.putSplitSeedShardMetadata(splitShardMetadata);
        }
        boolean split = randomBoolean();
        if(split == false)
            return splitMetadata;
        // split on depth 1
        int splitShard = randomIntBetween(0, seedShards - 1);
        int numSplit = randomIntBetween(2, 5);
        SplitShardMetadata splitShardMetadata = splitMetadata.getSplitShardMetadata(splitShard);
        splitShardMetadata.splitRange(numSplit, totalShard - 1);
        for(SplitShardMetadata range: splitShardMetadata.getEphemeralChildShardMetadata()) {
            splitMetadata.putSplitShardMetadata(range);
        }
        totalShard += numSplit;

        // split another shard on depth 1
        splitShard = (splitShard + 1)%seedShards;
        numSplit = randomIntBetween(2, 5);

        splitShardMetadata = splitMetadata.getSplitShardMetadata(splitShard);
        splitShardMetadata.splitRange(numSplit, totalShard-1);
        for(SplitShardMetadata range: splitShardMetadata.getEphemeralChildShardMetadata()) {
            splitMetadata.putSplitShardMetadata(range);
        }
        totalShard += numSplit;
        List<Integer> completedSplits = new ArrayList<>();
        for(int i: splitMetadata.getSplitShardIds()) {
            if(splitMetadata.isParentShard(i)) {
                boolean completeSplit = randomBoolean();
                if(completeSplit) {
                    splitMetadata.updateSplitMetadataForChildShards(i);
                    completedSplits.add(i);
                }
            }
        }

        // split on depth 2
        if(completedSplits.size() > 0) {
            splitShard = completedSplits.get(randomIntBetween(0, completedSplits.size() - 1));
            numSplit = randomIntBetween(2, 5);
            splitShardMetadata = splitMetadata.getSplitShardMetadata(splitShard);
            splitShardMetadata.splitRange(numSplit, totalShard - 1);
            for(SplitShardMetadata range: splitShardMetadata.getEphemeralChildShardMetadata()) {
                splitMetadata.putSplitShardMetadata(range);
            }
        }

        return splitMetadata;
    }

    public void testDiff() throws IOException {
        SplitMetadata splitMetadata = createTestInstance();
        SplitMetadata splitMetadata1 = new SplitMetadata(splitMetadata);
        int splitShard = -1;
        int seedShard = -1;
        int numSplits = 2;
        for(int shardId: splitMetadata.getSplitShardIds()) {
            if(splitMetadata.isParentShard(shardId) == false) {
                SplitShardMetadata splitShardMetadata = splitMetadata.getSplitShardMetadata(shardId);
                splitShardMetadata.splitRange(numSplits, splitMetadata.getSplitShardIds().size() -1 );
                for(SplitShardMetadata range: splitShardMetadata.getEphemeralChildShardMetadata()) {
                    splitMetadata.putSplitShardMetadata(range);
                    splitShard = range.getShardId();
                }
                splitMetadata.updateSplitMetadataForChildShards(shardId);
                seedShard = splitShardMetadata.getSeedShardId();
                break;
            }
        }
        assert splitShard != -1;
        assert seedShard != -1;
        SplitMetadata splitMetadata2 = new SplitMetadata(splitMetadata);

        // diff test
        SplitMetadata.SplitMetadataDiff diff = new SplitMetadata.SplitMetadataDiff(splitMetadata1, splitMetadata2);
        assertEquals(numSplits, ((DiffableUtils.MapDiff) diff.shardIdToRangeMap).getUpserts().size());
        assertEquals(1, ((DiffableUtils.MapDiff) diff.shardRanges).getUpserts().size());
        assertNotNull(((DiffableUtils.MapDiff) diff.shardIdToRangeMap).getUpserts().get(splitShard));
        assertNotNull(((DiffableUtils.MapDiff) diff.shardRanges).getUpserts().get(seedShard));
        assertEquals(splitMetadata2, diff.apply(splitMetadata1));
    }
}
