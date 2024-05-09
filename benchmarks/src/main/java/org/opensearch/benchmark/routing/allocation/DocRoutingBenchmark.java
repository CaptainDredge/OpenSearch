/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.routing.allocation;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.util.Murmur3HashFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.TreeSet;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@SuppressWarnings("unused")
public class DocRoutingBenchmark {


    public static int calculateNumRoutingShards(int numShards) {
        int log2MaxNumShards = 10; // logBase2(1024)
        int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1); // ceil(logBase2(numShards))
        int numSplits = log2MaxNumShards - log2NumShards;
        numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
        return numShards * (1 << numSplits);
    }

    public static int calculateNumRoutingShards(int oldRoutingFactor, int numShards) {
        int log2OrfDivShards = 32 - Integer.numberOfLeadingZeros(oldRoutingFactor / numShards - 1);
        int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1);
        int numSplits = log2OrfDivShards - log2NumShards;
        numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
        return numShards * (1 << numSplits);
    }


    private static class Partition {
        private final int routingFactor;
        private final int routingShard;

        Partition(int oldRoutingFactor, int newNumPrimaryShard) {
            this.routingShard = calculateNumRoutingShards(oldRoutingFactor, newNumPrimaryShard);
            this.routingFactor = this.routingShard / newNumPrimaryShard;
        }
    }

    private static class Document {
        private final int docId;
        private final String metadata;

        Document(int docId, String metadata) {
            this.docId = docId;
            this.metadata = metadata;
        }

        int getDocId() {
            return docId;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Document && ((Document) obj).docId == docId && ((Document) obj).metadata.equals(metadata);
        }

        @Override
        public int hashCode() {
            return docId;
        }
    }

    private static class IndexWithRecurringRouting {
        private final List<Integer> docs = new ArrayList<>(0);
        private final HashMap<Integer, List<Document>> inMemoryFileStore = new HashMap<>();
        private final Partition[] metaData = Collections.nCopies(1024, null).toArray(Partition[]::new);
        private final int[][] routingTable = new int[1024][2];
        private final int numPrimaryShard;
        private final int routingFactor;
        private final int routingShard;

        IndexWithRecurringRouting(int numPrimaryShard) {
            this.numPrimaryShard = numPrimaryShard;
            this.routingShard = calculateNumRoutingShards((int) Math.pow(2, 10) * this.numPrimaryShard + 1, this.numPrimaryShard);
            this.routingFactor = this.routingShard / this.numPrimaryShard;

            for (int i = 0; i < numPrimaryShard; ++i) {
                docs.add(0);
//                routingTable.add(new ArrayList<>(0));
            }

            for(int i=0; i<1024; i++)
                for(int j=0; j<2; j++)
                    routingTable[i][j] = -1; // initialize routing table
        }

        void split(final int shardId, int numShards) throws Exception {
            final int routingFactor;
            int maxShardNum = docs.size() - 1;

            for (int i = 0; i < numShards; ++i) {
                docs.add(0);
                //routingTable.add(new ArrayList<>(0));
                //routingTable.get(shardId).add(++maxShardNum);
                routingTable[shardId][i] = ++maxShardNum;
            }

            if (shardId >= this.numPrimaryShard) {
                Partition p = findParent(shardId);

                routingFactor = p.routingFactor;
            } else {
                routingFactor = this.routingFactor;
            }

            metaData[shardId] = new Partition(routingFactor, numShards);
            //recoverDocs(shardId);  // recover the docs from split shard
        }

        void recoverDocs(final int shardId) {

            // iterate on inMemoryFileStore documents for shardId
            if(inMemoryFileStore.containsKey(shardId)) {
                List<Document> docs = inMemoryFileStore.get(shardId);
                for(Document doc : docs) {
                    routeDoc(doc);
                }
            }
            inMemoryFileStore.remove(shardId);
            docs.set(shardId, 0);
        }
        int route(int routing, boolean murmur) {
            final int hash;

            if (murmur) {
                hash = Murmur3HashFunction.hash("" + routing);
            } else {
                hash = routing;
            }

            int shardId = Math.floorMod(hash, this.routingShard) / this.routingFactor;

            while (isPartition(shardId)) {
                final Partition p = metaData[shardId];
//                shardId = routingTable.get(shardId).get(Math.floorMod(hash, p.routingShard) / p.routingFactor);
                shardId = routingTable[shardId][(Math.floorMod(hash, p.routingShard) / p.routingFactor)];
            }

            docs.set(shardId, docs.get(shardId) + 1);
            return shardId;
        }

        void routeDoc(Document doc) {
            final int shardId = route(doc.getDocId(), true);
        }

        Document getDoc(int docId) {
            final int shardId = route(docId, true);
            assert(inMemoryFileStore.containsKey(shardId));
            return inMemoryFileStore.get(shardId).stream().filter(doc -> doc.getDocId() == docId).findFirst().get();
        }

        private boolean isPartition(int idx) {
//            return !routingTable.get(idx).isEmpty();
            return routingTable[idx][0] != -1;
        }

        private Partition findParent(int shardId) throws Exception {
            int maxShardNum = docs.size() - 1;

            for(int i=0; i<=maxShardNum; i++) {
                for(int j=0; j<2; j++) {
                    if(routingTable[i][j] == shardId) {
                        return metaData[i];
                    }
                }
            }
            throw new Exception("Parent not found!");
        }

        void ingest(int shardId, int numDocs) {
            for (int i = 0; i < numDocs; ++i)
                route(i, true);
        }
    }

    private static class IndexWithRangeRouting {
        private final List<Integer> docs = new ArrayList<>(0);
        private final HashMap<Integer, List<Document>> inMemoryFileStore = new HashMap<>();
        private final Map<Integer, TreeSet<HashRange>> primaryShardRanges = new HashMap<>();
        private final Map<Integer, SplitShardRange> shardRanges = new HashMap<>();
        private final List<List<Integer>> routingTable = new ArrayList<>(0);
        private final int numPrimaryShard;
        private final int routingFactor;
        private final int routingShard;

        IndexWithRangeRouting(int numPrimaryShard) {
            this.numPrimaryShard = numPrimaryShard;
            this.routingShard = calculateNumRoutingShards(this.numPrimaryShard);
            this.routingFactor = this.routingShard / this.numPrimaryShard;
            for(int i = 0; i < numPrimaryShard; ++i) {
                docs.add(0);
                routingTable.add(new ArrayList<>(0));
                this.primaryShardRanges.put(i, new TreeSet<>(List.of(new HashRange(0, i))));
            }
        }

        void Split(final int shardId, int numShards) {
            List<HashRange> splitRanges;
            int primaryShardId;
            if(shardId < numPrimaryShard) {
                primaryShardId = shardId;
                TreeSet<HashRange> ranges = primaryShardRanges.get(shardId);
                splitRanges = ranges.first().splitRange(numShards, docs.size() - 1);
                ranges.clear();
                ranges.addAll(splitRanges);
            } else {
                SplitShardRange splitShardRange = shardRanges.get(shardId);
                primaryShardId = splitShardRange.primaryShardId;
                TreeSet<HashRange> ranges = primaryShardRanges.get(primaryShardId);
                ranges.remove(splitShardRange.range);
                splitRanges = splitShardRange.range.splitRange(numShards, docs.size() - 1);
                ranges.addAll(splitRanges);
            }
            splitRanges.forEach(range -> {
                int newShardId = range.getShardId();
                routingTable.add(new ArrayList<>(0));
                routingTable.get(shardId).add(newShardId);
                docs.add(0);
                shardRanges.put(newShardId, new SplitShardRange(newShardId, primaryShardId, range));
            });
            //recoverDocs(shardId);
        }

        void routeDoc(Document doc, boolean murmur) {
            final int routing = doc.getDocId();
            int shardId = route(routing, murmur);
        }

        private int route(int routing, boolean murmur) {
            final int hash;
            if (murmur) {
                hash = Murmur3HashFunction.hash("" + routing);
            } else {
                hash = routing;
            }

            int shardId = Math.floorMod(hash, this.routingShard) / this.routingFactor;
            TreeSet<HashRange> ranges = primaryShardRanges.get(shardId);
            int rangehash = Math.floorMod(hash, Integer.MAX_VALUE);
            HashRange range = ranges.floor(new HashRange(rangehash, shardId));

            assert (range.contains(rangehash));
            shardId = range.getShardId();
            return shardId;
        }

        void recoverDocs(final int shardId) {

            // iterate on inMemoryFileStore documents for shardId
            if(inMemoryFileStore.containsKey(shardId)) {
                List<Document> docs = inMemoryFileStore.get(shardId);
                for(Document doc : docs) {
                    routeDoc(doc, true);
                }
            }
            inMemoryFileStore.remove(shardId);
            //docs.set(shardId, 0);
        }

        Document getDoc(int docId) {
            final int shardId = route(docId, true);
            assert(inMemoryFileStore.containsKey(shardId));
            return inMemoryFileStore.get(shardId).stream().filter(doc -> doc.getDocId() == docId).findFirst().get();
        }

        public static class SplitShardRange {
            private final int shardId;
            private final int primaryShardId;
            private final HashRange range;

            SplitShardRange(int shardId, int primaryShardId, HashRange range) {
                this.shardId = shardId;
                this.primaryShardId = primaryShardId;
                this.range = range;
            }

            SplitShardRange(int shardId, int primaryShardId) {
                this.shardId = shardId;
                this.primaryShardId = primaryShardId;
                this.range = new HashRange(0, shardId);
            }
        }

        public static class HashRange implements Comparable<HashRange> {
            private final int min;
            private final int max;

            private final int shardId;
            private final int upperBound = Integer.MAX_VALUE;

            HashRange(int min, int max, int shardId) {
                this.min = min;
                this.max = max;
                this.shardId = shardId;
            }

            HashRange(int min, int shardId) {
                this.min = min;
                this.max = upperBound;
                this.shardId = shardId;
            }

            boolean contains(int hash) {
                return hash >= min && hash <= max;
            }

            List<HashRange> splitRange(int numShards, int maxShardId) {
                List<HashRange> ranges = new ArrayList<>(numShards);
                int rangeSize = (max - min) / numShards;

                if(rangeSize <= 0) {
                    throw new IllegalArgumentException("Cannot split further");
                }
                int start = min;
                for (int i = 0; i < numShards; ++i) {
                    int end = i == numShards - 1 ? max : start + rangeSize;
                    ranges.add(new HashRange(start, end, ++maxShardId));
                    start = end + 1;
                }
                return ranges;
            }

            @Override
            public int compareTo(HashRange o) {
                return Integer.compare(min, o.min);
            }

            public int getShardId() {
                return shardId;
            }
        }
    }

    @State(Scope.Thread)
    public static class IndexState {
        IndexWithRecurringRouting index;
        IndexWithRangeRouting rangeIndex;
        List<Document>docs;

        @Param({"1", "100", "1000", "10000", "100000"})
        public int numDocs;

        @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
        public int depth;

        @Setup(Level.Iteration)
        public void setup() {
            index = new IndexWithRecurringRouting(1);
            rangeIndex = new IndexWithRangeRouting(1);
            recursiveSplit(index, rangeIndex, 0, depth);
        }

        @Setup(Level.Invocation)
        public void setupDocs() {
            docs = new ArrayList<>(numDocs);
            for (int i = 0; i < numDocs; ++i) {
                docs.add(new Document(i, "metadata"));
            }
        }

        public static void recursiveSplit(IndexWithRecurringRouting index, IndexWithRangeRouting rangeIndex, int shardId, int numSplits) {
            if(numSplits <= 0) {
                return;
            }
            try {
                index.split(shardId, 2);
                rangeIndex.Split(shardId, 2);
            } catch (Exception e) {
                e.printStackTrace();
            }

            int[] childIds = index.routingTable[shardId];
            for(int childId : childIds) {
                recursiveSplit(index, rangeIndex, childId, numSplits-1);
            }
        }
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void routeDocsRecurring(IndexState state) {
        for(Document doc : state.docs) {
            state.index.routeDoc(doc);
        }
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void routeDocsRange(IndexState state) {
        for(Document doc : state.docs) {
            state.rangeIndex.routeDoc(doc, true);
        }
    }
}
