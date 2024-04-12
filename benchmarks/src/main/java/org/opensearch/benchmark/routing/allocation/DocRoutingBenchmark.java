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
//            if(inMemoryFileStore.containsKey(shardId)) {
//                inMemoryFileStore.get(shardId).add(doc);
//            } else {
//                List<Document> docs = new ArrayList<>();
//                docs.add(doc);
//                inMemoryFileStore.put(shardId, docs);
//            }
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
//            for (int i = 0; i < routingTable.size(); ++i) {
//                for (int j = 0; j < routingTable.get(i).size(); ++j) {
//                    if (routingTable.get(i).get(j) == shardId) {
//                        return metaData.get(i);
//                    }
//                }
//            }

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

    @State(Scope.Thread)
    public static class IndexState {
        IndexWithRecurringRouting index;
        List<Document>docs;

        @Param({"1", "100", "1000", "10000", "100000"})
        public int numDocs;

        @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
        public int depth;

        @Setup(Level.Iteration)
        public void setup() {
            index = new IndexWithRecurringRouting(1);
            recursiveSplit(index, 0, depth);
        }

        @Setup(Level.Invocation)
        public void setupDocs() {
            docs = new ArrayList<>(numDocs);
            for (int i = 0; i < numDocs; ++i) {
                docs.add(new Document(i, "metadata"));
            }
        }

        public static void recursiveSplit(IndexWithRecurringRouting index, int shardId, int numSplits) {
            if(numSplits <= 0) {
                return;
            }
            try {
                index.split(shardId, 2);
            } catch (Exception e) {
                e.printStackTrace();
            }

//            for(int childIds : index.routingTable.get(shardId)) {
//                recursiveSplit(index, childIds, numSplits-1);
//            }
            int[] childIds = index.routingTable[shardId];
            for(int childId : childIds) {
                recursiveSplit(index, childId, numSplits-1);
            }
        }
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 2)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void routeDocs(IndexState state) {
        for(Document doc : state.docs) {
            state.index.routeDoc(doc);
        }
    }
}
