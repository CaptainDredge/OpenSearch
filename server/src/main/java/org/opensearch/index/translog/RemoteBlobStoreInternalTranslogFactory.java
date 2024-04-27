/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Translog Factory for the remotefs  translog {@link RemoteFsTranslog}
 *
 * @opensearch.internal
 */
public class RemoteBlobStoreInternalTranslogFactory implements TranslogFactory {

    private final Repository repository;

    private final ThreadPool threadPool;

    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private final Function<ShardId, ShardId> splittingParentShardIdSupplier;

    public RemoteBlobStoreInternalTranslogFactory(
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        String repositoryName,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        Function<ShardId, ShardId> splittingParentShardIdSupplier
    ) {
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(repositoryName);
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        this.repository = repository;
        this.threadPool = threadPool;
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.splittingParentShardIdSupplier = splittingParentShardIdSupplier;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier
    ) throws IOException {

        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
        return new RemoteFsTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            blobStoreRepository,
            threadPool,
            startedPrimarySupplier,
            remoteTranslogTransferTracker,
            splittingParentShardIdSupplier
        );
    }

    public Repository getRepository() {
        return repository;
    }
}
