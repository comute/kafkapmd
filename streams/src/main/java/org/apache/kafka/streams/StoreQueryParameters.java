/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.Objects;

/**
 * {@code StoreQueryParameters} allows you to pass a variety of parameters when fetching a store for interactive query.
 */
public class StoreQueryParameters<T> {

    private Integer partition;
    private boolean staleStores;
    private boolean bypassCache;
    private final String storeName;
    private final QueryableStoreType<T> queryableStoreType;

    private StoreQueryParameters(final String storeName, final QueryableStoreType<T> queryableStoreType, final Integer partition, final boolean staleStores, final boolean bypassCache) {
        this.storeName = storeName;
        this.queryableStoreType = queryableStoreType;
        this.partition = partition;
        this.staleStores = staleStores;
        this.bypassCache = bypassCache;
    }

    public static <T> StoreQueryParameters<T> fromNameAndType(final String storeName,
                                                              final QueryableStoreType<T>  queryableStoreType) {
        return new StoreQueryParameters<T>(storeName, queryableStoreType, null, false, false);
    }

    /**
     * Set a specific partition that should be queried exclusively.
     *
     * @param partition   The specific integer partition to be fetched from the stores list by using {@link StoreQueryParameters}.
     *
     * @return StoreQueryParameters a new {@code StoreQueryParameters} instance configured with the specified partition
     */
    public StoreQueryParameters<T> withPartition(final Integer partition) {
        return new StoreQueryParameters<T>(storeName, queryableStoreType, partition, staleStores, bypassCache);
    }

    /**
     * Enable querying of stale state stores, i.e., allow to query active tasks during restore as well as standby tasks.
     *
     * @return StoreQueryParameters a new {@code StoreQueryParameters} instance configured with serving from stale stores enabled
     */
    public StoreQueryParameters<T> enableStaleStores() {
        return new StoreQueryParameters<T>(storeName, queryableStoreType, partition, true, bypassCache);
    }

    public StoreQueryParameters<T> enableBypassCache() {
        return new StoreQueryParameters<>(storeName, queryableStoreType, partition, staleStores, true);
    }

    /**
     * Get the name of the state store that should be queried.
     *
     * @return String state store name
     */
    public String storeName() {
        return storeName;
    }

    /**
     * Get the queryable store type for which key is queried by the user.
     *
     * @return QueryableStoreType type of queryable store
     */
    public QueryableStoreType<T> queryableStoreType() {
        return queryableStoreType;
    }

    /**
     * Get the store partition that will be queried.
     * If the method returns {@code null}, it would mean that no specific partition has been requested,
     * so all the local partitions for the store will be queried.
     *
     * @return Integer partition
     */
    public Integer partition() {
        return partition;
    }

    /**
     * Get the flag staleStores. If {@code true}, include standbys and recovering stores along with running stores.
     *
     * @return boolean staleStores
     */
    public boolean staleStoresEnabled() {
        return staleStores;
    }

    public boolean bypassCacheEnabled() {
        return bypassCache;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StoreQueryParameters<?> that = (StoreQueryParameters<?>) o;
        return staleStores == that.staleStores &&
            bypassCache == that.bypassCache &&
            Objects.equals(partition, that.partition) &&
            Objects.equals(storeName, that.storeName) &&
            Objects.equals(queryableStoreType, that.queryableStoreType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, staleStores, bypassCache, storeName, queryableStoreType);
    }

    @Override
    public String toString() {
        return "StoreQueryParameters{" +
            "partition=" + partition +
            ", staleStores=" + staleStores +
            ", bypassCache=" + bypassCache +
            ", storeName='" + storeName + '\'' +
            ", queryableStoreType=" + queryableStoreType +
            '}';
    }
}