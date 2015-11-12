/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.rpc.client.sender;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.BucketHashFunction;
import io.datakernel.rpc.hash.HashFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class RpcStrategyRendezvousHashing implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
	private static final BucketHashFunction DEFAULT_BUCKET_HASH_FUNCTION = new DefaultBucketHashFunction();

	private final Map<Object, RpcSingleSenderStrategy> keyToStrategy;
	private final HashFunction<Object> hashFunction;
	private int minSubStrategiesForCreation;
	private BucketHashFunction bucketHashFunction;
	private int bucketCapacity;

	public RpcStrategyRendezvousHashing(HashFunction<Object> hashFunction) {
		this.hashFunction = checkNotNull(hashFunction);
		this.keyToStrategy = new HashMap<>();
		this.bucketHashFunction = DEFAULT_BUCKET_HASH_FUNCTION;
		this.minSubStrategiesForCreation = MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT;
		this.bucketCapacity = DEFAULT_BUCKET_CAPACITY;
	}

	public RpcStrategyRendezvousHashing withMinActiveSubStrategies(int minSubStrategiesForCreation) {
		checkArgument(minSubStrategiesForCreation > 0, "minSubStrategiesForCreation must be greater than 0");
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
		return this;
	}

	public RpcStrategyRendezvousHashing withBucketHashFunction(BucketHashFunction bucketHashFunction) {
		this.bucketHashFunction = checkNotNull(bucketHashFunction);
		return this;
	}

	public RpcStrategyRendezvousHashing withBucketCapacity(int capacity) {
		this.bucketCapacity = capacity;
		return this;
	}

	public RpcStrategyRendezvousHashing put(Object key, RpcSingleSenderStrategy strategy) {
		checkNotNull(strategy);
		keyToStrategy.put(key, strategy);
		return this;
	}

	@Override
	public List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
		Map<Object, Optional<RpcRequestSender>> keyToSender = createKeyToSender(pool, keyToStrategy);
		if (countPresentValues(keyToSender) >= minSubStrategiesForCreation) {
			return Optional.<RpcRequestSender>of(
					new RequestSenderRendezvousHashing(keyToSender, hashFunction, bucketHashFunction, bucketCapacity));
		} else {
			return Optional.absent();
		}
	}

	private static Map<Object, Optional<RpcRequestSender>> createKeyToSender(RpcClientConnectionPool pool,
	                                                                         Map<Object, RpcSingleSenderStrategy> keyToStrategy) {

		assert keyToStrategy != null;

		Map<Object, Optional<RpcRequestSender>> keyToSender = new HashMap<>();
		for (Object key : keyToStrategy.keySet()) {
			RpcSingleSenderStrategy strategy = keyToStrategy.get(key);
			Optional<RpcRequestSender> sender = strategy.create(pool);
			keyToSender.put(key, sender);
		}
		return keyToSender;
	}

	private static <T> int countPresentValues(Map<Object, Optional<T>> keyToSender) {
		int counter = 0;
		for (Optional<T> value : keyToSender.values()) {
			if (value.isPresent()) {
				++counter;
			}
		}
		return counter;
	}

	@VisibleForTesting
	final static class RequestSenderRendezvousHashing implements RpcRequestSender {

		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashFunction<Object> hashFunction;
		final RendezvousHashBucket hashBucket;

		public RequestSenderRendezvousHashing(Map<Object, Optional<RpcRequestSender>> keyToSender,
		                                      HashFunction<Object> hashFunction,
		                                      BucketHashFunction bucketHashFunction, int bucketCapacity) {
			checkNotNull(keyToSender);
			this.hashFunction = checkNotNull(hashFunction);
			this.hashBucket = RendezvousHashBucket.create(keyToSender, bucketHashFunction, bucketCapacity);
		}

		@Override
		public <T> void sendRequest(Object request, int timeout,final ResultCallback<T> callback) {
			RpcRequestSender sender = getRequestSender(request);
			if (sender == null) {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
				return;
			}
			sender.sendRequest(request, timeout, callback);
		}

		private RpcRequestSender getRequestSender(Object request) {
			int hash = hashFunction.hashCode(request);
			return hashBucket.chooseSender(hash);
		}
	}

	@VisibleForTesting
	static final class RendezvousHashBucket {

		private final RpcRequestSender[] sendersBucket;

		private RendezvousHashBucket(RpcRequestSender[] sendersBucket) {
			this.sendersBucket = sendersBucket;
		}

		// if activeAddresses is empty fill bucket with null
		public static RendezvousHashBucket create(Map<Object, Optional<RpcRequestSender>> keyToSender,
		                                          BucketHashFunction bucketHashFunction, int capacity) {
			checkArgument((capacity & (capacity - 1)) == 0, "capacity must be a power-of-two, got %d", capacity);
			checkNotNull(bucketHashFunction);
			RpcRequestSender[] sendersBucket = new RpcRequestSender[capacity];
			for (int n = 0; n < sendersBucket.length; n++) {
				RpcRequestSender chosenSender = null;
				int max = Integer.MIN_VALUE;
				for (Object key : keyToSender.keySet()) {
					Optional<RpcRequestSender> sender = keyToSender.get(key);
					if (sender.isPresent()) {
						int hash = bucketHashFunction.hash(key, n);
						if (hash >= max) {
							chosenSender = sender.get();
							max = hash;
						}
					}
				}
				sendersBucket[n] = chosenSender;
			}
			return new RendezvousHashBucket(sendersBucket);
		}

		public RpcRequestSender chooseSender(int hash) {
			return sendersBucket[hash & (sendersBucket.length - 1)];
		}
	}

	@VisibleForTesting
	static final class DefaultBucketHashFunction implements BucketHashFunction {
		private static final com.google.common.hash.HashFunction murmurHashAddressFunction = Hashing.murmur3_32();

		@Override
		public int hash(Object key, int bucket) {
			return murmurHashAddressFunction.newHasher()
					.putInt(key.hashCode())
					.putInt(bucket)
					.hash().asInt();
		}
	}
}