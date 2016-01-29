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

package io.datakernel.cube.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypeDouble;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeDate;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.RunnableWithException;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeMetadataStorage;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.examples.LogItem;
import io.datakernel.examples.LogItemSplitter;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpUtils;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogToCubeMetadataStorage;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.stream.StreamProducers;
import org.joda.time.LocalDate;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.cube.api.ReportingDSL.divide;
import static io.datakernel.cube.api.ReportingDSL.percent;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReportingTest {
	private static final Logger logger = LoggerFactory.getLogger(ReportingTest.class);

	private Eventloop eventloop;
	private Eventloop clientEventloop;
	private AsyncHttpServer server;
	private AsyncHttpClient httpClient;
	private CubeHttpClient cubeHttpClient;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = singletonList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	private static final int SERVER_PORT = 50001;
	private static final int TIMEOUT = 1000;

	private static final Map<String, KeyType> DIMENSIONS = ImmutableMap.<String, KeyType>builder()
			.put("date", new KeyTypeDate(LocalDate.now()))
			.put("advertiser", new KeyTypeInt())
			.put("campaign", new KeyTypeInt())
			.put("banner", new KeyTypeInt())
			.build();

	private static final Map<String, FieldType> MEASURES = ImmutableMap.<String, FieldType>builder()
			.put("impressions", new FieldTypeLong())
			.put("clicks", new FieldTypeLong())
			.put("conversions", new FieldTypeLong())
			.put("revenue", new FieldTypeDouble())
			.build();

	private static final Map<String, ReportingDSLExpression> COMPUTED_MEASURES = ImmutableMap.<String, ReportingDSLExpression>builder()
			.put("ctr", percent(divide("clicks", "impressions")))
			.build();

	private static final Map<String, String> CHILD_PARENT_RELATIONSHIPS = ImmutableMap.<String, String>builder()
			.put("campaign", "advertiser")
			.put("banner", "campaign")
			.build();

	private static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader, DIMENSIONS, MEASURES, CHILD_PARENT_RELATIONSHIPS);
	}

	private static Cube getCube(Eventloop eventloop, DefiningClassLoader classLoader,
	                            CubeMetadataStorage cubeMetadataStorage,
	                            AggregationMetadataStorage aggregationMetadataStorage,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            AggregationStructure cubeStructure) {
		Cube cube = new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, cubeStructure, 1_000_000, 1_000_000);
		cube.addAggregation(new AggregationMetadata("detailed", LogItem.DIMENSIONS,
				asList("impressions", "clicks", "conversions")));
		return cube;
	}

	private static ReportingConfiguration getReportingConfiguration() {
		return new ReportingConfiguration()
				.addResolvedAttributeForKey("advertiserName", singletonList("advertiser"), String.class, new AdvertiserResolver())
				.setComputedMeasures(COMPUTED_MEASURES);
	}

	private static class AdvertiserResolver implements AttributeResolver {
		@Override
		public Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes) {
			Map<PrimaryKey, Object[]> result = newHashMap();

			for (PrimaryKey key : keys) {
				String s = key.get(0).toString();

				switch (s) {
					case "1":
						result.put(key, new Object[]{"first"});
						break;
					case "2":
						result.put(key, new Object[]{"second"});
						break;
					case "3":
						result.put(key, new Object[]{"third"});
						break;
				}
			}

			return result;
		}
	}

	@Before
	public void setUp() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = new DefiningClassLoader();
		eventloop = new Eventloop();
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();
		AggregationStructure structure = getStructure(classLoader);

		ReportingConfiguration reportingConfiguration = getReportingConfiguration();
		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				getAggregationChunkStorage(eventloop, executor, structure, aggregationsDir);
		AggregationMetadataStorageSql aggregationMetadataStorage =
				new AggregationMetadataStorageSql(eventloop, executor, jooqConfiguration);
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, aggregationMetadataStorage);
		Cube cube = getCube(eventloop, classLoader, logToCubeMetadataStorage, aggregationMetadataStorage,
				aggregationChunkStorage, structure);
		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage, LOG_PARTITION_NAME);
		cube.setReportingConfiguration(reportingConfiguration);

		cube.saveAggregations(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		List<LogItem> logItems = asList(new LogItem(0, 1, 1, 1, 10, 1, 1, 0.0), new LogItem(1, 1, 1, 1, 20, 3, 1, 0.0),
				new LogItem(2, 1, 1, 1, 15, 2, 0, 0.0), new LogItem(3, 1, 1, 1, 30, 5, 2, 0.0),
				new LogItem(1, 2, 2, 2, 100, 5, 0, 0.0), new LogItem(1, 3, 3, 3, 80, 5, 0, 0.0));
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems =
				new StreamProducers.OfIterator<>(eventloop, logItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		cube.loadChunks(AsyncCallbacks.ignoreCompletionCallback());
		eventloop.run();

		server = CubeHttpServer.createServer(cube, eventloop, classLoader, SERVER_PORT);
		final CompletionCallbackFuture serverStartFuture = new CompletionCallbackFuture();
		eventloop.postConcurrently(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.listen();
				serverStartFuture.onComplete();
			}
		});
		new Thread(eventloop).start();
		serverStartFuture.await();

		clientEventloop = new Eventloop();
		NativeDnsResolver dnsClient = new NativeDnsResolver(clientEventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, TIMEOUT,
				HttpUtils.inetAddress("8.8.8.8"));
		httpClient = new AsyncHttpClient(clientEventloop, dnsClient);
		cubeHttpClient = new CubeHttpClient("http://127.0.0.1:" + SERVER_PORT, httpClient, TIMEOUT, structure, reportingConfiguration);
	}

	@Test
	public void testQuery() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date", "campaign")
				.measures("impressions", "clicks", "ctr", "revenue")
				.filters(new AggregationQuery.QueryPredicates()
						.eq("banner", 1)
						.between("date", 1, 2))
				.sort(AggregationQuery.QueryOrdering.asc("ctr"));

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(6, records.get(0).size());
		assertEquals(2, ((Number) records.get(0).get("date")).intValue());
		assertEquals(1, ((Number) records.get(0).get("advertiser")).intValue());
		assertEquals(15, ((Number) records.get(0).get("impressions")).intValue());
		assertEquals(13.333, ((Number) records.get(0).get("ctr")).doubleValue(), 1E-3);
		assertEquals(1, ((Number) records.get(1).get("date")).intValue());
		assertEquals(1, ((Number) records.get(1).get("advertiser")).intValue());
		assertEquals(20, ((Number) records.get(1).get("impressions")).intValue());
		assertEquals(15, ((Number) records.get(1).get("ctr")).doubleValue(), 1E-3);
		assertEquals(2, queryResult[0].getCount());
		assertEquals(newHashSet("impressions", "clicks", "ctr"), newHashSet(queryResult[0].getMeasures()));
		assertEquals(newHashSet("date", "advertiser", "campaign"), newHashSet(queryResult[0].getDimensions()));
		assertTrue(queryResult[0].getDrillDowns().isEmpty());
		assertTrue(queryResult[0].getAttributes().isEmpty());
		Map<String, Object> totals = queryResult[0].getTotals();
		assertEquals(35, ((Number) totals.get("impressions")).intValue());
		assertEquals(5, ((Number) totals.get("clicks")).intValue());
		assertEquals(14.285, ((Number) totals.get("ctr")).doubleValue(), 1E-3);
	}

	@Test
	public void testPaginationAndDrillDowns() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date")
				.measures("impressions")
				.limit(1)
				.offset(2);

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(1, records.size());
		assertEquals(2, records.get(0).size());
		assertEquals(2, ((Number) records.get(0).get("date")).intValue());
		assertEquals(15, ((Number) records.get(0).get("impressions")).intValue());
		assertEquals(4, queryResult[0].getCount());

		Set<List<String>> drillDowns = newHashSet();
		drillDowns.add(singletonList("advertiser"));
		drillDowns.add(asList("advertiser", "campaign"));
		drillDowns.add(asList("advertiser", "campaign", "banner"));
		assertEquals(drillDowns, queryResult[0].getDrillDowns());
	}

	@Test
	public void testFilterAttributes() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.dimensions("date")
				.attributes("advertiserName")
				.measures("impressions")
				.limit(0)
				.filters(new AggregationQuery.QueryPredicates().eq("advertiser", 1));

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		Map<String, Object> filterAttributes = queryResult[0].getFilterAttributes();
		assertEquals(1, filterAttributes.size());
		assertEquals("first", filterAttributes.get("advertiserName"));
	}

	@Test
	public void testSearchAndMeasureIgnoring() throws Exception {
		ReportingQuery query = new ReportingQuery()
				.attributes("advertiserName")
				.measures("clicks")
				.ignoreMeasures(true)
				.search("s");

		final ReportingQueryResult[] queryResult = new ReportingQueryResult[1];
		startBlocking(httpClient);
		cubeHttpClient.query(query, new ResultCallback<ReportingQueryResult>() {
			@Override
			public void onResult(ReportingQueryResult result) {
				queryResult[0] = result;
				stopBlocking(httpClient);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Query failed", exception);
			}
		});

		clientEventloop.run();

		List<Map<String, Object>> records = queryResult[0].getRecords();
		assertEquals(2, records.size());
		assertEquals(2, records.get(0).size());
		assertEquals(1, ((Number) records.get(0).get("advertiser")).intValue());
		assertEquals("first", records.get(0).get("advertiserName"));
		assertEquals(2, ((Number) records.get(1).get("advertiser")).intValue());
		assertEquals("second", records.get(1).get("advertiserName"));
		assertTrue(queryResult[0].getMeasures().isEmpty());
	}

	@After
	public void tearDown() throws Exception {
		final CompletionCallbackFuture serverStopFuture = new CompletionCallbackFuture();
		eventloop.postConcurrently(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.close();
				serverStopFuture.onComplete();
			}
		});
		serverStopFuture.await();
	}

	private static void startBlocking(EventloopService service) throws ExecutionException, InterruptedException {
		CompletionCallbackFuture future = new CompletionCallbackFuture();
		service.start(future);

		try {
			future.await();
		} catch (Exception e) {
			logger.error("Service {} start exception", e);
		}
	}

	private static void stopBlocking(EventloopService service) {
		CompletionCallbackFuture future = new CompletionCallbackFuture();
		service.stop(future);

		try {
			future.await();
		} catch (Exception e) {
			logger.error("Service {} stop exception", e);
		}
	}
}
