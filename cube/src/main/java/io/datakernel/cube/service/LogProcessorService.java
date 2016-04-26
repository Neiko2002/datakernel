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

package io.datakernel.cube.service;

import io.datakernel.async.CompletionCallback;
import io.datakernel.cube.Cube;
import io.datakernel.cube.Cube.ChunksOverlapStatus;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.logfs.LogProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogProcessorService implements EventloopService, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(LogProcessorService.class);

	private final Eventloop eventloop;
	private final Cube cube;
	private final LogProcessor logProcessor;

	// settings
	private int basicPeriodMillis;
	private int maxPeriodMillis;
	private int softOverlappingChunksThreshold;
	private int criticalOverlappingChunksThreshold;
	private double periodMultiplier;

	// state
	private int currentPeriodMillis;
	private ScheduledRunnable processingTask;

	public LogProcessorService(Eventloop eventloop, Cube cube, LogProcessor logProcessor,
	                           int basicPeriodMillis, int maxPeriodMillis, int softOverlappingChunksThreshold,
	                           int criticalOverlappingChunksThreshold, double periodMultiplier) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.logProcessor = logProcessor;
		this.currentPeriodMillis = basicPeriodMillis;
		this.basicPeriodMillis = basicPeriodMillis;
		this.maxPeriodMillis = maxPeriodMillis;
		this.softOverlappingChunksThreshold = softOverlappingChunksThreshold;
		this.criticalOverlappingChunksThreshold = criticalOverlappingChunksThreshold;
		this.periodMultiplier = periodMultiplier;
	}

	private void processLogs() {
		cube.loadChunks(new CompletionCallback() {
			@Override
			public void onComplete() {
				ChunksOverlapStatus status = cube.getChunksOverlapStatus(softOverlappingChunksThreshold, criticalOverlappingChunksThreshold);

				switch (status) {
					case CRITICAL:
						updatePeriod(currentPeriodMillis * periodMultiplier);
						logger.info("Overlap status is critical, skipping this operation. New period is {} s", getCurrentPeriodSeconds());
						scheduleNext();
						return;
					case SOFT:
						updatePeriod(currentPeriodMillis * periodMultiplier);
						logger.info("Overlap status is soft, increasing period to {} s", getCurrentPeriodSeconds());
						break;
					case OK:
						updatePeriod(currentPeriodMillis / periodMultiplier);
						logger.info("Overlap status is ok, decreasing period to {} s", getCurrentPeriodSeconds());
				}

				logProcessor.processLogs(new CompletionCallback() {
					@Override
					public void onComplete() {
						scheduleNext();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Processing logs failed", e);
						resetPeriod();
						scheduleNext();
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Could not load chunks", e);
				resetPeriod();
				scheduleNext();
			}
		});
	}

	private void updatePeriod(double newPeriod) {
		currentPeriodMillis = constrainPeriod((int) newPeriod);
	}

	private void resetPeriod() {
		currentPeriodMillis = basicPeriodMillis;
	}

	private int getCurrentPeriodSeconds() {
		return currentPeriodMillis / 1000;
	}

	private int constrainPeriod(int period) {
		if (period > maxPeriodMillis)
			return maxPeriodMillis;
		if (period < basicPeriodMillis)
			return basicPeriodMillis;
		return period;
	}

	private void scheduleNext() {
		if (processingTask != null && processingTask.isCancelled())
			return;

		processingTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + currentPeriodMillis, new Runnable() {
			@Override
			public void run() {
				processLogs();
			}
		});
	}

	@Override
	public void start(CompletionCallback callback) {
		callback.onComplete();
		processLogs();
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (processingTask != null) {
			processingTask.cancel();
		}

		callback.onComplete();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	// jmx

	@JmxAttribute
	public int getCurrentPeriodMillis() {
		return currentPeriodMillis;
	}

	@JmxAttribute
	public int getBasicPeriodMillis() {
		return basicPeriodMillis;
	}

	@JmxAttribute
	public void setBasicPeriodMillis(int basicPeriodMillis) {
		this.basicPeriodMillis = basicPeriodMillis;
	}

	@JmxAttribute
	public int getMaxPeriodMillis() {
		return maxPeriodMillis;
	}

	@JmxAttribute
	public void setMaxPeriodMillis(int maxPeriodMillis) {
		this.maxPeriodMillis = maxPeriodMillis;
	}

	@JmxAttribute
	public int getSoftOverlappingChunksThreshold() {
		return softOverlappingChunksThreshold;
	}

	@JmxAttribute
	public void setSoftOverlappingChunksThreshold(int softOverlappingChunksThreshold) {
		this.softOverlappingChunksThreshold = softOverlappingChunksThreshold;
	}

	@JmxAttribute
	public int getCriticalOverlappingChunksThreshold() {
		return criticalOverlappingChunksThreshold;
	}

	@JmxAttribute
	public void setCriticalOverlappingChunksThreshold(int criticalOverlappingChunksThreshold) {
		this.criticalOverlappingChunksThreshold = criticalOverlappingChunksThreshold;
	}

	@JmxAttribute
	public double getPeriodMultiplier() {
		return periodMultiplier;
	}

	@JmxAttribute
	public void setPeriodMultiplier(double periodMultiplier) {
		this.periodMultiplier = periodMultiplier;
	}
}
