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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.logfs.LogProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogProcessorService implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LogProcessorService.class);

	private final Eventloop eventloop;
	private final Cube cube;
	private final LogProcessor logProcessor;
	private final int logProcessingPeriodMillis;

	private ScheduledRunnable processingTask;

	public LogProcessorService(Eventloop eventloop, Cube cube, LogProcessor logProcessor, int logProcessingPeriodMillis) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.logProcessor = logProcessor;
		this.logProcessingPeriodMillis = logProcessingPeriodMillis;
	}

	private void processLogs() {
		cube.loadChunks(new CompletionCallback() {
			@Override
			protected void onComplete() {
				if (cube.containsExcessiveNumberOfOverlappingChunks()) {
					logger.info("Cube contains excessive number of overlapping chunks. Skipping this aggregation operation");
					scheduleNext();
					return;
				}

				logProcessor.processLogs(new CompletionCallback() {
					@Override
					protected void onComplete() {
						scheduleNext();
					}

					@Override
					protected void onException(Exception e) {
						logger.error("Processing logs failed", e);
						scheduleNext();
					}
				});
			}

			@Override
			protected void onException(Exception e) {
				logger.error("Could not load chunks", e);
				scheduleNext();
			}
		});
	}

	private void scheduleNext() {
		if (processingTask != null && processingTask.isCancelled())
			return;

		processingTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + logProcessingPeriodMillis, new Runnable() {
			@Override
			public void run() {
				processLogs();
			}
		});
	}

	@Override
	public void start(CompletionCallback callback) {
		callback.complete();
		processLogs();
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (processingTask != null) {
			processingTask.cancel();
		}

		callback.complete();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
