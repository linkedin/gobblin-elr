/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;

import static org.apache.gobblin.configuration.ConfigurationKeys.KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY;

@Slf4j
public class KafkaCommonUtil {
  public static final long KAFKA_FLUSH_TIMEOUT_SECONDS = 15L;
  public static final String MAP_KEY_VALUE_DELIMITER_KEY = "->";
  public static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  public static void runWithTimeout(final Runnable runnable, long timeout, TimeUnit timeUnit) throws Exception {
    runWithTimeout(() -> {
      runnable.run();
      return null;
    }, timeout, timeUnit);
  }

  public static <T> T runWithTimeout(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<T> future = executor.submit(callable);
    // This does not cancel the already-scheduled task.
    executor.shutdown();
    try {
      return future.get(timeout, timeUnit);
    } catch (TimeoutException e) {
      // stop the running thread
      future.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      // unwrap the root cause
      Throwable t = e.getCause();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof Exception) {
        throw (Exception) t;
      } else {
        throw new IllegalStateException(t);
      }
    }
  }

  public static Map<String, String> getKafkaBrokerToSimpleNameMap(State state) {
    Map<String, String> kafkaBrokerUriToSimpleName = new HashMap<>();
    if (!state.contains(KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY)) {
        log.warn("Configuration does not contain value for {}", KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY);
        return kafkaBrokerUriToSimpleName;
    }
    String mapStr = state.getProp(KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY);
    for (String entry : LIST_SPLITTER.splitToList(mapStr)) {
      String[] items = entry.trim().split(MAP_KEY_VALUE_DELIMITER_KEY);
      kafkaBrokerUriToSimpleName.put(items[0], items[1]);
    }

    return kafkaBrokerUriToSimpleName;
  }
}
