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

package org.apache.gobblin.runtime.app;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/** Unit tests for {@link ServiceBasedAppLauncher#awaitStopped(long, java.util.concurrent.TimeUnit)}. */
public class ServiceBasedAppLauncherTest {

  /** A do-nothing service that starts and stops cleanly. */
  private static Service noopService() {
    return new AbstractIdleService() {
      @Override protected void startUp() { }
      @Override protected void shutDown() { }
    };
  }

  private static ServiceBasedAppLauncher newLauncher(String name) throws Exception {
    Properties props = new Properties();
    // Bound the service-start wait so a misbehaving service can't hang the test (default is FOREVER).
    props.setProperty(ServiceBasedAppLauncher.STARTUP_TIMEOUT_SECONDS, "30");
    return new ServiceBasedAppLauncher(props, name);
  }

  @Test
  public void testAwaitStoppedReturnsTrueWhenNeverStarted() throws Exception {
    // No serviceManager exists until start(); there is nothing to wait for, so it must return immediately.
    ServiceBasedAppLauncher launcher = newLauncher("never-started");
    assertTrue(launcher.awaitStopped(1, TimeUnit.SECONDS));
  }

  @Test
  public void testAwaitStoppedTimesOutWhileRunningThenReturnsAfterStop() throws Exception {
    ServiceBasedAppLauncher launcher = newLauncher("await-stopped");
    launcher.addService(noopService());
    launcher.start();

    // Services are RUNNING (not terminal), so a short wait must time out -> false.
    assertFalse(launcher.awaitStopped(200, TimeUnit.MILLISECONDS),
        "awaitStopped must time out (false) while the application is still running");

    launcher.stop();

    // After stop(), every service reaches a terminal state, so awaitStopped must return true.
    assertTrue(launcher.awaitStopped(30, TimeUnit.SECONDS),
        "awaitStopped must return true once the application has stopped");
  }
}
