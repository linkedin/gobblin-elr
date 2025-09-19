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

package org.apache.gobblin.service.monitoring;

import com.google.common.util.concurrent.Service;


/**
 * A marker interface for spec change monitors to generalize initialization in {@link org.apache.gobblin.service.modules.core.GobblinServiceManager#specStoreChangeMonitor}
 */
public interface SpecChangeMonitor extends Service {

  /**
   * Set the monitor to active state where it can start processing events.
   * Should be called from {@link org.apache.gobblin.service.modules.core.GobblinServiceManager} after the service is started.
   */
  void setActive();
}
