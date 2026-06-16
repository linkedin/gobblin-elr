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

package org.apache.gobblin.temporal.yarn;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.ConfigValueFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.temporal.api.enums.v1.WorkflowExecutionStatus;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.cluster.GobblinTemporalClusterManager;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.apache.gobblin.yarn.GobblinYarnLogSource;
import org.apache.gobblin.yarn.YarnContainerSecurityManager;
import org.apache.gobblin.yarn.YarnHelixUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * The Yarn ApplicationMaster class for Gobblin using Temporal.
 *
 * <p>
 *   This class runs the {@link YarnService} for all Yarn-related stuffs like ApplicationMaster registration
 *   and un-registration and Yarn container provisioning.
 * </p>
 *
 */
@Alpha
public class GobblinTemporalApplicationMaster extends GobblinTemporalClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalApplicationMaster.class);

  @Getter
  private final YarnService _yarnService;
  private LogCopier logCopier;

  public GobblinTemporalApplicationMaster(String applicationName, String applicationId, ContainerId containerId, Config config,
      YarnConfiguration yarnConfiguration) throws Exception {
    super(applicationName, applicationId, config.withValue(GobblinYarnConfigurationKeys.CONTAINER_NUM_KEY,
            ConfigValueFactory.fromAnyRef(YarnHelixUtils.getContainerNum(containerId.toString()))),
        Optional.<Path>absent());

    if (config.hasPath(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY)) {
      String containerLogDir = config.getString(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY);
      GobblinYarnLogSource gobblinYarnLogSource = new GobblinYarnLogSource();
      if (gobblinYarnLogSource.isLogSourcePresent()) {
        Path appWorkDir = PathUtils.combinePaths(containerLogDir, GobblinClusterUtils.getAppWorkDirPath(this.clusterName, this.applicationId), "AppMaster");
        logCopier = gobblinYarnLogSource.buildLogCopier(this.config, containerId.toString(), this.fs, appWorkDir);
        this.applicationLauncher.addService(logCopier);
      }
    }
    YarnHelixUtils.setYarnClassPath(config, yarnConfiguration);
    YarnHelixUtils.setAdditionalYarnClassPath(config, yarnConfiguration);
    this._yarnService = buildTemporalYarnService(this.config, applicationName, this.applicationId, yarnConfiguration, this.fs);
    this.applicationLauncher.addService(this._yarnService);

    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnContainerSecurityManager since security is enabled");
      this.applicationLauncher.addService(buildYarnContainerSecurityManager(this.config, this.fs));
    }

    // Add additional services
    List<String> serviceClassNames = ConfigUtils.getStringList(this.config,
        GobblinYarnConfigurationKeys.APP_MASTER_SERVICE_CLASSES);

    for (String serviceClassName : serviceClassNames) {
      Class<?> serviceClass = Class.forName(serviceClassName);
      this.applicationLauncher.addService((Service) GobblinConstructorUtils.invokeLongestConstructor(serviceClass, this));
    }
  }

  /**
   * Build the {@link YarnService} for the Application Master.
   */
  protected YarnService buildTemporalYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs)
      throws Exception {
    return new DynamicScalingYarnService(config, applicationName, applicationId, yarnConfiguration, fs, this.eventBus);
  }

  /**
   * Build the {@link YarnTemporalAppMasterSecurityManager} for the Application Master.
   */
  private YarnContainerSecurityManager buildYarnContainerSecurityManager(Config config, FileSystem fs) {
    return new YarnTemporalAppMasterSecurityManager(config, fs, this.eventBus, this.logCopier, this._yarnService);
  }

  /** Block until the AM application has fully stopped (all services incl. {@code YarnService} terminal). */
  @VisibleForTesting
  boolean awaitApplicationStopped(long timeout, TimeUnit unit) throws InterruptedException {
    return this.applicationLauncher.awaitStopped(timeout, unit);
  }

  /** The configured flow SLA ({@code gobblin.flow.sla.time}) in millis, or a generous fallback when unset. */
  private static long flowSlaMillis(Config config) {
    if (!config.hasPath(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME)) {
      return TimeUnit.DAYS.toMillis(1);
    }
    TimeUnit unit = TimeUnit.valueOf(ConfigUtils.getString(config,
        ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME_UNIT,
        ConfigurationKeys.DEFAULT_GOBBLIN_FLOW_FINISH_DEADLINE_TIME_UNIT));
    return unit.toMillis(config.getLong(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME));
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    options.addOption("d", GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME, true, "Yarn application id");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinTemporalApplicationMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) ||
          (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME))) {
        printUsage(options);
        System.exit(1);
      }

      //Because AM is restarted with the original AppSubmissionContext, it may have outdated delegation tokens.
      //So the refreshed tokens should be added into the container's UGI before any HDFS/Hive/RM access is performed.
      YarnHelixUtils.updateToken(GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinTemporalApplicationMaster.class,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE);

      LOGGER.info(JvmUtils.getJvmInputArguments());

      ContainerId containerId =
          ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));

      Config config = ConfigFactory.load();
      WorkflowExecutionStatus terminalStatus;
      try (GobblinTemporalApplicationMaster applicationMaster = new GobblinTemporalApplicationMaster(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME),
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME), containerId,
          config, new YarnConfiguration())) {

        applicationMaster.start();

        // start() can return while the workflow is still running (the job runs inside a service startUp(), so
        // ServiceBasedAppLauncher proceeds once app.start.waitForServicesTimeout elapses). Exiting then would
        // cancel/un-register the in-flight workflow and mis-report a successful long job as CANCELLED. Instead wait
        // until the application has actually stopped (all services incl. YarnService terminal -> after un-register),
        // bounded by the flow SLA (gobblin.flow.sla.time); the GaaS control plane cancels SLA overruns, unblocking
        // this wait.
        if (!applicationMaster.awaitApplicationStopped(flowSlaMillis(config), TimeUnit.MILLISECONDS)) {
          LOGGER.warn("AM did not stop within the flow SLA; proceeding to exit");
        }
        terminalStatus = GobblinTemporalJobLauncher.getLastTerminalStatus();
      }

      // Surface the captured workflow outcome as the AM JVM exit code (null = never reached terminal -> non-zero).
      int exitCode = GobblinTemporalJobLauncher.computeExitCode(terminalStatus);
      LOGGER.info("GobblinTemporalApplicationMaster exiting with code {} (workflow terminal status: {})",
          exitCode, terminalStatus);
      System.exit(exitCode);
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}

