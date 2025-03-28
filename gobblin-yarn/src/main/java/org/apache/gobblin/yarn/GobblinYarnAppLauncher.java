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

package org.apache.gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.metrics.reporter.util.KafkaReporterUtils;
import org.apache.gobblin.rest.JobExecutionInfoServer;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.util.AzkabanTags;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.EmailUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.filesystem.HdfsJarUploadUtils;
import org.apache.gobblin.util.hadoop.TokenUtils;
import org.apache.gobblin.util.io.StreamUtils;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.yarn.event.ApplicationReportArrivalEvent;
import org.apache.gobblin.yarn.event.GetApplicationReportFailureEvent;
import org.apache.gobblin.yarn.helix.HelixClusterLifecycleManager;

import static org.apache.gobblin.metrics.GobblinMetrics.METRICS_STATE_CUSTOM_TAGS;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


/**
 * A client driver to launch Gobblin as a Yarn application.
 *
 * <p>
 *   This class, upon starting, will check if there's a Yarn application that it has previously submitted and
 *   it is able to reconnect to. More specifically, it checks if an application with the same application name
 *   exists and can be reconnected to, i.e., if the application has not completed yet. If so, it simply starts
 *   monitoring that application.
 * </p>
 *
 * <p>
 *   On the other hand, if there's no such a reconnectable Yarn application, This class will launch a new Yarn
 *   application and start the {@link GobblinApplicationMaster}. It also persists the new application ID so it
 *   is able to reconnect to the Yarn application if it is restarted for some reason. Once the application is
 *   launched, this class starts to monitor the application by periodically polling the status of the application
 *   through a {@link ListeningExecutorService}.
 * </p>
 *
 * <p>
 *   If a shutdown signal is received, it sends a Helix
 *   {@link org.apache.helix.model.Message.MessageType#SCHEDULER_MSG} to the {@link GobblinApplicationMaster}
 *   asking it to shutdown and release all the allocated containers. It also sends an email notification for
 *   the shutdown if {@link GobblinYarnConfigurationKeys#EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY} is {@code true}.
 * </p>
 *
 * <p>
 *   This class has a scheduled task to get the {@link ApplicationReport} of the Yarn application periodically.
 *   Since it may fail to get the {@link ApplicationReport} due to reason such as the Yarn cluster is down for
 *   maintenance, it keeps track of the count of consecutive failures to get the {@link ApplicationReport}. If
 *   this count exceeds the maximum number allowed, it will initiate a shutdown.
 * </p>
 *
 * <p>
 *   Users of {@link GobblinYarnAppLauncher} need to call {@link #initializeYarnClients} which a child class can override.
 * </p>
 *
 * @author Yinan Li
 */
public class GobblinYarnAppLauncher {
  public static final String GOBBLIN_YARN_CONFIG_OUTPUT_PATH = "gobblin.yarn.configOutputPath";

  //Configuration key to signal the GobblinYarnAppLauncher mode
  public static final String GOBBLIN_YARN_APP_LAUNCHER_MODE = "gobblin.yarn.appLauncherMode";
  public static final String DEFAULT_GOBBLIN_YARN_APP_LAUNCHER_MODE = "";
  public static final String AZKABAN_APP_LAUNCHER_MODE_KEY = "azkaban";

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private static final String GOBBLIN_YARN_APPLICATION_TYPE = "GOBBLIN_YARN";

  // The application tags are set by HadoopJavaJob on mapreduce key even though it is not a mapreduce job
  // Reference: https://github.com/azkaban/azkaban/blob/6db750049f6fdf7842e18b8d533a3b736429bdf4/az-hadoop-jobtype-plugin/src/main/java/azkaban/jobtype/AbstractHadoopJavaProcessJob.java#L96
  private static final String APPLICATION_TAGS_KEY = "hadoop-inject.mapreduce.job.tags";

  // The set of Yarn application types this class is interested in. This is used to
  // lookup the application this class has launched previously upon restarting.
  private static final Set<String> APPLICATION_TYPES = ImmutableSet.of(GOBBLIN_YARN_APPLICATION_TYPE);

  // The set of Yarn application states under which the driver can reconnect to the Yarn application after restart
  private static final EnumSet<YarnApplicationState> RECONNECTABLE_APPLICATION_STATES = EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING
  );

  private final String applicationName;
  private final String appQueueName;
  private final String appViewAcl;

  private final Config config;

  @Getter
  private final Optional<HelixClusterLifecycleManager> helixClusterLifecycleManager;

  protected final Configuration yarnConfiguration;
  private final FileSystem fs;

  private final EventBus eventBus = new EventBus(GobblinYarnAppLauncher.class.getSimpleName());

  private final ScheduledExecutorService applicationStatusMonitor;
  private final long appReportIntervalMinutes;

  private final Optional<String> appMasterJvmArgs;
  private final String proxyJvmArgs;

  private final String sinkLogRootDir;

  private final Closer closer = Closer.create();

  // Yarn application ID
  private volatile Optional<ApplicationId> applicationId = Optional.absent();

  private volatile Optional<ServiceManager> serviceManager = Optional.absent();

  // Maximum number of consecutive failures allowed to get the ApplicationReport
  private final int maxGetApplicationReportFailures;

  // A count on the number of consecutive failures on getting the ApplicationReport
  private final AtomicInteger getApplicationReportFailureCount = new AtomicInteger();

  // This flag tells if the Yarn application has already completed. This is used to
  // tell if it is necessary to send a shutdown message to the ApplicationMaster.
  private volatile boolean applicationCompleted = false;

  private volatile boolean stopped = false;

  private final boolean emailNotificationOnShutdown;
  private final boolean detachOnExitEnabled;

  private final int appMasterMemoryMbs;
  private final int jvmMemoryOverheadMbs;
  private final double jvmMemoryXmxRatio;
  private Optional<AbstractTokenRefresher> tokenRefreshManager = Optional.absent();

  private final String containerTimezone;
  private final String appLauncherMode;
  protected final String originalYarnRMAddress;
  protected final Map<String, YarnClient> potentialYarnClients = new HashMap<>();
  private YarnClient yarnClient;
  private static final int MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT = 5;

  private final boolean jarCacheEnabled;

  private final Set<String> libJarNames = new HashSet<>(); // List of jars that are shared between appMaster and containers

  public GobblinYarnAppLauncher(Config config, YarnConfiguration yarnConfiguration) throws IOException {
    this.config = config.withValue(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY,
        ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));

    this.applicationName = config.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY);
    this.appQueueName = config.getString(GobblinYarnConfigurationKeys.APP_QUEUE_KEY);

    this.yarnConfiguration = yarnConfiguration;
    YarnHelixUtils.setYarnClassPath(config, this.yarnConfiguration);
    YarnHelixUtils.setAdditionalYarnClassPath(config, this.yarnConfiguration);
    this.yarnConfiguration.set("fs.automatic.close", "false");
    this.originalYarnRMAddress = this.yarnConfiguration.get(GobblinYarnConfigurationKeys.YARN_RESOURCE_MANAGER_ADDRESS);

    this.fs = GobblinClusterUtils.buildFileSystem(config, this.yarnConfiguration);
    this.closer.register(this.fs);

    boolean isHelixEnabled = ConfigUtils.getBoolean(config, GobblinYarnConfigurationKeys.HELIX_ENABLED,
        GobblinYarnConfigurationKeys.DEFAULT_HELIX_ENABLED);
    this.helixClusterLifecycleManager = isHelixEnabled
        ? Optional.of(this.closer.register(new HelixClusterLifecycleManager(config)))
        : Optional.absent();

    this.applicationStatusMonitor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("GobblinYarnAppStatusMonitor")));
    this.appReportIntervalMinutes = config.getLong(GobblinYarnConfigurationKeys.APP_REPORT_INTERVAL_MINUTES_KEY);

    this.appMasterJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.proxyJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.YARN_APPLICATION_PROXY_JVM_ARGS) ?
        config.getString(GobblinYarnConfigurationKeys.YARN_APPLICATION_PROXY_JVM_ARGS) : StringUtils.EMPTY;

    this.sinkLogRootDir = ConfigUtils.getString(config, GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY, null);

    this.maxGetApplicationReportFailures = config.getInt(GobblinYarnConfigurationKeys.MAX_GET_APP_REPORT_FAILURES_KEY);

    this.emailNotificationOnShutdown =
        config.getBoolean(GobblinYarnConfigurationKeys.EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY);

    this.appMasterMemoryMbs = this.config.getInt(GobblinYarnConfigurationKeys.APP_MASTER_MEMORY_MBS_KEY);

    this.jvmMemoryXmxRatio = ConfigUtils.getDouble(this.config,
        GobblinYarnConfigurationKeys.APP_MASTER_JVM_MEMORY_XMX_RATIO_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_APP_MASTER_JVM_MEMORY_XMX_RATIO);

    Preconditions.checkArgument(this.jvmMemoryXmxRatio >= 0 && this.jvmMemoryXmxRatio <= 1,
        GobblinYarnConfigurationKeys.APP_MASTER_JVM_MEMORY_XMX_RATIO_KEY + " must be between 0 and 1 inclusive");

    this.jvmMemoryOverheadMbs = ConfigUtils.getInt(this.config,
        GobblinYarnConfigurationKeys.APP_MASTER_JVM_MEMORY_OVERHEAD_MBS_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_APP_MASTER_JVM_MEMORY_OVERHEAD_MBS);

    Preconditions.checkArgument(this.jvmMemoryOverheadMbs < this.appMasterMemoryMbs * this.jvmMemoryXmxRatio,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY + " cannot be more than "
            + GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY + " * "
            + GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY);

    this.appViewAcl = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.APP_VIEW_ACL,
        GobblinYarnConfigurationKeys.DEFAULT_APP_VIEW_ACL);
    this.containerTimezone = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_TIMEZONE,
        GobblinYarnConfigurationKeys.DEFAULT_GOBBLIN_YARN_CONTAINER_TIMEZONE);

    this.detachOnExitEnabled = ConfigUtils
        .getBoolean(this.config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_DETACH_ON_EXIT_ENABLED,
            GobblinYarnConfigurationKeys.DEFAULT_GOBBLIN_YARN_DETACH_ON_EXIT);
    this.appLauncherMode = ConfigUtils.getString(this.config, GOBBLIN_YARN_APP_LAUNCHER_MODE, DEFAULT_GOBBLIN_YARN_APP_LAUNCHER_MODE);
    this.jarCacheEnabled = ConfigUtils.getBoolean(this.config, GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED_DEFAULT);

    try {
      config = addDynamicConfig(config);
      outputConfigToFile(config);
    } catch (SchemaRegistryException e) {
      throw new IOException(e);
    }
  }

  public void initializeYarnClients(Config config) {
    Set<String> potentialRMAddresses = new HashSet<>(ConfigUtils.getStringList(config, GobblinYarnConfigurationKeys.OTHER_YARN_RESOURCE_MANAGER_ADDRESSES));
    potentialRMAddresses.add(originalYarnRMAddress);
    for (String rmAddress : potentialRMAddresses) {
      YarnClient tmpYarnClient = YarnClient.createYarnClient();
      this.yarnConfiguration.set(GobblinYarnConfigurationKeys.YARN_RESOURCE_MANAGER_ADDRESS, rmAddress);
      tmpYarnClient.init(new YarnConfiguration(this.yarnConfiguration));
      this.potentialYarnClients.put(rmAddress, tmpYarnClient);
    }
  }

  /**
   * Launch a new Gobblin instance on Yarn.
   *
   * @throws IOException if there's something wrong launching the application
   * @throws YarnException if there's something wrong launching the application
   */
  public void launch() throws IOException, YarnException, InterruptedException {
    this.eventBus.register(this);

    //Before connect with yarn client, we need to login to get the token
    if (ConfigUtils.getBoolean(config, GobblinYarnConfigurationKeys.ENABLE_KEY_MANAGEMENT, false)) {
      this.tokenRefreshManager = Optional.of(buildTokenRefreshManager());
      this.tokenRefreshManager.get().loginAndScheduleTokenRenewal();
    }

    startYarnClient();

    this.applicationId = getReconnectableApplicationId();

    if (!this.applicationId.isPresent()) {
      LOGGER.info("No reconnectable application found so submitting a new application");
      this.yarnClient = potentialYarnClients.get(this.originalYarnRMAddress);
      this.applicationId = Optional.of(setupAndSubmitApplication());
    }

    if (helixClusterLifecycleManager.isPresent()) {
      this.helixClusterLifecycleManager.get()
          .getIsApplicationRunningFlag().compareAndSet(false, isApplicationRunning());
    }

    this.applicationStatusMonitor.scheduleAtFixedRate(() -> {
      try {
        eventBus.post(new ApplicationReportArrivalEvent(yarnClient.getApplicationReport(applicationId.get())));
      } catch (YarnException | IOException e) {
        LOGGER.error("Failed to get application report for Gobblin Yarn application " + applicationId.get(), e);
        eventBus.post(new GetApplicationReportFailureEvent(e));
      }
    }, 0, this.appReportIntervalMinutes, TimeUnit.MINUTES);

    addServices();
  }

  public boolean isApplicationRunning() {
    return this.applicationId.isPresent() && !this.applicationCompleted && !this.detachOnExitEnabled;
  }

  private void addServices() throws IOException {
    List<Service> services = Lists.newArrayList();
    if (this.tokenRefreshManager.isPresent()) {
      LOGGER.info("Adding KeyManagerService since key management is enabled");
      services.add(this.tokenRefreshManager.get());
    }
    if (!this.config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_DISABLE_DRIVER_COPY) ||
        !this.config.getBoolean(GobblinYarnConfigurationKeys.LOG_COPIER_DISABLE_DRIVER_COPY)) {
      services.add(buildLogCopier(this.config,
        new Path(this.sinkLogRootDir, this.applicationName + Path.SEPARATOR + this.applicationId.get().toString()),
        GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, this.applicationName, this.applicationId.get().toString())));
    }
    if (config.getBoolean(ConfigurationKeys.JOB_EXECINFO_SERVER_ENABLED_KEY)) {
      LOGGER.info("Starting the job execution info server since it is enabled");
      Properties properties = ConfigUtils.configToProperties(config);
      JobExecutionInfoServer executionInfoServer = new JobExecutionInfoServer(properties);
      services.add(executionInfoServer);
      if (config.getBoolean(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY)) {
        LOGGER.info("Starting the admin UI server since it is enabled");
        services.add(ServiceBasedAppLauncher.createAdminServer(properties,
                                                               executionInfoServer.getAdvertisedServerUri()));
      }
    } else if (config.getBoolean(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY)) {
      LOGGER.warn("NOT starting the admin UI because the job execution info server is NOT enabled");
    }

    if (services.size() > 0) {
      this.serviceManager = Optional.of(new ServiceManager(services));
      this.serviceManager.get().startAsync();
    } else {
      serviceManager = Optional.absent();
    }
  }

  /**
   * Stop this {@link GobblinYarnAppLauncher} instance.
   *
   * @throws IOException if this {@link GobblinYarnAppLauncher} instance fails to clean up its working directory.
   */
  public synchronized void stop() throws IOException, TimeoutException {
    if (this.stopped) {
      return;
    }

    LOGGER.info("Stopping the " + GobblinYarnAppLauncher.class.getSimpleName());

    try {
      if (this.serviceManager.isPresent()) {
        this.serviceManager.get().stopAsync().awaitStopped(5, TimeUnit.MINUTES);
      }

      ExecutorsUtils.shutdownExecutorService(this.applicationStatusMonitor, Optional.of(LOGGER), 5, TimeUnit.MINUTES);

      stopYarnClient();

      if (!this.detachOnExitEnabled) {
        LOGGER.info("Disabling all live Helix instances..");
      }
    } finally {
      try {
        if (this.applicationId.isPresent() && !this.detachOnExitEnabled) {
          cleanUpAppWorkDirectory(this.applicationId.get());
        }
      } finally {
        this.closer.close();
      }
    }

    this.stopped = true;
  }

  @Subscribe
  public void handleApplicationReportArrivalEvent(ApplicationReportArrivalEvent applicationReportArrivalEvent) {
    ApplicationReport applicationReport = applicationReportArrivalEvent.getApplicationReport();

    YarnApplicationState appState = applicationReport.getYarnApplicationState();
    LOGGER.info("Gobblin Yarn application state: " + appState.toString());

    // Reset the count on failures to get the ApplicationReport when there's one success
    this.getApplicationReportFailureCount.set(0);

    if (appState == YarnApplicationState.FINISHED ||
        appState == YarnApplicationState.FAILED ||
        appState == YarnApplicationState.KILLED) {
      applicationCompleted = true;
    }

    if (helixClusterLifecycleManager.isPresent()) {
      this.helixClusterLifecycleManager.get()
          .getIsApplicationRunningFlag().set(this.isApplicationRunning());
    }

    if (applicationCompleted) {
      LOGGER.info("Gobblin Yarn application finished with final status: " +
          applicationReport.getFinalApplicationStatus().toString());
      if (applicationReport.getFinalApplicationStatus() == FinalApplicationStatus.FAILED) {
        LOGGER.error("Gobblin Yarn application failed for the following reason: " + applicationReport.getDiagnostics());
      }

      try {
        GobblinYarnAppLauncher.this.stop();
      } catch (IOException ioe) {
        LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
      } catch (TimeoutException te) {
        LOGGER.error("Timeout in stopping the service manager", te);
      } finally {
        if (this.emailNotificationOnShutdown) {
          sendEmailOnShutdown(Optional.of(applicationReport));
        }
      }
    }
  }

  @Subscribe
  public void handleGetApplicationReportFailureEvent(
      GetApplicationReportFailureEvent getApplicationReportFailureEvent) {
    int numConsecutiveFailures = this.getApplicationReportFailureCount.incrementAndGet();
    if (numConsecutiveFailures > this.maxGetApplicationReportFailures) {
      LOGGER.warn(String
          .format("Number of consecutive failures to get the ApplicationReport %d exceeds the threshold %d",
              numConsecutiveFailures, this.maxGetApplicationReportFailures));

      try {
        stop();
      } catch (IOException ioe) {
        LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
      } catch (TimeoutException te) {
        LOGGER.error("Timeout in stopping the service manager", te);
      } finally {
        if (this.emailNotificationOnShutdown) {
          sendEmailOnShutdown(Optional.<ApplicationReport>absent());
        }
      }
    }
  }

  @VisibleForTesting
  void startYarnClient() {
    for (YarnClient yarnClient : potentialYarnClients.values()) {
      yarnClient.start();
    }
  }

  @VisibleForTesting
  void stopYarnClient() {
    for (YarnClient yarnClient : potentialYarnClients.values()) {
      yarnClient.stop();
    }
  }

  /**
   * A utility method that removes the "application_" prefix from the Yarn application id when the {@link GobblinYarnAppLauncher}
   * is launched via Azkaban. This is because when an Azkaban application is killed, Azkaban finds the Yarn application id
   * from the logs by searching for the pattern "application_". This is a hacky workaround to prevent Azkaban to detect the
   * Yarn application id from the logs.
   * @param applicationId
   * @return a sanitized application Id in the Azkaban mode.
   */
  private String sanitizeApplicationId(String applicationId) {
    if (this.detachOnExitEnabled && this.appLauncherMode.equalsIgnoreCase(AZKABAN_APP_LAUNCHER_MODE_KEY)) {
      applicationId = applicationId.replaceAll("application_", "");
    }
    return applicationId;
  }

  @VisibleForTesting
  Optional<ApplicationId> getReconnectableApplicationId() throws YarnException, IOException {
    for (YarnClient yarnClient: potentialYarnClients.values()) {
      List<ApplicationReport> applicationReports = yarnClient.getApplications(APPLICATION_TYPES, RECONNECTABLE_APPLICATION_STATES);
      if (applicationReports == null || applicationReports.isEmpty()) {
        continue;
      }

      // Try to find an application with a matching application name
      for (ApplicationReport applicationReport : applicationReports) {
        if (this.applicationName.equals(applicationReport.getName())) {
          String applicationId = sanitizeApplicationId(applicationReport.getApplicationId().toString());
          LOGGER.info("Found reconnectable application with application ID: " + applicationId);
          LOGGER.info("Application Tracking URL: " + applicationReport.getTrackingUrl());
          this.yarnClient = yarnClient;
          return Optional.of(applicationReport.getApplicationId());
        }
      }
    }

    return Optional.absent();
  }

  /**
   * Setup and submit the Gobblin Yarn application.
   * Retain at least the current and last month's jars in the cache (if configured) to handle executions running for ~30 days max
   * @throws IOException if there's anything wrong setting up and submitting the Yarn application
   * @throws YarnException if there's anything wrong setting up and submitting the Yarn application
   */
  @VisibleForTesting
  ApplicationId setupAndSubmitApplication() throws IOException, YarnException, InterruptedException {
    LOGGER.info("creating new yarn application");
    YarnClientApplication gobblinYarnApp = this.yarnClient.createApplication();
    ApplicationSubmissionContext appSubmissionContext = gobblinYarnApp.getApplicationSubmissionContext();
    if (config.hasPath(APPLICATION_TAGS_KEY)) {
      String tags = config.getString(APPLICATION_TAGS_KEY);
      Set<String> tagSet = new HashSet<>(Arrays.asList(tags.split(",")));
      appSubmissionContext.setApplicationTags(tagSet);
    }
    appSubmissionContext.setApplicationType(GOBBLIN_YARN_APPLICATION_TYPE);
    appSubmissionContext.setMaxAppAttempts(ConfigUtils.getInt(config, GobblinYarnConfigurationKeys.APP_MASTER_MAX_ATTEMPTS_KEY, GobblinYarnConfigurationKeys.DEFAULT_APP_MASTER_MAX_ATTEMPTS_KEY));
    ApplicationId applicationId = appSubmissionContext.getApplicationId();
    LOGGER.info("created new yarn application: " + applicationId.getId());

    GetNewApplicationResponse newApplicationResponse = gobblinYarnApp.getNewApplicationResponse();
    // Set up resource type requirements for ApplicationMaster
    Resource resource = prepareContainerResource(newApplicationResponse);

    // Add lib jars, and jars and files that the ApplicationMaster need as LocalResources
    Map<String, LocalResource> appMasterLocalResources = addAppMasterLocalResources(applicationId);

    ContainerLaunchContext amContainerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    amContainerLaunchContext.setLocalResources(appMasterLocalResources);
    amContainerLaunchContext.setEnvironment(YarnHelixUtils.getEnvironmentVariables(this.yarnConfiguration));
    amContainerLaunchContext.setCommands(Lists.newArrayList(buildApplicationMasterCommand(applicationId.toString(), resource.getMemory())));

    if (this.jarCacheEnabled) {
      Path jarCachePath = YarnHelixUtils.calculatePerMonthJarCachePath(this.config);
      // Retain at least the current and last month's jars to handle executions running for ~30 days max
      boolean cleanedSuccessfully = YarnHelixUtils.retainKLatestJarCachePaths(jarCachePath.getParent(), 2, this.fs);
      if (!cleanedSuccessfully) {
        LOGGER.warn("Failed to delete older jar cache directories");
      }
    }

    Map<ApplicationAccessType, String> acls = new HashMap<>(1);
    acls.put(ApplicationAccessType.VIEW_APP, this.appViewAcl);
    amContainerLaunchContext.setApplicationACLs(acls);

    if (UserGroupInformation.isSecurityEnabled()) {
      setupSecurityTokens(amContainerLaunchContext);
    }

    // Setup the application submission context
    appSubmissionContext.setApplicationName(this.applicationName);
    appSubmissionContext.setResource(resource);
    appSubmissionContext.setQueue(this.appQueueName);
    appSubmissionContext.setPriority(Priority.newInstance(0));
    appSubmissionContext.setAMContainerSpec(amContainerLaunchContext);
    // Also setup container local resources by copying local jars and files the container need to HDFS
    addContainerLocalResources(applicationId);

    // Submit the application
    LOGGER.info("Submitting application " + sanitizeApplicationId(applicationId.toString()));
    this.yarnClient.submitApplication(appSubmissionContext);

    LOGGER.info("Application successfully submitted and accepted");
    ApplicationReport applicationReport = this.yarnClient.getApplicationReport(applicationId);
    LOGGER.info("Application Name: " + applicationReport.getName());
    LOGGER.info("Application Tracking URL: " + applicationReport.getTrackingUrl());
    LOGGER.info("Application User: " + applicationReport.getUser() + " Queue: " + applicationReport.getQueue());

    // Temporal workflow tracking url
    String temporalWorkflowTrackingUrl = ConfigUtils.getString(config, "gobblin.temporal.ui.server.url", "");
    if (StringUtils.isNotBlank(temporalWorkflowTrackingUrl)) {
      LOGGER.info("Temporal Workflow Tracking URL: " + temporalWorkflowTrackingUrl);
    }

    return applicationId;
  }

  private Resource prepareContainerResource(GetNewApplicationResponse newApplicationResponse) {
    int memoryMbs = this.appMasterMemoryMbs;
    int maximumMemoryCapacity = newApplicationResponse.getMaximumResourceCapability().getMemory();
    if (memoryMbs > maximumMemoryCapacity) {
      LOGGER.info(String.format("Specified AM memory [%d] is above the maximum memory capacity [%d] of the "
          + "cluster, using the maximum memory capacity instead.", memoryMbs, maximumMemoryCapacity));
      memoryMbs = maximumMemoryCapacity;
    }

    int vCores = this.config.getInt(GobblinYarnConfigurationKeys.APP_MASTER_CORES_KEY);
    int maximumVirtualCoreCapacity = newApplicationResponse.getMaximumResourceCapability().getVirtualCores();
    if (vCores > maximumVirtualCoreCapacity) {
      LOGGER.info(String.format("Specified AM vcores [%d] is above the maximum vcore capacity [%d] of the "
          + "cluster, using the maximum vcore capacity instead.", memoryMbs, maximumMemoryCapacity));
      vCores = maximumVirtualCoreCapacity;
    }

    // Set up resource type requirements for ApplicationMaster
    return Resource.newInstance(memoryMbs, vCores);
  }

  private Map<String, LocalResource> addAppMasterLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, this.applicationName, applicationId.toString());
    Path jarsRootDir = this.jarCacheEnabled ? YarnHelixUtils.calculatePerMonthJarCachePath(this.config) : appWorkDir;

    Path appMasterWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_MASTER_WORK_DIR_NAME);
    Path appMasterJarsCacheDir = new Path(jarsRootDir, GobblinYarnConfigurationKeys.APP_MASTER_WORK_DIR_NAME);
    LOGGER.info("Configured GobblinApplicationMaster work directory to: {}", appMasterWorkDir);
    LOGGER.info("Configured GobblinApplicationMaster jars directory to: {}", appMasterJarsCacheDir);

    Map<String, LocalResource> appMasterResources = Maps.newHashMap();
    FileSystem localFs = FileSystem.getLocal(new Configuration());

    if (this.config.hasPath(GobblinYarnConfigurationKeys.LIB_JARS_DIR_KEY)) {
      // Lib jars are shared between all containers, store at the root level
      Path libJarsDestDir = new Path(jarsRootDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME);
      Path unsharedJarsDestDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME);
      addLibJars(new Path(this.config.getString(GobblinYarnConfigurationKeys.LIB_JARS_DIR_KEY)),
          Optional.of(appMasterResources), libJarsDestDir, unsharedJarsDestDir, localFs);
      this.libJarNames.addAll(appMasterResources.keySet());
      LOGGER.info("Added lib jars to directory: {} and execution-private directory: {}", libJarsDestDir, unsharedJarsDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_JARS_KEY)) {
      Path appJarsDestDir = new Path(appMasterJarsCacheDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      Path unsharedJarsDestDir = new Path(appMasterWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_JARS_KEY),
          Optional.of(appMasterResources), appJarsDestDir, unsharedJarsDestDir, localFs);
      LOGGER.info("Added app jars to directory: {} and execution-private directory: {}", appJarsDestDir, unsharedJarsDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_FILES_LOCAL_KEY),
          Optional.of(appMasterResources), appFilesDestDir, localFs);
      LOGGER.info("Added app local files to directory: {}", appFilesDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_FILES_REMOTE_KEY)) {
      YarnHelixUtils.addRemoteFilesToLocalResources(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_FILES_REMOTE_KEY),
          appMasterResources, yarnConfiguration);
      LOGGER.info("Added remote files to local resources");
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_ZIPS_REMOTE_KEY)) {
      YarnHelixUtils.addRemoteZipsToLocalResources(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_ZIPS_REMOTE_KEY),
          appMasterResources, yarnConfiguration);
      LOGGER.info("Added remote zips to local resources");
    }
    if (this.config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) {
      Path appFilesDestDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addJobConfPackage(this.config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY), appFilesDestDir,
          appMasterResources);
      LOGGER.info("Added job conf package to directory: {}", appFilesDestDir);
    }

    return appMasterResources;
  }

  private void addContainerLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, this.applicationName,
        applicationId.toString());
    Path jarsRootDir = this.jarCacheEnabled ? YarnHelixUtils.calculatePerMonthJarCachePath(this.config) : appWorkDir;
    Path containerWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);
    Path containerJarsRootDir = new Path(jarsRootDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);
    LOGGER.info("Configured Container work directory to: {}", containerWorkDir);
    LOGGER.info("Configured Container jars directory to: {}", containerJarsRootDir);
    FileSystem localFs = FileSystem.getLocal(new Configuration());

    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY)) {
      Path appJarsDestDir = new Path(containerJarsRootDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      Path unsharedJarsDestDir = new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY),
          Optional.<Map<String, LocalResource>>absent(), appJarsDestDir, unsharedJarsDestDir, localFs);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_FILES_LOCAL_KEY),
          Optional.<Map<String, LocalResource>>absent(), appFilesDestDir, localFs);
    }
  }

  private void addLibJars(Path srcLibJarDir, Optional<Map<String, LocalResource>> resourceMap, Path destCacheDir, Path unsharedDir,
      FileSystem localFs) throws IOException {

    // Missing classpath-jars will be a fatal error.
    if (!localFs.exists(srcLibJarDir)) {
      throw new IllegalStateException(String.format("The library directory[%s] are not being found, abort the application", srcLibJarDir));
    }

    FileStatus[] libJarFiles = localFs.listStatus(srcLibJarDir);
    if (libJarFiles == null || libJarFiles.length == 0) {
      return;
    }

    for (FileStatus libJarFile : libJarFiles) {
      Path destFilePath = HdfsJarUploadUtils.calculateDestJarFilePath(this.fs, libJarFile.getPath().getName(), unsharedDir, destCacheDir);
      if (HdfsJarUploadUtils.uploadJarToHdfs(fs, libJarFile, MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT, destFilePath) && resourceMap.isPresent()) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
      } else {
        LOGGER.warn("Failed to upload jar file {} to HDFS", libJarFile.getPath());
      }
    }
  }
  private void addAppJars(String jarFilePathList, Optional<Map<String, LocalResource>> resourceMap, Path destCacheDir, Path unsharedDir,
      FileSystem localFs) throws IOException {
    for (String jarFilePath : SPLITTER.split(jarFilePathList)) {
      Path srcFilePath = new Path(jarFilePath);
      FileStatus localJar = localFs.getFileStatus(srcFilePath);
      Path destFilePath = HdfsJarUploadUtils.calculateDestJarFilePath(this.fs, localJar.getPath().getName(), unsharedDir, destCacheDir);
      if (HdfsJarUploadUtils.uploadJarToHdfs(fs, localJar, MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT, destFilePath)) {
        if (resourceMap.isPresent()) {
          YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
        }
      } else {
        LOGGER.warn("Failed to upload jar file {} to HDFS", srcFilePath);
      }
    }
  }

  private void addAppLocalFiles(String localFilePathList, Optional<Map<String, LocalResource>> resourceMap,
      Path destDir, FileSystem localFs) throws IOException {

    for (String localFilePath : SPLITTER.split(localFilePathList)) {
      Path srcFilePath = new Path(localFilePath);
      Path destFilePath = new Path(destDir, srcFilePath.getName());
      if (localFs.exists(srcFilePath)) {
        if (this.fs.exists(destFilePath)) {
          LOGGER.info("The destination app jar {} already exists, skipping upload", destFilePath);
        } else {
          this.fs.copyFromLocalFile(srcFilePath, destFilePath);
        }
        if (resourceMap.isPresent()) {
          YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
        }
      } else {
        LOGGER.warn(String.format("The request file %s doesn't exist", srcFilePath));
      }
    }
  }

  private void addJobConfPackage(String jobConfPackagePath, Path destDir, Map<String, LocalResource> resourceMap)
      throws IOException {
    Path srcFilePath = new Path(jobConfPackagePath);
    Path destFilePath = new Path(destDir, srcFilePath.getName() + GobblinClusterConfigurationKeys.TAR_GZ_FILE_SUFFIX);
    StreamUtils.tar(FileSystem.getLocal(this.yarnConfiguration), this.fs, srcFilePath, destFilePath);
    YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.ARCHIVE, resourceMap);
  }

  @VisibleForTesting
  protected String buildApplicationMasterCommand(String applicationId, int memoryMbs) {
    Class appMasterClass;
    try {
       String appMasterClassName = ConfigUtils.getString(
          config, GobblinYarnConfigurationKeys.APP_MASTER_CLASS, GobblinYarnConfigurationKeys.DEFAULT_APP_MASTER_CLASS);
       appMasterClass = Class.forName(appMasterClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    String logFileName = ConfigUtils.getString(config,
        GobblinYarnConfigurationKeys.APP_MASTER_LOG_FILE_NAME, appMasterClass.getSimpleName());

    return new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append((int) (memoryMbs * this.jvmMemoryXmxRatio) - this.jvmMemoryOverheadMbs).append("M")
        .append(" -D").append(GobblinYarnConfigurationKeys.JVM_USER_TIMEZONE_CONFIG).append("=").append(this.containerTimezone)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_DIR_NAME).append("=").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_FILE_NAME).append("=").append(logFileName).append(".").append(ApplicationConstants.STDOUT)
        .append(" -D").append(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY).append("=").append(config.getString(GobblinYarnConfigurationKeys.YARN_APPLICATION_LAUNCHER_START_TIME_KEY))
        .append(" -D").append(GobblinYarnConfigurationKeys.YARN_APPLICATION_LIB_JAR_LIST).append("=").append(String.join(",", this.libJarNames))
        .append(" ").append(JvmUtils.formatJvmArguments(this.appMasterJvmArgs))
        .append(" ").append(this.proxyJvmArgs)
        .append(" ").append(appMasterClass.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME)
        .append(" ").append(applicationId)
        .append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            logFileName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            logFileName).append(".").append(ApplicationConstants.STDERR)
        .toString();
  }

  private void setupSecurityTokens(ContainerLaunchContext containerLaunchContext) throws IOException, InterruptedException {
    LOGGER.info("setting up SecurityTokens for containerLaunchContext.");
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    String renewerName = this.yarnConfiguration.get(YarnConfiguration.RM_PRINCIPAL);

    // Pass on the credentials from the hadoop token file if present.
    // The value in the token file takes precedence.
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      LOGGER.info("HADOOP_TOKEN_FILE_LOCATION is set to {} reading tokens from it for containerLaunchContext.", System.getenv(HADOOP_TOKEN_FILE_LOCATION));
      Credentials tokenFileCredentials = Credentials.readTokenStorageFile(new File(System.getenv(HADOOP_TOKEN_FILE_LOCATION)),
              new Configuration());
      credentials.addAll(tokenFileCredentials);
      LOGGER.debug("All containerLaunchContext tokens: {} present in file {} ", credentials.getAllTokens(), System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    TokenUtils.getAllFSTokens(new Configuration(), credentials, renewerName,
        Optional.absent(), ConfigUtils.getStringList(this.config, TokenUtils.OTHER_NAMENODES));
    // Only pass token here and no secrets. (since there is no simple way to remove single token/ get secrets)
    // For RM token, only pass the RM token for the current RM, or the RM will fail to update the token
    Credentials finalCredentials = new Credentials();
    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
      if (token.getKind().equals(new Text("RM_DELEGATION_TOKEN")) && !token.getService().equals(new Text(this.originalYarnRMAddress))) {
        continue;
      }
      finalCredentials.addToken(token.getService(), token);
    }
    Closer closer = Closer.create();
    try {
      DataOutputBuffer dataOutputBuffer = closer.register(new DataOutputBuffer());
      finalCredentials.writeTokenStorageToStream(dataOutputBuffer);
      ByteBuffer fsTokens = ByteBuffer.wrap(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength());
      containerLaunchContext.setTokens(fsTokens);
      LOGGER.info("Setting containerLaunchContext with All credential tokens: " + finalCredentials.getAllTokens());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private LogCopier buildLogCopier(Config config, Path sinkLogDir, Path appWorkDir) throws IOException {
    FileSystem rawLocalFs = this.closer.register(new RawLocalFileSystem());
    rawLocalFs.initialize(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration());

    LogCopier.Builder builder = LogCopier.newBuilder()
            .useSrcFileSystem(this.fs)
            .useDestFileSystem(rawLocalFs)
            .readFrom(getHdfsLogDir(appWorkDir))
            .writeTo(sinkLogDir)
            .acceptsLogFileExtensions(ImmutableSet.of(ApplicationConstants.STDOUT, ApplicationConstants.STDERR));
    return builder.build();
  }

  private Path getHdfsLogDir(Path appWorkDir) throws IOException {
    Path logRootDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_LOGS_DIR_NAME);
    if (!this.fs.exists(logRootDir)) {
      this.fs.mkdirs(logRootDir);
    }

    return logRootDir;
  }

  /**
   *
   * @return
   * @throws IOException
   */
  private AbstractTokenRefresher buildTokenRefreshManager() throws IOException {
    Path tokenFilePath = YarnContainerSecurityManager.getYarnTokenFilePath(this.config, this.fs);
    String securityManagerClassName = ConfigUtils.getString(config, GobblinYarnConfigurationKeys.SECURITY_MANAGER_CLASS, GobblinYarnConfigurationKeys.DEFAULT_SECURITY_MANAGER_CLASS);

    try {
      // The constructor utils will catastrophically fail if you pass null as a param because it's not possible to infer the type of null.
      // So, there are 2 separate code paths for helix and non-helix security manager
      if (helixClusterLifecycleManager.isPresent()) {
        ClassAliasResolver<AbstractYarnAppSecurityManager> aliasResolver = new ClassAliasResolver<>(AbstractYarnAppSecurityManager.class);
        return (AbstractYarnAppSecurityManager) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(aliasResolver.resolve(securityManagerClassName)), this.config, this.fs,
            tokenFilePath, this.helixClusterLifecycleManager.get());
      }

      ClassAliasResolver aliasResolver = new ClassAliasResolver<>(AbstractTokenRefresher.class);
      return (AbstractTokenRefresher) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(aliasResolver.resolve(securityManagerClassName)), this.config, this.fs, tokenFilePath);
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void cleanUpAppWorkDirectory(ApplicationId applicationId) throws IOException {
    // Create a new filesystem as this.fs may have been closed by the Yarn Application, and FS.get() will return a cached instance of the closed FS
    FileSystem fs = GobblinClusterUtils.createFileSystem(this.config, this.yarnConfiguration);
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, fs, this.applicationName, applicationId.toString());
    if (fs.exists(appWorkDir)) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      fs.delete(appWorkDir, true);
    }
  }

  private void sendEmailOnShutdown(Optional<ApplicationReport> applicationReport) {
    String subject = String.format("Gobblin Yarn application %s completed", this.applicationName);

    StringBuilder messageBuilder = new StringBuilder("Gobblin Yarn ApplicationReport:");
    if (applicationReport.isPresent()) {
      messageBuilder.append("\n");
      messageBuilder.append("\tApplication ID: ").append(applicationReport.get().getApplicationId()).append("\n");
      messageBuilder.append("\tApplication attempt ID: ")
          .append(applicationReport.get().getCurrentApplicationAttemptId()).append("\n");
      messageBuilder.append("\tFinal application status: ").append(applicationReport.get().getFinalApplicationStatus())
          .append("\n");
      messageBuilder.append("\tStart time: ").append(applicationReport.get().getStartTime()).append("\n");
      messageBuilder.append("\tFinish time: ").append(applicationReport.get().getFinishTime()).append("\n");

      if (!Strings.isNullOrEmpty(applicationReport.get().getDiagnostics())) {
        messageBuilder.append("\tDiagnostics: ").append(applicationReport.get().getDiagnostics()).append("\n");
      }

      ApplicationResourceUsageReport resourceUsageReport = applicationReport.get().getApplicationResourceUsageReport();
      if (resourceUsageReport != null) {
        messageBuilder.append("\tUsed containers: ").append(resourceUsageReport.getNumUsedContainers()).append("\n");
        Resource usedResource = resourceUsageReport.getUsedResources();
        if (usedResource != null) {
          messageBuilder.append("\tUsed memory (MBs): ").append(usedResource.getMemory()).append("\n");
          messageBuilder.append("\tUsed vcores: ").append(usedResource.getVirtualCores()).append("\n");
        }
      }
    } else {
      messageBuilder.append(' ').append("Not available");
    }

    try {
      EmailUtils.sendEmail(ConfigUtils.configToState(this.config), subject, messageBuilder.toString());
    } catch (EmailException ee) {
      LOGGER.error("Failed to send email notification on shutdown", ee);
    }
  }

  private static Config addDynamicConfig(Config config) throws IOException {
    Properties properties = ConfigUtils.configToProperties(config);
    if (KafkaReporterUtils.isKafkaReportingEnabled(properties) && KafkaReporterUtils.isKafkaAvroSchemaRegistryEnabled(properties)) {
      KafkaAvroSchemaRegistry registry = new KafkaAvroSchemaRegistry(properties);
      return addMetricReportingDynamicConfig(config, registry);
    } else {
      return config;
    }
  }

  /**
   * Write the config to the file specified with the config key {@value GOBBLIN_YARN_CONFIG_OUTPUT_PATH} if it
   * is configured.
   * @param config the config to output
   * @throws IOException
   */
  @VisibleForTesting
  static void outputConfigToFile(Config config)
      throws IOException {
    // If a file path is specified then write the Azkaban config to that path in HOCON format.
    // This can be used to generate an application.conf file to pass to the yarn app master and containers.
    if (config.hasPath(GOBBLIN_YARN_CONFIG_OUTPUT_PATH)) {
      File configFile = new File(config.getString(GOBBLIN_YARN_CONFIG_OUTPUT_PATH));
      File parentDir = configFile.getParentFile();

      if (parentDir != null && !parentDir.exists()) {
        if (!parentDir.mkdirs()) {
          throw new IOException("Error creating directories for " + parentDir);
        }
      }

      ConfigRenderOptions configRenderOptions = ConfigRenderOptions.defaults();
      configRenderOptions = configRenderOptions.setComments(false);
      configRenderOptions = configRenderOptions.setOriginComments(false);
      configRenderOptions = configRenderOptions.setFormatted(true);
      configRenderOptions = configRenderOptions.setJson(false);

      String renderedConfig = config.root().render(configRenderOptions);

      FileUtils.writeStringToFile(configFile, renderedConfig, Charsets.UTF_8);
    }
  }

  /**
   * A method that adds dynamic config related to Kafka-based metric reporting. In particular, if Kafka based metric
   * reporting is enabled and {@link KafkaAvroSchemaRegistry} is configured, this method registers the metric reporting
   * related schemas and adds the returned schema ids to the config to be used by metric reporters in {@link org.apache.gobblin.yarn.GobblinApplicationMaster}
   * and the {@link org.apache.gobblin.cluster.GobblinTaskRunner}s. The advantage of doing this is that the TaskRunners
   * do not have to initiate a connection with the schema registry server and reduces the chances of metric reporter
   * instantiation failures in the {@link org.apache.gobblin.cluster.GobblinTaskRunner}s.
   * @param config
   */
  @VisibleForTesting
  static Config addMetricReportingDynamicConfig(Config config, KafkaAvroSchemaRegistry registry) throws IOException {
    Properties properties = ConfigUtils.configToProperties(config);
    if (KafkaReporterUtils.isEventsEnabled(properties)) {
      Schema schema = KafkaReporterUtils.getGobblinTrackingEventSchema();
      String schemaId = registry.register(schema, KafkaReporterUtils.getEventsTopic(properties).get());
      LOGGER.info("Adding schemaId {} for GobblinTrackingEvent to the config", schemaId);
      config = config.withValue(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_AVRO_SCHEMA_ID,
          ConfigValueFactory.fromAnyRef(schemaId));
    }

    if (KafkaReporterUtils.isMetricsEnabled(properties)) {
      Schema schema = KafkaReporterUtils.getMetricReportSchema();
      String schemaId = registry.register(schema, KafkaReporterUtils.getMetricsTopic(properties).get());
      LOGGER.info("Adding schemaId {} for MetricReport to the config", schemaId);
      List<Tag<?>> tags = Lists.newArrayList();
      tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
      GobblinMetrics.addCustomTagsToProperties(properties, tags);

      config = config
          .withValue(ConfigurationKeys.METRICS_REPORTING_METRICS_KAFKA_AVRO_SCHEMA_ID,
              ConfigValueFactory.fromAnyRef(schemaId))
          .withValue(METRICS_STATE_CUSTOM_TAGS,
              ConfigValueFactory.fromAnyRef(properties.getProperty(METRICS_STATE_CUSTOM_TAGS)));
    }
    return config;
  }

  public static void main(String[] args) throws Exception {
    final GobblinYarnAppLauncher gobblinYarnAppLauncher =
        new GobblinYarnAppLauncher(ConfigFactory.load(), new YarnConfiguration());
    gobblinYarnAppLauncher.initializeYarnClients(ConfigFactory.load());

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          gobblinYarnAppLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timeout in stopping the service manager", te);
        } finally {
          if (gobblinYarnAppLauncher.emailNotificationOnShutdown) {
            gobblinYarnAppLauncher.sendEmailOnShutdown(Optional.<ApplicationReport>absent());
          }
        }
      }
    });

    gobblinYarnAppLauncher.launch();
  }
}
