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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.MysqlStateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.runtime.metastore.mysql.MysqlDatasetStateStoreEntryManager;

import javax.sql.DataSource;


/**
 * A custom extension to {@link MysqlStateStore} for storing and reading {@link JobState.DatasetState}s.
 *
 * <p>
 *   The purpose of having this class is to hide some implementation details that are unnecessarily
 *   exposed if using the {@link MysqlStateStore} to store and serve job/dataset states between job runs.
 * </p>
 *
 * <p>
 *   In addition to persisting and reading {@link JobState.DatasetState}s. This class is also able to
 *   read job state files of existing jobs that store serialized instances of {@link JobState} for
 *   backward compatibility.
 * </p>
 *
 */
public class MysqlDatasetStateStore extends MysqlStateStore<JobState.DatasetState>
    implements DatasetStateStore<JobState.DatasetState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDatasetStateStore.class);

  public MysqlDatasetStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues)
      throws IOException {
    super(dataSource, stateStoreTableName, compressedValues, JobState.DatasetState.class);
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(String jobName) throws IOException {
    List<JobState.DatasetState> previousDatasetStates =
        getAll(jobName, "%" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX, JobStateSearchColumns.TABLE_NAME_ONLY);

    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();

    for (JobState.DatasetState previousDatasetState : previousDatasetStates) {
      datasetStatesByUrns.put(previousDatasetState.getDatasetUrn(), previousDatasetState);
    }

    // The dataset (job) state from the deprecated "current.jst" will be read even though
    // the job has transitioned to the new dataset-based mechanism
    if (datasetStatesByUrns.size() > 1) {
      datasetStatesByUrns.remove(ConfigurationKeys.DEFAULT_DATASET_URN);
    }

    return datasetStatesByUrns;
  }

  /**
   * Get the latest {@link JobState.DatasetState} of a given dataset.
   *
   * @param storeName the name of the dataset state store
   * @param datasetUrn the dataset URN
   * @return the latest {@link JobState.DatasetState} of the dataset or {@link null} if it is not found
   * @throws IOException
   */
  public JobState.DatasetState getLatestDatasetState(String storeName, String datasetUrn) throws IOException {
    String alias =
        Strings.isNullOrEmpty(datasetUrn) ? CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX
            : CharMatcher.is(':').replaceFrom(datasetUrn, '.') + "-" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;
    return get(storeName, alias, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    String jobName = datasetState.getJobName();
    String jobId = datasetState.getJobId();

    datasetUrn = CharMatcher.is(':').replaceFrom(datasetUrn, '.');
    String tableName = Strings.isNullOrEmpty(datasetUrn) ? jobId + DATASET_STATE_STORE_TABLE_SUFFIX
        : datasetUrn + "-" + jobId + DATASET_STATE_STORE_TABLE_SUFFIX;
    LOGGER.info("Persisting " + tableName + " to the job state store");

    put(jobName, tableName, datasetState);
    createAlias(jobName, tableName, getAliasName(datasetUrn));
  }

  @Override
  public void persistDatasetURNs(String storeName, Collection<String> datasetUrns)
      throws IOException {
    //do nothing for now
  }

  private static String getAliasName(String datasetUrn) {
    return Strings.isNullOrEmpty(datasetUrn) ? CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX
        : datasetUrn + "-" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;
  }

  @Override
  public List<MysqlDatasetStateStoreEntryManager> getMetadataForTables(StateStorePredicate predicate)
      throws IOException {
    // get the metadata from the parent class and convert
    List<MysqlStateStoreEntryManager> entryManagers =
        (List<MysqlStateStoreEntryManager>) super.getMetadataForTables(predicate);

    return entryManagers.stream().map(entry -> new MysqlDatasetStateStoreEntryManager(entry.getStoreName(),
        entry.getTableName(), entry.getTimestamp(), this)).collect(Collectors.toList());

  }
}
