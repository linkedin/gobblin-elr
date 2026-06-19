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

package org.apache.gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;


@Test(groups = { "gobblin.util" })
public class HadoopUtilsTest {

  public static final String TEST_DIR_PATH = "path/to/dir";
  public static final String TEST_CHILD_DIR_NAME = "HadoopUtilsTestDir";

  @Test
  public void fsShortSerializationTest() {
    State state = new State();
    short mode = 420;
    FsPermission perms = new FsPermission(mode);

    HadoopUtils.serializeWriterFilePermissions(state, 0, 0, perms);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(mode, deserializedPerms.toShort());
  }

  @Test
  public void fsOctalSerializationTest() {
    State state = new State();
    String mode = "0755";

    HadoopUtils.setWriterFileOctalPermissions(state, 0, 0, mode);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(Integer.parseInt(mode, 8), deserializedPerms.toShort());
  }

  @Test
  public void testRenameRecursively() throws Exception {

    final Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), TEST_CHILD_DIR_NAME);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      fs.mkdirs(hadoopUtilsTestDir);

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRename/a/b/c"));

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c"));
      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/t1.txt"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e/t2.txt"));

      HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging"), new Path(hadoopUtilsTestDir, "testRename"));

      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/t1.txt")));
      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/e/t2.txt")));
    } finally {
      fs.delete(hadoopUtilsTestDir, true);
    }

  }

  @Test
  public void testRenameRecursivelyWithAccessDeniedOnExistenceCheck() throws Exception {
    final Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    FileSystem fs = Mockito.spy(FileSystem.getLocal(new Configuration()));
    Path targetDir = new Path(hadoopUtilsTestDir, "testRename");

    // For testing that the rename works when the target
    Mockito.doThrow(new AccessDeniedException("Test")).when(fs).exists(targetDir);

    try {
      fs.mkdirs(hadoopUtilsTestDir);

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRename/a/b/c"));

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c"));
      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/t1.txt"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e/t2.txt"));

      HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging"), targetDir);

      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/t1.txt")));
      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/e/t2.txt")));
    } finally {
      fs.delete(hadoopUtilsTestDir, true);
    }

  }

  @Test(groups = { "performance" })
  public void testRenamePerformance() throws Exception {

    FileSystem fs = Mockito.mock(FileSystem.class);

    Path sourcePath = new Path("/source");
    Path s1 = new Path(sourcePath, "d1");

    FileStatus[] sourceStatuses = new FileStatus[10000];
    FileStatus[] targetStatuses = new FileStatus[1000];

    for (int i = 0; i < sourceStatuses.length; i++) {
      sourceStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }
    for (int i = 0; i < targetStatuses.length; i++) {
      targetStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }

    Mockito.when(fs.getUri()).thenReturn(new URI("file:///"));
    Mockito.when(fs.getFileStatus(sourcePath)).thenAnswer(getDelayedAnswer(getFileStatus(sourcePath, true)));
    Mockito.when(fs.exists(sourcePath)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(sourcePath)).thenAnswer(getDelayedAnswer(new FileStatus[]{getFileStatus(s1, true)}));
    Mockito.when(fs.exists(s1)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(s1)).thenAnswer(getDelayedAnswer(sourceStatuses));

    Path target = new Path("/target");
    Path s1Target = new Path(target, "d1");
    Mockito.when(fs.exists(target)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.exists(s1Target)).thenAnswer(getDelayedAnswer(true));

    Mockito.when(fs.mkdirs(Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.rename(Mockito.any(Path.class), Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));

    HadoopUtils.renameRecursively(fs, sourcePath, target);
  }

  private <T> Answer<T> getDelayedAnswer(final T result) throws Exception {
    return new Answer<T>() {
      @Override
      public T answer(InvocationOnMock invocation)
          throws Throwable {
        Thread.sleep(50);
        return result;
      }
    };
  }

  private FileStatus getFileStatus(Path path, boolean dir) {
    return new FileStatus(1, dir, 1, 1, 1, path);
  }

  @Test
  public void testSafeRenameRecursively() throws Exception {
    final Logger log = LoggerFactory.getLogger("HadoopUtilsTest.testSafeRenameRecursively");

    final Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      // do many iterations to catch rename race conditions
      for (int i = 0; i < 100; i++) {
        fs.mkdirs(hadoopUtilsTestDir);
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c"));

        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c"));
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/t1.txt"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e/t2.txt"));

        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c"));
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/t3.txt"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e/t4.txt"));

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        final Throwable[] runnableErrors = {null, null};

        Future<?> renameFuture = executorService.submit(new Runnable() {

          @Override
          public void run() {
            try {
              HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging1"), new Path(
                  hadoopUtilsTestDir, "testSafeRename"));
            } catch (Throwable e) {
              log.error("Rename error: " + e, e);
              runnableErrors[0] = e;
            }
          }
        });

        Future<?> safeRenameFuture = executorService.submit(new Runnable() {

          @Override
          public void run() {
            try {
              HadoopUtils.safeRenameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging2"), new Path(
                  hadoopUtilsTestDir, "testSafeRename"));
            } catch (Throwable e) {
              log.error("Safe rename error: " + e, e);
              runnableErrors[1] = e;
            }
          }
        });

        // Wait for the executions to complete
        renameFuture.get(10, TimeUnit.SECONDS);
        safeRenameFuture.get(10, TimeUnit.SECONDS);

        executorService.shutdownNow();

        Assert.assertNull(runnableErrors[0], "Runnable 0 error: " + runnableErrors[0]);
        Assert.assertNull(runnableErrors[1], "Runnable 1 error: " + runnableErrors[1]);

        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t1.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t3.txt")));
        Assert.assertTrue(!fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/e/t2.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t2.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t4.txt")));
        fs.delete(hadoopUtilsTestDir, true);
      }
    } finally {
      fs.delete(hadoopUtilsTestDir, true);
    }

  }

  @Test
  public void testRenameRecursivelyOrderedMovesDataBeforeMetadata() throws Exception {
    final Path testDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    final FileSystem fs = Mockito.spy(FileSystem.getLocal(new Configuration()));

    // Record the destination of every successful rename, in the order they actually happen. Data renames are
    // slowed down so that, WITHOUT the phase barrier, the fast metadata renames would land first -- making this a
    // real regression guard for the ordering (not a pass-by-luck on fast local renames).
    final List<String> renameDestinations = Collections.synchronizedList(Lists.<String>newArrayList());
    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        Path dst = (Path) invocation.getArguments()[1];
        if (dst.toString().endsWith(".orc")) {
          Thread.sleep(200);
        }
        Boolean result = (Boolean) invocation.callRealMethod();
        if (Boolean.TRUE.equals(result)) {
          renameDestinations.add(dst.toString());
        }
        return result;
      }
    }).when(fs).rename(Mockito.any(Path.class), Mockito.any(Path.class));

    try {
      // Staging tree mirrors an Iceberg table: data/ holds the data files, metadata/ references them by path.
      Path staging = new Path(testDir, "staging");
      fs.mkdirs(new Path(staging, "table1/data"));
      fs.mkdirs(new Path(staging, "table1/metadata"));
      fs.create(new Path(staging, "table1/data/f1.orc")).close();
      fs.create(new Path(staging, "table1/data/f2.orc")).close();
      fs.create(new Path(staging, "table1/metadata/m1.avro")).close();
      fs.create(new Path(staging, "table1/metadata/m2.avro")).close();

      // Pre-create the target table dir and its data/metadata children so the rename descends to file level --
      // the incremental/retry case where per-file ordering actually matters (no atomic whole-subtree move).
      Path target = new Path(testDir, "target");
      fs.mkdirs(new Path(target, "table1/data"));
      fs.mkdirs(new Path(target, "table1/metadata"));

      HadoopUtils.renameRecursivelyOrdered(fs, staging, target, HadoopUtils::isIcebergMetadataDir);

      // Every file landed at the target.
      Assert.assertTrue(fs.exists(new Path(target, "table1/data/f1.orc")));
      Assert.assertTrue(fs.exists(new Path(target, "table1/data/f2.orc")));
      Assert.assertTrue(fs.exists(new Path(target, "table1/metadata/m1.avro")));
      Assert.assertTrue(fs.exists(new Path(target, "table1/metadata/m2.avro")));

      // Every data file was renamed strictly before any metadata file.
      int lastDataIdx = -1;
      int firstMetadataIdx = Integer.MAX_VALUE;
      for (int i = 0; i < renameDestinations.size(); i++) {
        String dst = renameDestinations.get(i);
        if (dst.endsWith(".orc")) {
          lastDataIdx = Math.max(lastDataIdx, i);
        } else if (dst.endsWith(".avro")) {
          firstMetadataIdx = Math.min(firstMetadataIdx, i);
        }
      }
      Assert.assertTrue(lastDataIdx >= 0, "Expected at least one data rename, got: " + renameDestinations);
      Assert.assertTrue(firstMetadataIdx != Integer.MAX_VALUE,
          "Expected at least one metadata rename, got: " + renameDestinations);
      Assert.assertTrue(lastDataIdx < firstMetadataIdx,
          "All data files must be renamed before any metadata file. Actual order: " + renameDestinations);
    } finally {
      fs.delete(testDir, true);
    }
  }

  @Test
  public void testRenameRecursivelyOrderedFreshTableCopiesEverything() throws Exception {
    // When the target table dir does not yet exist, the whole subtree moves atomically (nothing is deferred);
    // verify the ordered variant still copies every file correctly.
    final Path testDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      Path staging = new Path(testDir, "staging");
      fs.mkdirs(new Path(staging, "table1/data"));
      fs.mkdirs(new Path(staging, "table1/metadata"));
      fs.create(new Path(staging, "table1/data/f1.orc")).close();
      fs.create(new Path(staging, "table1/metadata/m1.avro")).close();

      Path target = new Path(testDir, "target");

      HadoopUtils.renameRecursivelyOrdered(fs, staging, target, HadoopUtils::isIcebergMetadataDir);

      Assert.assertTrue(fs.exists(new Path(target, "table1/data/f1.orc")));
      Assert.assertTrue(fs.exists(new Path(target, "table1/metadata/m1.avro")));
    } finally {
      fs.delete(testDir, true);
    }
  }

  @Test
  public void testRenameRecursivelyOrderedAbortsBeforeMetadataWhenDataRenameFails() throws Exception {
    // If a data-phase rename fails, the metadata phase must never run (Iceberg metadata must not be published
    // when the data it references failed to land).
    final Path testDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    final FileSystem fs = Mockito.spy(FileSystem.getLocal(new Configuration()));

    // Fail every data (.orc) rename; record any metadata (.avro) rename that is attempted.
    final List<String> metadataRenameAttempts = Collections.synchronizedList(Lists.<String>newArrayList());
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Path dst = (Path) invocation.getArguments()[1];
        if (dst.toString().endsWith(".avro")) {
          metadataRenameAttempts.add(dst.toString());
        }
        if (dst.toString().endsWith(".orc")) {
          throw new IOException("Injected failure renaming data file " + dst);
        }
        return invocation.callRealMethod();
      }
    }).when(fs).rename(Mockito.any(Path.class), Mockito.any(Path.class));

    try {
      Path staging = new Path(testDir, "staging");
      fs.mkdirs(new Path(staging, "table1/data"));
      fs.mkdirs(new Path(staging, "table1/metadata"));
      fs.create(new Path(staging, "table1/data/f1.orc")).close();
      fs.create(new Path(staging, "table1/data/f2.orc")).close();
      fs.create(new Path(staging, "table1/metadata/m1.avro")).close();
      fs.create(new Path(staging, "table1/metadata/m2.avro")).close();

      // Pre-create the target table dir and children so the rename descends to file level.
      Path target = new Path(testDir, "target");
      fs.mkdirs(new Path(target, "table1/data"));
      fs.mkdirs(new Path(target, "table1/metadata"));

      try {
        HadoopUtils.renameRecursivelyOrdered(fs, staging, target, HadoopUtils::isIcebergMetadataDir);
        Assert.fail("Expected the rename to fail during the data phase");
      } catch (IOException expected) {
        // expected: the data-phase failure aborts the whole operation
      }

      Assert.assertTrue(metadataRenameAttempts.isEmpty(),
          "Metadata must not be renamed after a data-phase failure, but these were attempted: "
              + metadataRenameAttempts);
      // And no metadata file should have actually landed at the target.
      Assert.assertFalse(fs.exists(new Path(target, "table1/metadata/m1.avro")));
      Assert.assertFalse(fs.exists(new Path(target, "table1/metadata/m2.avro")));
    } finally {
      fs.delete(testDir, true);
    }
  }

  @Test
  public void testIsIcebergMetadataDir() {
    Assert.assertTrue(HadoopUtils.isIcebergMetadataDir(new Path("/data/openhouse/db/tbl-uuid/metadata")));
    Assert.assertFalse(HadoopUtils.isIcebergMetadataDir(new Path("/data/openhouse/db/tbl-uuid/data")));
    // The `/data` HDFS mount is not an Iceberg metadata dir, and the predicate matches the dir, not its files.
    Assert.assertFalse(HadoopUtils.isIcebergMetadataDir(new Path("/data")));
    Assert.assertFalse(HadoopUtils.isIcebergMetadataDir(new Path("/data/openhouse/db/tbl-uuid/metadata/m1.avro")));
  }

  @Test
  public void testSanitizePath() throws Exception {
    Assert.assertEquals(HadoopUtils.sanitizePath("/A:B/::C:::D\\", "abc"), "/AabcB/abcabcCabcabcabcDabc");
    Assert.assertEquals(HadoopUtils.sanitizePath(":\\:\\/", ""), "/");
    try {
      HadoopUtils.sanitizePath("/A:B/::C:::D\\", "a:b");
      throw new RuntimeException();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("substitute contains illegal characters"));
    }
  }

  @Test
  public void testStateToConfiguration() throws IOException {
    Map<String, String> vals = Maps.newHashMap();
    vals.put("test_key1", "test_val1");
    vals.put("test_key2", "test_val2");

    Configuration expected = HadoopUtils.newConfiguration();
    State state = new State();
    for (Map.Entry<String, String> entry : vals.entrySet()) {
      state.setProp(entry.getKey(), entry.getValue());
      expected.set(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(HadoopUtils.getConfFromState(state), expected);
    Assert.assertEquals(HadoopUtils.getConfFromState(state, Optional.<String>absent()), expected);
    Assert.assertEquals(HadoopUtils.getConfFromState(state, Optional.of("dummy")), expected);
  }

  @Test
  public void testEncryptedStateToConfiguration() throws IOException {
    Map<String, String> vals = Maps.newHashMap();
    vals.put("test_key1", "test_val1");
    vals.put("test_key2", "test_val2");

    State state = new State();
    for (Map.Entry<String, String> entry : vals.entrySet()) {
      state.setProp(entry.getKey(), entry.getValue());
    }

    Map<String, String> encryptedVals = Maps.newHashMap();
    encryptedVals.put("key1", "val1");
    encryptedVals.put("key2", "val2");

    final String encryptedPath = "encrypted.name.space";
    for (Map.Entry<String, String> entry : encryptedVals.entrySet()) {
      state.setProp(encryptedPath + "." + entry.getKey(), entry.getValue());
    }

    Configuration configuration = HadoopUtils.getConfFromState(state, Optional.of(encryptedPath));

    for (Map.Entry<String, String> entry : vals.entrySet()) {
      String val = configuration.get(entry.getKey());
      Assert.assertEquals(val, entry.getValue());
    }

    for (Map.Entry<String, String> entry : encryptedVals.entrySet()) {
      Assert.assertNotNull(configuration.get(entry.getKey())); //Verify key with child path exist as decryption is unit tested in ConfigUtil.
    }
  }

  @Test
  public void testMoveToTrash() throws IOException {
    Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    Configuration conf = new Configuration();
    // Set the time to keep it in trash to 10 minutes.
    // 0 means object will be deleted instantly.
    conf.set("fs.trash.interval", "10");
    FileSystem fs = FileSystem.getLocal(conf);
    Trash trash = new Trash(fs, conf);
    TrashPolicy trashPolicy = TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory());
    Path trashPath = Path.mergePaths(trashPolicy.getCurrentTrashDir(), hadoopUtilsTestDir);

    fs.mkdirs(hadoopUtilsTestDir);
    Assert.assertTrue(fs.exists(hadoopUtilsTestDir));
    // Move the parent dir to trash because we created it at the beginning of this function.
    HadoopUtils.moveToTrash(fs, hadoopUtilsTestDir.getParent(), conf);
    Assert.assertFalse(fs.exists(hadoopUtilsTestDir));
    Assert.assertTrue(fs.exists(trashPath));
  }

  @Test
  public void testEnsureDirectoryExistsWithAclPreservation() throws Exception {
    final Path testDir = new Path(new Path(TEST_DIR_PATH), "HadoopUtilsTestDir");
    FileSystem fs = Mockito.mock(FileSystem.class);
    Path targetDir = new Path(testDir, "target");

    Mockito.when(fs.exists(targetDir)).thenReturn(false);
    Mockito.when(fs.exists(targetDir.getParent())).thenReturn(true);

    // Create ACL entries
    List<AclEntry> aclEntries = Lists.newArrayList(
        new AclEntry.Builder()
            .setName("user1")
            .setType(AclEntryType.USER)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.ALL)
            .build(),
        new AclEntry.Builder()
            .setName("group1")
            .setType(AclEntryType.GROUP)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.READ_EXECUTE)
            .build()
    );

    // Create OwnerAndPermission with the ACLs
    OwnerAndPermission ownerAndPermission = getOwnerAndPermissionForAclEntries(aclEntries);

    // Mock mkdirs to return true
    Mockito.when(fs.mkdirs(targetDir)).thenReturn(true);
    // Call ensureDirectoryExists with copyOnlySourceAclToDest=true
    HadoopUtils.ensureDirectoryExists(fs, targetDir,
        Collections.singletonList(ownerAndPermission).listIterator(),
        true, true);
    // Verify mkdirs was called
    Mockito.verify(fs).mkdirs(targetDir);
    Mockito.verify(fs).removeAcl(targetDir);
    // Verify modifyAclEntries was called with correct ACLs
    Mockito.verify(fs).modifyAclEntries(targetDir, aclEntries);
  }

  @Test
  public void testEnsureDirectoryExistsWithExistingDirectory() throws Exception {
    final Path testDir = new Path(new Path(TEST_DIR_PATH), TEST_CHILD_DIR_NAME);
    FileSystem fs = Mockito.mock(FileSystem.class);
    // Create target directory path
    Path targetDir = new Path(testDir, "target");

    Mockito.when(fs.exists(targetDir)).thenReturn(true);
    Mockito.when(fs.exists(targetDir.getParent())).thenReturn(true);

    // Create new ACLs to set
    List<AclEntry> aclEntries = Lists.newArrayList(
        new AclEntry.Builder()
            .setName("user2")
            .setType(AclEntryType.USER)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.ALL)
            .build()
    );

    OwnerAndPermission ownerAndPermission = getOwnerAndPermissionForAclEntries(aclEntries);

    // Call ensureDirectoryExists - should be a no-op since directory exists
    HadoopUtils.ensureDirectoryExists(fs, targetDir,
        Collections.singletonList(ownerAndPermission).listIterator(),
        true, true);

    // Verify mkdirs was not called
    Mockito.verify(fs, Mockito.never()).mkdirs(targetDir);
    // Verify removeAcl was not called
    Mockito.verify(fs, Mockito.never()).removeAcl(targetDir);
    // Verify setAcl was not called
    Mockito.verify(fs, Mockito.never()).modifyAclEntries(Mockito.any(Path.class), Mockito.anyList());

  }

  @Test
  public void testEnsureDirectoryExistsWithAcl() throws Exception {
    final Path testDir = new Path(new Path(TEST_DIR_PATH), TEST_CHILD_DIR_NAME);
    FileSystem fs = Mockito.mock(FileSystem.class);

    // Create target directory path
    Path targetDir = new Path(testDir, "target");

    Mockito.when(fs.exists(targetDir)).thenReturn(false);
    Mockito.when(fs.exists(targetDir.getParent())).thenReturn(true);

    // Create ACL entries
    List<AclEntry> aclEntries = Lists.newArrayList(
        new AclEntry.Builder()
            .setName("user1")
            .setType(AclEntryType.USER)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.ALL)
            .build(),
        new AclEntry.Builder()
            .setName("group1")
            .setType(AclEntryType.GROUP)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.READ_EXECUTE)
            .build()
    );

    // Create OwnerAndPermission with the ACLs
    OwnerAndPermission ownerAndPermission = getOwnerAndPermissionForAclEntries(aclEntries);

    // Mock mkdirs to return true
    Mockito.when(fs.mkdirs(targetDir)).thenReturn(true);

    // Call ensureDirectoryExists
    HadoopUtils.ensureDirectoryExists(fs, targetDir,
        Collections.singletonList(ownerAndPermission).listIterator(),
        true);

    // Verify mkdirs was called
    Mockito.verify(fs).mkdirs(targetDir);
    // Verify removeAcl was never called
    Mockito.verify(fs, Mockito.never()).removeAcl(Mockito.any(Path.class));
    // Verify modifyAclEntries was called with correct ACLs
    Mockito.verify(fs).modifyAclEntries(targetDir, aclEntries);
  }

  @Test
  public void testEnsureDirectoryExistsWithEmptyAcl() throws Exception {
    final Path testDir = new Path(new Path(TEST_DIR_PATH), TEST_CHILD_DIR_NAME);
    FileSystem fs = Mockito.mock(FileSystem.class);

    // Create target directory path
    Path targetDir = new Path(testDir, "target");

    Mockito.when(fs.exists(targetDir)).thenReturn(false);
    Mockito.when(fs.exists(targetDir.getParent())).thenReturn(true);

    // Create OwnerAndPermission with empty ACLs
    OwnerAndPermission ownerAndPermission = getOwnerAndPermissionForAclEntries(Collections.emptyList());

    // Mock mkdirs to return true
    Mockito.when(fs.mkdirs(targetDir)).thenReturn(true);

    // Call ensureDirectoryExists
    HadoopUtils.ensureDirectoryExists(fs, targetDir,
        Collections.singletonList(ownerAndPermission).listIterator(),
        true);

    // Verify mkdirs was called
    Mockito.verify(fs).mkdirs(targetDir);
    // Verify removeAcl was never called
    Mockito.verify(fs, Mockito.never()).removeAcl(Mockito.any(Path.class));
    // Verify modifyAclEntries was not called since ACLs are empty
    Mockito.verify(fs, Mockito.never()).modifyAclEntries(Mockito.any(Path.class), Mockito.anyList());
  }

  private OwnerAndPermission getOwnerAndPermissionForAclEntries(List<AclEntry> aclEntries) {
    return new OwnerAndPermission(
        "owner",
        "group",
        new FsPermission("755"),
        aclEntries
    );
  }
}
