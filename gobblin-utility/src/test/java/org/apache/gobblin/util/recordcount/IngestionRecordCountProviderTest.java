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

package org.apache.gobblin.util.recordcount;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link IngestionRecordCountProvider}.
 */
@Test(groups = { "gobblin.util.recordcount" })
public class IngestionRecordCountProviderTest {

  @Test
  public void testFileNameFormat() {
    IngestionRecordCountProvider filenameRecordCountProvider = new IngestionRecordCountProvider();
    Assert.assertEquals(IngestionRecordCountProvider.constructFilePath("/a/b/c.avro", 123), "/a/b/c.123.avro");
    Assert.assertEquals(filenameRecordCountProvider.getRecordCount(new Path("/a/b/c.123.avro")), 123);
    Assert.assertEquals(IngestionRecordCountProvider.containsRecordCount("/a/b/c.123.avro"), true);
    Assert.assertEquals(IngestionRecordCountProvider.containsRecordCount("/a/b/c.xyz.avro"), false);
  }

}
