/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */


package com.netflix.bdp.s3mper.listing;

import com.netflix.bdp.s3mper.metastore.impl.BigTableMetastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Random;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DarkloadTest {

    private final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

    private BigTableMetastore meta;
    
    private Configuration conf;
    private FileSystem fs;
    private String testBucket;
    private Path testPath;
    
    @Before
    public void setup() throws Exception {
        final String runId =  Integer.toHexString(new Random().nextInt());

        for (final String envVar : asList(GOOGLE_APPLICATION_CREDENTIALS)) {
            if (isNullOrEmpty(System.getenv(envVar))) {
                fail("Required environment variable " + envVar + " is not defined");
            }
        }

        conf = new Configuration();

        conf.set("fs.gs.project.id", "steel-ridge-91615");
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        conf.set("google.cloud.auth.service.account.json.keyfile",
                 System.getenv(GOOGLE_APPLICATION_CREDENTIALS));

        conf.setBoolean("s3mper.disable", false);
        conf.setBoolean("s3mper.darkload", true);
        conf.setBoolean("s3mper.reporting.disabled", true);
        conf.setBoolean("s3mper.failOnError", false);
        conf.setBoolean("s3mper.task.failOnError", false);
        conf.setBoolean("s3mper.failOnTimeout", false);
        conf.setBoolean("s3mper.listing.directory.tracking", true);
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        conf.setBoolean("s3mper.listing.task.check", true);
        conf.setBoolean("s3mper.listing.statOnMissingFile", true);
        conf.set("s3mper.metastore.name", "ConsistentListingMetastoreTest-" + runId);
        conf.setBoolean("s3mper.metastore.create", true);
        conf.setBoolean("fs.gs.impl.disable.cache", true);

        testBucket = System.getProperty("fs.test.bucket", "gs://rohan-test/");
        testPath = new Path(testBucket, System.getProperty("fs.test.path", "/test-" +  runId));

        fs = FileSystem.get(testPath.toUri(), conf);
        fs.mkdirs(testPath);

        meta = new BigTableMetastore();
        meta.initalize(testPath.toUri(), conf);
    }

    @After
    public void teardown() throws Exception {
        fs.delete(testPath, true);

        if (fs != null) {
            fs.close();
        }

        if (meta != null) {
            meta.close();
        }
    }

    @Test
    public void testDarkloading() throws Throwable {
        Path path = new Path(testPath + "/test");
        meta.add(path, false);
        assertEquals(0, fs.listStatus(path.getParent()).length);
    }

    @Test(expected = FileNotFoundException.class)
    public void testDarkloadingDisabled() throws Throwable {
        Configuration noDarkloadConf = new Configuration(conf);
        noDarkloadConf.setBoolean("s3mper.failOnError", true);
        noDarkloadConf.setBoolean("s3mper.darkload", false);
        FileSystem noDarkloadFs = FileSystem.get(testPath.toUri(), noDarkloadConf);
        Path path = new Path(testPath + "/test");
        meta.add(path, false);
        noDarkloadFs.listStatus(path.getParent());
    }
}