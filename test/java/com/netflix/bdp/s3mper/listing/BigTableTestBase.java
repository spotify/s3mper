package com.netflix.bdp.s3mper.listing;

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.impl.BigTableMetastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BigTableTestBase {
    protected static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

    protected static final Logger log = Logger.getLogger(BigTableTestBase.class.getName());

    protected static BigTableMetastore meta;

    protected static Configuration conf;
    protected static FileSystem markerFs;
    protected static FileSystem deleteFs;
    protected static String testBucket;
    protected static Path testPath;

    @BeforeClass
    public static void setUpClass() throws Exception {
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
        conf.setBoolean("s3mper.failOnError", true);
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", true);
        conf.setBoolean("s3mper.reporting.disabled", true);
        conf.setLong("s3mper.listing.recheck.count", 10);
        conf.setLong("s3mper.listing.recheck.period", 1000);
        conf.setFloat("s3mper.listing.threshold", 1);
        conf.set("s3mper.metastore.name", "ConsistentListingMetastoreTest-" + runId);
        conf.setBoolean("s3mper.metastore.create", true);

        testBucket = System.getProperty("fs.test.bucket", "gs://rohan-test/");
        testPath = new Path(testBucket, System.getProperty("fs.test.path", "/test-" +  runId));

        markerFs = FileSystem.get(testPath.toUri(), conf);

        Configuration deleteConf = new Configuration(conf);
        deleteConf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        deleteFs = FileSystem.get(testPath.toUri(), deleteConf);

        meta = new BigTableMetastore();
        meta.initalize(testPath.toUri(), conf);

        Configuration janitorConf = new Configuration(conf);
        janitorConf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (markerFs != null) {
          markerFs.close();
        }

        if (deleteFs != null) {
          deleteFs.close();
        }

        if (meta != null) {
          meta.close();
        }
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("==========================   Setting Up =========================== ");

        conf.setBoolean("s3mper.disable", false);
        conf.setBoolean("s3mper.failOnError", true);
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", true);
        conf.setBoolean("s3mper.reporting.disabled", true);
        conf.setLong("s3mper.listing.recheck.count", 10);
        conf.setLong("s3mper.listing.recheck.period", 1000);
        conf.setFloat("s3mper.listing.threshold", 1);
        conf.setBoolean("s3mper.listing.directory.tracking", true);

        deleteFs.delete(testPath, true);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("==========================  Tearing Down  =========================");
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        deleteFs.delete(testPath, true);
        conf.setFloat("s3mper.listing.threshold", 1);
    }

    protected void validateMetadata(Path parent, FileInfo... children) throws Exception {
        List<FileInfo> list = meta.list(Collections.singletonList(parent));
        assertThat(list, is(Arrays.asList(children)));
    }
}
