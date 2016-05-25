package com.netflix.bdp.s3mper.listing;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.SourceLocation;
import org.aspectj.runtime.internal.AroundClosure;
import org.junit.Test;

import java.io.OutputStream;
import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PartialReadTest extends BigTableTestBase {
    private Object doRenameNotifier = new Object();
    private Object finnishRenameNotifier = new Object();
    private volatile boolean metadataCopied = false;
    private volatile boolean renameExecuted = false;
    private volatile boolean metadataDeleted = false;

    private class BlockingRename implements ProceedingJoinPoint {
        private final FileSystem fs;
        private final Path src;
        private final Path dst;

        public BlockingRename(FileSystem fs, Path src, Path dst) {
            this.fs = fs;
            this.src = src;
            this.dst = dst;
        }

        @Override
        public void set$AroundClosure(AroundClosure arc) {

        }

        @Override
        public Object proceed() throws Throwable {
            metadataCopied = true;
            synchronized (doRenameNotifier) {
                doRenameNotifier.wait();
            }

            boolean success = fs.rename(src, dst);

            renameExecuted = true;

            if (success) {
                synchronized (finnishRenameNotifier) {
                    finnishRenameNotifier.wait();
                }

            }

            return success;
        }

        @Override
        public Object proceed(Object[] args) throws Throwable {
            return null;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public String toShortString() {
            return null;
        }

        @Override
        public String toLongString() {
            return null;
        }

        @Override
        public Object getThis() {
            return null;
        }

        @Override
        public Object getTarget() {
            return this.fs;
        }

        @Override
        public Object[] getArgs() {
            return new Object[] {this.src, this.dst};
        }

        @Override
        public Signature getSignature() {
            return null;
        }

        @Override
        public SourceLocation getSourceLocation() {
            return null;
        }

        @Override
        public String getKind() {
            return null;
        }

        @Override
        public StaticPart getStaticPart() {
            return null;
        }
    }

    private class Listing implements ProceedingJoinPoint {
        private final FileSystem fs;
        private final Path path;

        public Listing(FileSystem fs, Path path) {
            this.fs = fs;
            this.path = path;
        }

        @Override
        public void set$AroundClosure(AroundClosure arc) {

        }

        @Override
        public Object proceed() throws Throwable {
            return this.fs.listStatus(this.path);
        }

        @Override
        public Object proceed(Object[] args) throws Throwable {
            return null;
        }

        @Override
        public String toShortString() {
            return null;
        }

        @Override
        public String toLongString() {
            return null;
        }

        @Override
        public Object getThis() {
            return this.fs;
        }

        @Override
        public Object getTarget() {
            return this.fs;
        }

        @Override
        public Object[] getArgs() {
            return new Object[]{this.path};
        }

        @Override
        public Signature getSignature() {
            return null;
        }

        @Override
        public SourceLocation getSourceLocation() {
            return null;
        }

        @Override
        public String getKind() {
            return null;
        }

        @Override
        public StaticPart getStaticPart() {
            return null;
        }
    }

    private class ConcreteListingAspect extends ConsistentListingAspect {

        @Override
        public void init() {

        }

        @Override
        public void create() {

        }

        @Override
        public void list() {

        }

        @Override
        public void rename() {

        }

        @Override
        public void delete() {

        }
    }

    @Test
    public void testPartialRead() throws Throwable {
        final URI uri = URI.create(testBucket);
        final Configuration noS3mperConf = new Configuration(conf);
        noS3mperConf.setBoolean("s3mper.disable", true);
        noS3mperConf.setBoolean("fs.gs.impl.disable.cache", true);
        // Pure filesystem without s3mper
        final FileSystem noS3mperFs = FileSystem.get(uri, noS3mperConf);

        final Path folder1 = new Path(testPath + "/rename/");
        final Path folder2 = new Path(testPath + "/rename2/");
        final Path file = new Path(folder1, "file.test");

        assertTrue(deleteFs.mkdirs(folder1));
        assertTrue(deleteFs.mkdirs(folder2));

        OutputStream fout = deleteFs.create(file);
        assertNotNull(fout);
        fout.close();

        final ConsistentListingAspect aspect = new ConcreteListingAspect();
        aspect.initialize(conf, uri);
        final BlockingRename blockingRename = new BlockingRename(noS3mperFs, folder1, folder2);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    // This rename will block until being notified after duplicating the metadata
                    assertTrue((Boolean) aspect.metastoreRename(blockingRename));
                    metadataDeleted = true;
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        };
        thread.start();

        while (!metadataCopied) {
            Thread.sleep(100);
        }

        final Listing listing = new Listing(noS3mperFs, folder2);
        boolean exception = false;
        try {
            // Data was not moved yet. Exception should be thrown.
            aspect.metastoreCheck(listing);
        } catch (S3ConsistencyException e){
            exception = true;
            synchronized (doRenameNotifier) {
                doRenameNotifier.notify();
            }
        }
        assertTrue(exception);

        while (!renameExecuted) {
            Thread.sleep(100);
        }

        final Listing rootListing = new Listing(noS3mperFs, testPath);
        exception = false;
        try {
            // Files have been moved by now. Listing the source parent should fail.
            aspect.metastoreCheck(rootListing);
        } catch (S3ConsistencyException e){
            exception = true;
            synchronized (finnishRenameNotifier) {
                finnishRenameNotifier.notify();
            }
        }
        assertTrue(exception);

        while (!metadataDeleted) {
            Thread.sleep(100);
        }

        // Rename completed successfully. Everything can be listed
        aspect.metastoreCheck(listing);
        aspect.metastoreCheck(rootListing);
    }
}
