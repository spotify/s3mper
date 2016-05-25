package com.netflix.bdp.s3mper.listing;

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

    private class BlockingListing implements ProceedingJoinPoint {
        private final FileSystem fs;
        private final Path src;
        private final Path dst;

        public BlockingListing(FileSystem fs, Path src, Path dst) {
            this.fs = fs;
            this.src = src;
            this.dst = dst;
        }

        @Override
        public void set$AroundClosure(AroundClosure arc) {

        }

        @Override
        public Object proceed() throws Throwable {
            synchronized (this) {
                this.wait();
            }
            return true;
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
        final FileSystem fs = FileSystem.get(uri, conf);

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
        final BlockingListing blockingListing = new BlockingListing(fs, folder1, folder2);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    aspect.metastoreRename(blockingListing);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        };
        thread.start();

        Thread.sleep(5000);

        final Listing listing = new Listing(fs, folder2);
        boolean exception = false;
        try {
            aspect.metastoreCheck(listing);
        } catch (S3ConsistencyException e){
            exception = true;
            synchronized (blockingListing) {
                blockingListing.notify();
            }
        }
        assertTrue(exception);

        Thread.sleep(5000);

        aspect.metastoreCheck(listing);
    }
}
