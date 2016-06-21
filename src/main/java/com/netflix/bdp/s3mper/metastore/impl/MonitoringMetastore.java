package com.netflix.bdp.s3mper.metastore.impl;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.RemoteMeter;
import com.spotify.metrics.core.RemoteSemanticMetricRegistry;
import com.spotify.metrics.core.RemoteTimer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * WIP
 *
 * Add monitoring via semantic metrics to an arbitrary FilesystemMetastore
 * implementation.
 *
 * @author liljencrantz@spotify.com
 */
public class MonitoringMetastore implements FileSystemMetastore{

    private static final Logger log = Logger.getLogger(MonitoringMetastore.class);

    private final FileSystemMetastore inner;
    private final RemoteSemanticMetricRegistry registry;

    private final MetricId baseId = MetricId.build("hadoop");
    private final MetricId meterBaseId = baseId.tagged(
            "what", "s3mper-metastore-requests", "unit", "request");
    private final MetricId timerBaseId = baseId.tagged(
            "unit", "ns");

    public MonitoringMetastore(FileSystemMetastore inner, RemoteSemanticMetricRegistry registry) {
        log.info("Metastore level monitoring enabled");
        this.inner = inner;
        this.registry = registry;
    }

    @VisibleForTesting
    RemoteMeter okMeter(String action) {
        return registry.meter(meterBaseId.tagged(
                "action", action, "result", "ok"));
    }

    @VisibleForTesting
    RemoteMeter errorMeter(String action, String result) {
        return registry.meter(meterBaseId.tagged(
                "action", action, "result", result));
    }

    @VisibleForTesting
    RemoteTimer.Context timer(String action) {
        return registry.timer(timerBaseId.tagged("what", "s3mper-metastore-latency-" + action)).time();
    }

    @Override
    public void initalize(final URI uri, final Configuration conf) throws Exception {
        new MonitoredAction<List<FileInfo>>("initialize", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                inner.initalize(uri, conf);
                return null;
            }
        }).run();

    }

    @Override
    public List<FileInfo> list(final List<Path> path) throws Exception {
        return new MonitoredAction<List<FileInfo>>("list", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                return inner.list(path);
            }
        }).run();
    }

    @Override
    public void add(final List<FileInfo> paths) throws Exception {
        new MonitoredAction<List<FileInfo>>("add", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                inner.add(paths);
                return null;
            }
        }).run();
    }

    @Override
    public void add(final Path path, final boolean directory) throws Exception {
        new MonitoredAction<List<FileInfo>>("add", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                inner.add(path, directory);
                return null;
            }
        }).run();
    }

    @Override
    public void delete(final Path path) throws Exception {
        new MonitoredAction<List<FileInfo>>("delete", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                inner.delete(path);
                return null;
            }
        }).run();
    }

    @Override
    public void delete(final List<Path> paths) throws Exception {
        new MonitoredAction<List<FileInfo>>("delete", new Callable<List<FileInfo>>(){
            @Override
            public List<FileInfo> call() throws Exception {
                inner.delete(paths);
                return null;
            }
        }).run();
    }

    @Override
    public void close() {
        try {
            new MonitoredAction<List<FileInfo>>("close", new Callable<List<FileInfo>>(){
                @Override
                public List<FileInfo> call() {
                    inner.close();
                    return null;
                }
            }).run();
        } catch (Exception e) {
            log.error("Ooops", e);
        }
    }

    @Override
    public int getTimeout() {
        return inner.getTimeout();
    }

    @Override
    public void setTimeout(int timeout) {
        inner.setTimeout(timeout);
    }

    private class MonitoredAction<T>{
        private final String name;
        private final Callable<T> action;

        private MonitoredAction(String name, Callable<T> action) {
            this.name = name;
            this.action = action;
        }

        public T run() throws Exception {
            try {
                RemoteTimer.Context timer = timer(name);
                T res = action.call();
                timer.stop();
                okMeter(name).mark();
                return res;
            } catch (Exception e) {
                errorMeter(name, e.getMessage()).mark();
                throw e;
            }
        }
    }
}
