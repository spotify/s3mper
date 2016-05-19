package com.netflix.bdp.s3mper.alert.impl;

import com.codahale.metrics.Histogram;
import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.core.MetricId;
import com.codahale.metrics.Meter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.List;

public class SemanticMetricsDispatcher implements AlertDispatcher{

    private final SemanticMetricRegistry registry;

    private final Meter recoverMeter;
    private final Histogram recoverHistogram;
    private final Meter timeoutMeter;

    public SemanticMetricsDispatcher(SemanticMetricRegistry registry) {
        this.registry = registry;
        this.recoverMeter = registry.meter(MetricId.build().tagged(
                "what", "missed-rate"));
        this.recoverHistogram = registry.histogram(MetricId.build().tagged(
                "what", "missed-count"));
        this.timeoutMeter = registry.meter(MetricId.build().tagged(
                "what", "timeout-rate"));
    }


    @Override
    public void alert(List<Path> paths) {

    }

    @Override
    public void timeout(String operation, List<Path> paths) {
        this.timeoutMeter.mark();
    }

    @Override
    public void init(URI uri, Configuration conf) {

    }

    @Override
    public void setConfig(Configuration conf) {

    }

    @Override
    public void recovered(List<Path> paths) {
        recoverMeter.mark();
        recoverHistogram.update(paths.size());
    }
}
