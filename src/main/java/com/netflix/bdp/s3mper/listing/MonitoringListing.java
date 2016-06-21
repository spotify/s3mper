package com.netflix.bdp.s3mper.listing;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.RemoteMeter;
import com.spotify.metrics.core.RemoteSemanticMetricRegistry;
import com.spotify.metrics.core.RemoteTimer;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.SourceLocation;
import org.aspectj.runtime.internal.AroundClosure;

/**
 * A Listing implementation that composes simple monitoring
 * on top of an existing Listing implementation.
 */
public class MonitoringListing implements Listing {

    private static final Logger log =
            Logger.getLogger(MonitoringListing.class);

    private final Listing inner;

    private final RemoteSemanticMetricRegistry registry;

    private final MetricId baseId = MetricId.build("hadoop");
    private final MetricId meterBaseId = baseId.tagged(
            "what", "s3mper-listing-requests", "unit", "request");
    private final MetricId timerBaseId = baseId.tagged(
            "unit", "ns");

    public MonitoringListing(
            RemoteSemanticMetricRegistry registry, Listing inner) {
        log.info("Listing level monitoring enabled");
        this.registry = registry;
        this.inner = inner;
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
        return registry.timer(timerBaseId.tagged("what", "s3mper-listing-latency-" + action)).time();
    }

    @Override
    public Object metastoreUpdate(ProceedingJoinPoint pjp) throws Throwable {
        try {
            RemoteTimer.Context timer = timer("update");
            Object res = inner.metastoreUpdate(new MonitoredJoinPoint(pjp, "update"));
            timer.stop();
            okMeter("update").mark();
            return res;
        } catch (Exception e) {
            errorMeter("update", e.getClass().toString()).mark();
            throw e;
        }
    }

    @Override
    public Object metastoreCheck(ProceedingJoinPoint pjp) throws Throwable {
        try {
            RemoteTimer.Context timer = timer("check");
            Object res = inner.metastoreCheck(new MonitoredJoinPoint(pjp, "check"));
            timer.stop();
            okMeter("check").mark();
            return res;
        } catch (Exception e) {
            errorMeter("check", e.getClass().toString()).mark();
            throw e;
        }
    }

    @Override
    public Object metastoreRename(ProceedingJoinPoint pjp) throws Throwable {
        try {
            RemoteTimer.Context timer = timer("rename");
            Object res = inner.metastoreRename(new MonitoredJoinPoint(pjp, "rename"));
            timer.stop();
            okMeter("rename").mark();
            return res;
        } catch (Exception e) {
            errorMeter("rename", e.getClass().toString()).mark();
            throw e;
        }
    }

    @Override
    public Object metastoreDelete(ProceedingJoinPoint pjp) throws Throwable {
        try {
            RemoteTimer.Context timer = timer("delete");
            Object res = inner.metastoreDelete(new MonitoredJoinPoint(pjp, "delete"));
            timer.stop();
            okMeter("delete").mark();
            return res;
        } catch (Exception e) {
            errorMeter("delete", e.getClass().toString()).mark();
            throw e;
        }
    }

    private class MonitoredJoinPoint implements ProceedingJoinPoint {

        private final ProceedingJoinPoint inner;
        private final String action;
        private final MetricId pjpMeterBaseId = baseId.tagged(
                "what", "s3mper-listing-pjp-requests", "unit", "request");

        private MonitoredJoinPoint(ProceedingJoinPoint inner, String action) {
            this.inner = inner;
            this.action = action + "-pjp";
        }

        @VisibleForTesting
        RemoteMeter okPjpMeter(String action) {
            return registry.meter(pjpMeterBaseId.tagged(
                    "action", action, "result", "ok"));
        }

        @VisibleForTesting
        RemoteMeter errorPjpMeter(String action, String result) {
            return registry.meter(pjpMeterBaseId.tagged(
                    "action", action, "result", result));
        }

        @Override
        public void set$AroundClosure(AroundClosure arc) {
          inner.set$AroundClosure(arc);
        }

        @Override
        public Object proceed() throws Throwable {
            try {
                RemoteTimer.Context timer = timer(action);
                Object res = inner.proceed();
                timer.stop();
                okPjpMeter(action).mark();
                return res;
            } catch (Exception e) {
                errorPjpMeter(action, e.getClass().toString()).mark();
                throw e;
            }
        }

        @Override
        public Object proceed(Object[] args) throws Throwable {
            try {
                RemoteTimer.Context timer = timer(action);
                Object res = inner.proceed(args);
                timer.stop();
                okPjpMeter(action).mark();
                return res;
            } catch (Exception e) {
                errorPjpMeter(action, e.getClass().toString()).mark();
                throw e;
            }
        }

        @Override
        public String toShortString() {
            return inner.toShortString();
        }

        @Override
        public String toLongString() {
            return inner.toLongString();
        }

        @Override
        public Object getThis() {
            return inner.getThis();
        }

        @Override
        public Object getTarget() {
            return inner.getTarget();
        }

        @Override
        public Object[] getArgs() {
            return inner.getArgs();
        }

        @Override
        public Signature getSignature() {
            return inner.getSignature();
        }

        @Override
        public SourceLocation getSourceLocation() {
            return inner.getSourceLocation();
        }

        @Override
        public String getKind() {
            return inner.getKind();
        }

        @Override
        public StaticPart getStaticPart() {
            return getStaticPart();
        }
    }

}
