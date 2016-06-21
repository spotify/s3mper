package com.netflix.bdp.s3mper.listing;

import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.RemoteMeter;
import com.spotify.metrics.core.RemoteSemanticMetricRegistry;
import com.spotify.metrics.core.RemoteTimer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MonitoringListingTest {

    @Mock
    Listing inner;

    @Mock
    RemoteSemanticMetricRegistry registry;

    @Mock
    ProceedingJoinPoint pjp;

    @Mock
    RemoteMeter meter;

    @Mock
    RemoteTimer timer;

    @Mock
    RemoteTimer.Context timerContext;

    Listing listing;

    @Before
    public void setUp() throws Exception {
        listing = spy(new MonitoringListing(registry, inner));
        when(registry.meter(any(MetricId.class))).thenReturn(meter);
        when(registry.timer(any(MetricId.class))).thenReturn(timer);
        when(timer.time()).thenReturn(timerContext);
    }

    @Test
    public void testOkUpdate() throws Throwable {
        listing.metastoreUpdate(pjp);
        verify(inner, times(1)).metastoreUpdate(pjp);
        verify(meter, times(1)).mark();
        verify(timerContext, times(1)).stop();
    }

    @Test
    public void testOkCheck() throws Throwable {
        listing.metastoreCheck(pjp);
        verify(inner, times(1)).metastoreCheck(pjp);
        verify(meter, times(1)).mark();
        verify(timerContext, times(1)).stop();
    }

    @Test
    public void testOkRename() throws Throwable {
        listing.metastoreRename(pjp);
        verify(inner, times(1)).metastoreRename(pjp);
        verify(meter, times(1)).mark();
        verify(timerContext, times(1)).stop();
    }

    @Test
    public void testOkDelete() throws Throwable {
        listing.metastoreDelete(pjp);
        verify(inner, times(1)).metastoreDelete(pjp);
        verify(meter, times(1)).mark();
        verify(timerContext, times(1)).stop();
    }

    @Test
    public void testFailedUpdate() throws Throwable {
        Exception exc = new Exception();
        when(inner.metastoreUpdate(pjp)).thenThrow(exc);
        try {
            listing.metastoreUpdate(pjp);
        } catch (Exception e) {
            assertEquals(exc, e);
            verify(inner, times(1)).metastoreUpdate(pjp);
            verify(meter, times(1)).mark();
            verify(timerContext, times(0)).stop();
            return;
        }
        assert(false);
    }

    @Test
    public void testFailedCheck() throws Throwable {
        Exception exc = new Exception();
        when(inner.metastoreCheck(pjp)).thenThrow(exc);
        try {
            listing.metastoreCheck(pjp);
        } catch (Exception e) {
            assertEquals(exc, e);
            verify(inner, times(1)).metastoreCheck(pjp);
            verify(meter, times(1)).mark();
            verify(timerContext, times(0)).stop();
            return;
        }
        assert(false);
    }

    @Test
    public void testFailedRename() throws Throwable {
        Exception exc = new Exception();
        when(inner.metastoreRename(pjp)).thenThrow(exc);
        try {
            listing.metastoreRename(pjp);
        } catch (Exception e) {
            assertEquals(exc, e);
            verify(inner, times(1)).metastoreRename(pjp);
            verify(meter, times(1)).mark();
            verify(timerContext, times(0)).stop();
            return;
        }
        assert(false);
    }

    @Test
    public void testFailedDelete() throws Throwable {
        Exception exc = new Exception();
        when(inner.metastoreDelete(pjp)).thenThrow(exc);
        try {
            listing.metastoreDelete(pjp);
        } catch (Exception e) {
            assertEquals(exc, e);
            verify(inner, times(1)).metastoreDelete(pjp);
            verify(meter, times(1)).mark();
            verify(timerContext, times(0)).stop();
            return;
        }
        assert(false);
    }
}
