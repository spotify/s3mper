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

import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.netflix.bdp.s3mper.metastore.Metastore;
import com.netflix.bdp.s3mper.metastore.impl.MonitoringMetastore;
import com.spotify.metrics.core.RemoteSemanticMetricRegistry;
import com.spotify.metrics.remote.SemanticAggregatorMetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import java.net.URI;

/**
 * This class does the AspectJ swizzling but defers all actual
 * work to an underlying Listing implementation.
 *
 * @author dweeks
 */
@Aspect
public abstract class ConsistentListingAspect implements Listing {
    private FileSystemMetastore metastore = null;
    private AlertDispatcher alertDispatcher = null;

    private static final Logger log =
            Logger.getLogger(ConsistentListingAspect.class.getName());

    private RemoteSemanticMetricRegistry registry;
    private Listing listing;
    private final ConsistentListingConfig config = new ConsistentListingConfig();

    @Pointcut
    public abstract void init();

    /**
     * Creates the metastore on initialization.
     * <p>
     * #TODO The metastore isn't created instantly by DynamoDB.  This should wait until
     * the initialization is complete.  If the store doesn't exist, calls will fail until
     * it is created.
     *
     * @param jp
     * @throws Exception
     */
    @Before("init()")
    public synchronized void initialize(JoinPoint jp) throws Exception {

        URI uri = (URI) jp.getArgs()[0];
        Configuration conf = (Configuration) jp.getArgs()[1];

        config.updateConfig(conf);

        registry = null;
        if (config.isMonitoring()) {
            registry =
                    new SemanticAggregatorMetricRegistry(
                            config.getMonitoringHost(),
                            config.getMonitoringPort(),
                            config.getMonitoringConcurrency(),
                            config.getMonitoringHighWaterMark());
        }

        if (metastore == null) {
            log.debug("Initializing S3mper Metastore");

            //FIXME: This is defaulted to the dynamodb metastore impl, but shouldn't
            //       reference it directly like this.
            Class<?> metaImpl = conf.getClass("s3mper.metastore.impl", com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore.class);

            FileSystemMetastore m;

            try {
                m = Metastore.getFilesystemMetastore(conf);
                m.initalize(uri, conf);
                if (config.isMonitoring()) {
                    metastore = new MonitoringMetastore(m, registry);
                } else {
                    metastore = m;
                }
            } catch (Exception e) {
                config.disable();

                if (config.isFailOnError()) {
                    throw e;
                }
            }

        } else {
            log.debug("S3mper Metastore already initialized.");
        }

        if (alertDispatcher == null) {
            log.debug("Initializing Alert Dispatcher");

            try {
                Class<?> dispatcherImpl =
                        conf.getClass(
                                "s3mper.dispatcher.impl",
                                com.netflix.bdp.s3mper.alert.impl.CloudWatchAlertDispatcher.class);

                alertDispatcher = (AlertDispatcher) ReflectionUtils.newInstance(dispatcherImpl, conf);
                alertDispatcher.init(uri, conf);
            } catch (Exception e) {
                log.error("Error initializing s3mper alert dispatcher", e);

                config.disable();

                if (config.isFailOnError()) {
                    throw e;
                }
            }
        } else {
            alertDispatcher.setConfig(conf);
        }

        //Check again after updating configs
        if (config.isDisabled()) {
            return;
        }

        Listing l = new ConsistentListing(config, metastore, alertDispatcher);

        if (config.isMonitoring()) {
            listing = new MonitoringListing(
                    registry,
                    l);
        } else {
            listing = l;
        }
    }

    @Pointcut
    public abstract void create();

    /**
     * Updates the metastore when a FileSystem.create(...) method is called.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    @Around("create() && !within(ConsistentListingAspect)")
    public Object metastoreUpdate(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        config.updateConfig(conf);

        if (config.isDisabled()) {
            return pjp.proceed();
        }

        return listing.metastoreUpdate(pjp);
    }

    @Pointcut
    public abstract void list();

    /**
     * Ensures that all the entries in the metastore also exist in the FileSystem listing.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    @Around("list() && !cflow(delete()) && !within(ConsistentListingAspect)")
    public Object metastoreCheck(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        config.updateConfig(conf);

        if (config.isDisabled()) {
            return pjp.proceed();
        }

        return listing.metastoreCheck(pjp);
    }

    @Pointcut
    public abstract void rename();

    /**
     * Rename listing records based on a rename call from the FileSystem.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    @Around("rename() && !within(ConsistentListingAspect)")
    public Object metastoreRename(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        config.updateConfig(conf);

        if (config.isDisabled()) {
            return pjp.proceed();
        }

        return listing.metastoreRename(pjp);
    }

    @Pointcut
    public abstract void delete();

    /**
     * Deletes listing records based on a delete call from the FileSystem.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    @Around("delete() && !within(ConsistentListingAspect)")
    public Object metastoreDelete(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        config.updateConfig(conf);

        if (config.isDisabled()) {
            return pjp.proceed();
        }

        return listing.metastoreDelete(pjp);
    }

}
