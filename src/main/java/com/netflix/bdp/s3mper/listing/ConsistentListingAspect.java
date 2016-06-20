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

import com.spotify.metrics.remote.SemanticAggregatorMetricRegistry;
import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

/**
 * This class does the AspectJ swizzling but defers all actual
 * work to an underlying Listing implementation.
 *
 * @author dweeks
 */
@Aspect
public abstract class ConsistentListingAspect implements Listing {
    private static final Logger log = Logger.getLogger(ConsistentListingAspect.class.getName());
    
    Listing listing = new MonitoringListing(
            new SemanticAggregatorMetricRegistry(
                    "http://semigator.services.lon3.spotify.net", 20000, 5, 1000),
            new ConsistentListing());

    @Pointcut
    public abstract void init();
    /**
     * Creates the metastore on initialization.
     * 
     * #TODO The metastore isn't created instantly by DynamoDB.  This should wait until
     * the initialization is complete.  If the store doesn't exist, calls will fail until
     * it is created.
     * 
     * @param jp
     * @throws Exception  
     */
    @Override
    @Before("init()")
    public synchronized void initialize(JoinPoint jp) throws Exception {
        listing.initialize(jp);
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
        return listing.metastoreDelete(pjp);
    }
}
