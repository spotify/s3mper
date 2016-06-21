package com.netflix.bdp.s3mper.listing;

import org.aspectj.lang.ProceedingJoinPoint;

/**
 * All operations that we need to intercept for consistency.
 *
 * @author liljencrantz@spotify.com
 */
public interface Listing {
    Object metastoreUpdate(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreCheck(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreRename(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreDelete(ProceedingJoinPoint pjp) throws Throwable;
}
