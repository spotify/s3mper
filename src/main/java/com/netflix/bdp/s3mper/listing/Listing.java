package com.netflix.bdp.s3mper.listing;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;

/**
 * All operations that we need to intercept for consistency.
 */
public interface Listing {
    void initialize(JoinPoint jp) throws Exception;
    Object metastoreUpdate(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreCheck(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreRename(ProceedingJoinPoint pjp) throws Throwable;
    Object metastoreDelete(ProceedingJoinPoint pjp) throws Throwable;
}
