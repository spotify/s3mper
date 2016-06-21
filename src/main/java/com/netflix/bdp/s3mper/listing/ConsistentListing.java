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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.netflix.bdp.s3mper.common.PathUtil.normalize;
import static java.lang.String.format;

/**
 * This class provides advice to the S3 hadoop FileSystem implementation and uses
 * a metastore for consistent listing.
 *
 * @author dweeks
 */
public class ConsistentListing implements Listing {
    private static final Logger log = Logger.getLogger(ConsistentListing.class.getName());

    private final ConsistentListingConfig config;
    private final FileSystemMetastore metastore;
    private final AlertDispatcher alertDispatcher;

    public ConsistentListing(ConsistentListingConfig config, FileSystemMetastore metastore, AlertDispatcher alertDispatcher) {
        this.config = config;
        this.metastore = metastore;
        this.alertDispatcher = alertDispatcher;
    }

    /**
     * Updates the metastore when a FileSystem.create(...) method is called.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    public Object metastoreUpdate(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();

        Object result = pjp.proceed();

        Path path = null;

        if (result instanceof Boolean && !((Boolean) result)) {
            return result;
        }

        try {
            //Locate the path parameter in the arguments
            for (Object arg : pjp.getArgs()) {
                if (arg instanceof Path) {
                    path = (Path) arg;
                    break;
                }
            }

            metastore.add(path, config.isTrackDirectories() && pjp.getSignature().getName().contains("mkdir"));
        } catch (TimeoutException t) {
            log.error("Timeout occurred adding path to metastore: " + path, t);

            alertDispatcher.timeout("metastoreUpdate", Collections.singletonList(path));

            if (config.isFailOnTimeout()) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Failed to add path to metastore: " + path, e);

            if (shouldFail(conf)) {
                throw e;
            }
        }

        return result;
    }

    /**
     * Ensures that all the entries in the metastore also exist in the FileSystem listing.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    public Object metastoreCheck(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        FileSystem fs = (FileSystem) pjp.getThis();

        FileStatus[] s3Listing = (FileStatus[]) pjp.proceed();
        FileStatus[] originalListing = null;
        if (config.isDarkload()) {
            originalListing = s3Listing.clone();
        }

        List<Path> pathsToCheck = new ArrayList<Path>();

        Object pathArg = pjp.getArgs()[0];

        //Locate paths in the arguments
        if (pathArg instanceof Path) {
            pathsToCheck.add((Path) pathArg);
        } else if (pathArg instanceof List) {
            pathsToCheck.addAll((List) pathArg);
        } else if (pathArg.getClass().isArray()) {
            pathsToCheck.addAll(Arrays.asList((Path[]) pathArg));
        }

        //HACK: This is just to prevent the emr metrics from causing consisteny failures
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
            if (e.getClassName().contains("emr.metrics")) {
                log.debug("Ignoring EMR metrics listing for paths: " + pathsToCheck);
                return s3Listing;
            }
        }
        //END HACK

        long recheck = config.getRecheckCount();
        long delay = config.getRecheckPeriod();

        try {
            if (isTask(conf) && !config.isCheckTaskListings()) {
                log.info("Skipping consistency check for task listing");
                return s3Listing;
            }

            if (isTask(conf)) {
                recheck = config.getTaskRecheckCount();
                delay = config.getTaskRecheckPeriod();
            }
        } catch (Exception e) {
            log.error("Error checking for task side listing", e);
        }

        try {
            List<FileInfo> metastoreListing = metastore.list(pathsToCheck);

            List<Path> missingPaths = ImmutableList.of();
            if (config.isStatOnMissingFile()) {
                missingPaths = checkListing(metastoreListing, s3Listing);

                if (!missingPaths.isEmpty()) {
                    List<FileStatus> fullListing = new ArrayList<FileStatus>();
                    fullListing.addAll(Arrays.asList(s3Listing));
                    for (Path path : missingPaths) {
                        FileStatus status = fs.getFileStatus(path);
                        fullListing.add(status);
                    }
                    s3Listing = fullListing.toArray(new FileStatus[0]);
                }
            } else {

                int checkAttempt;

                for (checkAttempt = 0; checkAttempt <= recheck; checkAttempt++) {
                    missingPaths = checkListing(metastoreListing, s3Listing);

                    if (config.isDelistDeleteMarkedFiles()) {
                        s3Listing = delistDeletedPaths(metastoreListing, s3Listing);
                    }

                    if (missingPaths.isEmpty()) {
                        break;
                    }

                    //Check if acceptable threshold of data has been met.  This is a little
                    //ambigious becuase S3 could potentially have more files than the
                    //metastore (via out-of-band access) and throw off the ratio
                    if (config.getFileThreshold() < 1 && metastoreListing.size() > 0) {
                        float ratio = s3Listing.length / (float) metastoreListing.size();

                        if (ratio > config.getFileThreshold()) {
                            log.info(
                                    format(
                                            "Proceeding with incomplete listing at ratio %f (%f as acceptable). Still missing paths: %s",
                                            ratio, config.getFileThreshold(), missingPaths));

                            missingPaths.clear();
                            break;
                        }
                    }

                    if (recheck == 0) {
                        break;
                    }

                    log.info(
                            format("Rechecking consistency in %d (ms).  Files missing %d. Missing paths: %s",
                                    delay, missingPaths.size(), missingPaths));
                    Thread.sleep(delay);
                    s3Listing = (FileStatus[]) pjp.proceed();
                }

                if (!missingPaths.isEmpty()) {
                    alertDispatcher.alert(missingPaths);

                    if (shouldFail(conf)) {
                        throw new S3ConsistencyException("Consistency check failed. See go/s3mper for details. Missing paths: " + missingPaths);
                    } else {
                        log.error("Consistency check failed.  See go/s3mper for details. Missing paths: " + missingPaths);
                    }
                } else {
                    if (checkAttempt > 0) {
                        log.info(format("Listing achieved consistency after %d attempts", checkAttempt));
                        alertDispatcher.recovered(pathsToCheck);
                    }
                }
            }
        } catch (TimeoutException t) {
            log.error("Timeout occurred listing metastore paths: " + pathsToCheck, t);

            alertDispatcher.timeout("metastoreCheck", pathsToCheck);

            if (config.isFailOnTimeout()) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Failed to list metastore for paths: " + pathsToCheck, e);

            if (shouldFail(conf)) {
                throw e;
            }
        }

        return config.isDarkload() ? originalListing : s3Listing;
    }

    /**
     * Check the the metastore listing against the s3 listing and return any paths
     * missing from s3.
     *
     * @param metastoreListing
     * @param s3Listing
     * @return
     */
    private List<Path> checkListing(List<FileInfo> metastoreListing, FileStatus[] s3Listing) {
        Map<String, FileStatus> s3paths = new HashMap<String, FileStatus>();

        if (s3Listing != null) {
            for (FileStatus fileStatus : s3Listing) {
                s3paths.put(fileStatus.getPath().toUri().normalize().getSchemeSpecificPart(), fileStatus);
            }
        }

        List<Path> missingPaths = new ArrayList<Path>();

        for (FileInfo f : metastoreListing) {
            if (f.isDeleted()) {
                continue;
            }
            if (!s3paths.containsKey(f.getPath().toUri().normalize().getSchemeSpecificPart())) {
                missingPaths.add(f.getPath());
            }
        }

        return missingPaths;
    }

    private FileStatus[] delistDeletedPaths(List<FileInfo> metastoreListing, FileStatus[] s3Listing) {
        if (s3Listing == null || s3Listing.length == 0) {
            return s3Listing;
        }

        Set<String> delistedPaths = new HashSet<String>();

        for (FileInfo file : metastoreListing) {
            if (file.isDeleted()) {
                delistedPaths.add(normalize(file.getPath()));
            }
        }

        List<FileStatus> s3files = Arrays.asList(s3Listing);

        for (Iterator<FileStatus> i = s3files.iterator(); i.hasNext(); ) {
            FileStatus file = i.next();

            if (delistedPaths.contains(normalize(file.getPath()))) {
                i.remove();
            }
        }

        return s3files.toArray(new FileStatus[s3files.size()]);
    }

    private static class RenameInfo {
        final Path srcPath;
        final Path dstPath;
        final boolean srcExists;
        final boolean dstExists;
        final boolean srcIsFile;
        final boolean dstIsFile;

        public RenameInfo(FileSystem fs, Path srcPath, Path dstPath) throws IOException {
            this.srcPath = srcPath;
            this.dstPath = dstPath;
            srcIsFile = fs.isFile(srcPath);
            dstIsFile = fs.isFile(dstPath);
            srcExists = fs.exists(srcPath);
            dstExists = fs.exists(dstPath);
        }
    }

    /**
     * Rename listing records based on a rename call from the FileSystem.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    public Object metastoreRename(final ProceedingJoinPoint pjp) throws Throwable {
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        FileSystem fs = (FileSystem) pjp.getTarget();

        Path srcPath = (Path) pjp.getArgs()[0];
        Path dstPath = (Path) pjp.getArgs()[1];

        Preconditions.checkNotNull(srcPath);
        Preconditions.checkNotNull(dstPath);

        RenameInfo renameInfo = new RenameInfo(fs, srcPath, dstPath);
        metadataRename(conf, fs, renameInfo);

        Object obj = pjp.proceed();
        if ((Boolean) obj) {
            // Everything went fine delete the old metadata.
            // If not then we'll keep the metadata to prevent incomplete listings.
            // Manual cleanup will be required in the case of failure.
            metadataCleanup(conf, fs, renameInfo);
        }

        return obj;
    }

    private void metadataRename(Configuration conf, FileSystem fs, RenameInfo info) throws Exception {
        try {
            final String error = "Unsupported move " + info.srcPath.toUri().getPath()
                    + " to " + info.dstPath.toUri().getPath() + ": ";

            if (info.srcPath.isRoot()) {
                throw new IOException(error + "Cannot rename root");
            }

            if (!info.srcExists) {
                throw new IOException(error + "Source does not exist");
            }

            if (!info.dstExists && !fs.exists(info.dstPath.getParent())) {
                throw new IOException(error + "Target parent does not exist");
            }

            if (info.dstExists && info.dstIsFile) {
                throw new IOException(error + "Target file exists");
            }

            if (info.dstExists) {
                Path actualDst = new Path(info.dstPath, info.srcPath.getName());
                if (info.srcIsFile) {
                    renameFile(info.srcPath, actualDst);
                } else {
                    renameFolder(info.srcPath, actualDst);
                }
            } else {
                if (info.srcIsFile) {
                    renameFile(info.srcPath, info.dstPath);
                } else {
                    renameFolder(info.srcPath, info.dstPath);
                }
            }
        } catch (TimeoutException t) {
            log.error("Timeout occurred rename metastore path: " + info.srcPath, t);

            alertDispatcher.timeout("metastoreRename", Collections.singletonList(info.srcPath));

            if (config.isFailOnTimeout()) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Error rename paths from metastore: " + info.srcPath, e);

            if (shouldFail(conf)) {
                throw e;
            }
        }
    }

    private void renameFile(Path src, Path dst) throws Exception {
        metastore.add(dst, false);
    }

    private void renameFolder(Path src, Path dst) throws Exception {
        metastore.add(dst, true);
        List<FileInfo> metastoreFiles = metastore.list(Collections.singletonList(src));
        for (FileInfo info : metastoreFiles) {
            Path target = new Path(dst, info.getPath().getName());
            if (info.isDirectory()) {
                renameFolder(info.getPath(), target);
            } else {
                renameFile(info.getPath(), target);
            }
        }
    }

    private void metadataCleanup(Configuration conf, FileSystem fs, RenameInfo info) throws Exception {
        try {
            renameCleanup(fs, new FileInfo(info.srcPath, false, !info.srcIsFile));
        } catch (TimeoutException t) {
            log.error("Timeout occurred rename cleanup metastore path: " + info.srcPath, t);

            alertDispatcher.timeout("metastoreRenameCleanup", Collections.singletonList(info.srcPath));

            if (config.isFailOnTimeout()) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Error executing rename cleanup for paths from metastore: " + info.srcPath, e);

            if (shouldFail(conf)) {
                throw e;
            }
        }
    }

    private void renameCleanup(FileSystem fs, FileInfo info) throws Exception {
        if (!info.isDirectory()) {
            metastore.delete(info.getPath());
            return;
        }

        List<FileInfo> metastoreFiles = metastore.list(Collections.singletonList(info.getPath()));
        for (FileInfo fileInfo : metastoreFiles) {
            renameCleanup(fs, fileInfo);
        }
        metastore.delete(info.getPath());
    }

    /**
     * Deletes listing records based on a delete call from the FileSystem.
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    @Override
    public Object metastoreDelete(final ProceedingJoinPoint pjp) throws Throwable {

        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();

        Path deletePath = (Path) pjp.getArgs()[0];

        boolean recursive = false;

        if (pjp.getArgs().length > 1) {
            recursive = (Boolean) pjp.getArgs()[1];
        }

        try {
            FileSystem s3fs = (FileSystem) pjp.getTarget();

            Set<Path> filesToDelete = new HashSet<Path>();
            filesToDelete.add(deletePath);

            List<FileInfo> metastoreFiles = metastore.list(Collections.singletonList(deletePath));

            for (FileInfo f : metastoreFiles) {
                filesToDelete.add(f.getPath());
            }

            try {
                if (s3fs.getFileStatus(deletePath).isDir() && recursive) {
                    filesToDelete.addAll(recursiveList(s3fs, deletePath));
                }
            } catch (Exception e) {
                log.info("A problem occurred deleting path: " + deletePath + " " + e.getMessage());
            }

            for (Path path : filesToDelete) {
                metastore.delete(path);
            }
        } catch (TimeoutException t) {
            log.error("Timeout occurred deleting metastore path: " + deletePath, t);

            alertDispatcher.timeout("metastoreDelete", Collections.singletonList(deletePath));

            if (config.isFailOnTimeout()) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Error deleting paths from metastore: " + deletePath, e);

            if (shouldFail(conf)) {
                throw e;
            }
        }

        return pjp.proceed();
    }

    private List<Path> recursiveList(FileSystem fs, Path path) throws IOException {
        List<Path> result = new ArrayList<Path>();

        try {
            result.add(path);

            if (!fs.isFile(path)) {
                FileStatus[] children = fs.listStatus(path);

                if (children == null) {
                    return result;
                }

                for (FileStatus child : children) {
                    if (child.isDir()) {
                        result.addAll(recursiveList(fs, child.getPath()));
                    } else {
                        result.add(child.getPath());
                    }
                }
            }
        } catch (Exception e) {
            log.info("A problem occurred recursively deleting path: " + path + " " + e.getMessage());
        }

        return result;
    }

    /**
     * Check to see if the current context is within an executing task.
     *
     * @param conf
     * @return
     */
    private boolean isTask(Configuration conf) {
        return conf.get("mapred.task.id") != null;
    }

    /**
     * Handles the various options for when failure should occur.
     *
     * @param conf
     * @return
     */
    private boolean shouldFail(Configuration conf) {
        boolean isTask = isTask(conf);

        return (!isTask && config.isFailOnError()) || (isTask && config.isTaskFailOnError());
    }

}
