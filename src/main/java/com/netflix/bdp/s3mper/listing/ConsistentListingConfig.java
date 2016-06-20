package com.netflix.bdp.s3mper.listing;

import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Track S3mper configuration
 *
 * @author liljencrantz@spotify.com
 */
public class ConsistentListingConfig {

    private static final Logger log =
            Logger.getLogger(ConsistentListingConfig.class);

    private FileSystemMetastore metastore = null;
    private AlertDispatcher alertDispatcher = null;

    private boolean disabled = true;
    private boolean monitoring = Boolean.getBoolean("s3mper.monitoring");

    private boolean darkload = Boolean.getBoolean("s3mper.darkload");
    private boolean failOnError = Boolean.getBoolean("s3mper.failOnError");
    private boolean taskFailOnError = Boolean.getBoolean("s3mper.task.failOnError");
    private boolean checkTaskListings = Boolean.getBoolean("s3mper.listing.task.check");
    private boolean failOnTimeout = Boolean.getBoolean("s3mper.failOnTimeout");
    private boolean trackDirectories = Boolean.getBoolean("s3mper.listing.directory.tracking");
    private boolean delistDeleteMarkedFiles = true;

    private float fileThreshold = 1;

    private long recheckCount = Long.getLong("s3mper.listing.recheck.count", 15);
    private long recheckPeriod = Long.getLong("s3mper.listing.recheck.period", TimeUnit.MINUTES.toMillis(1));
    private long taskRecheckCount = Long.getLong("s3mper.listing.task.recheck.count", 0);
    private long taskRecheckPeriod = Long.getLong("s3mper.listing.task.recheck.period", TimeUnit.MINUTES.toMillis(1));
    private boolean statOnMissingFile = Boolean.getBoolean("s3mper.listing.statOnMissingFile");

    public void updateConfig(Configuration conf) {
        disabled = conf.getBoolean("s3mper.disable", disabled);

        if (disabled) {
            log.warn("S3mper Consistency explicitly disabled.");
            return;
        }

        monitoring = conf.getBoolean("s3mper.monitoring", monitoring);
        darkload = conf.getBoolean("s3mper.darkload", darkload);
        failOnError = conf.getBoolean("s3mper.failOnError", failOnError);
        taskFailOnError = conf.getBoolean("s3mper.task.failOnError", taskFailOnError);
        checkTaskListings = conf.getBoolean("s3mper.listing.task.check", checkTaskListings);
        failOnTimeout = conf.getBoolean("s3mper.failOnTimeout", failOnTimeout);
        delistDeleteMarkedFiles = conf.getBoolean("s3mper.listing.delist.deleted", delistDeleteMarkedFiles);
        trackDirectories = conf.getBoolean("s3mper.listing.directory.tracking", trackDirectories);

        fileThreshold = conf.getFloat("s3mper.listing.threshold", fileThreshold);

        recheckCount = conf.getLong("s3mper.listing.recheck.count", recheckCount);
        recheckPeriod = conf.getLong("s3mper.listing.recheck.period", recheckPeriod);
        taskRecheckCount = conf.getLong("s3mper.listing.task.recheck.count", taskRecheckCount);
        taskRecheckPeriod = conf.getLong("s3mper.listing.task.recheck.period", taskRecheckPeriod);

        statOnMissingFile = conf.getBoolean("s3mper.listing.statOnMissingFile", false);
    }

    /**
     * Disables listing.  Once this is set, it cannot be re-enabled through
     * the configuration object.
     */
    public void disable() {
        log.warn("Disabling s3mper listing consistency.");
        disabled = true;
    }

    public FileSystemMetastore getMetastore() {
        return metastore;
    }

    public AlertDispatcher getAlertDispatcher() {
        return alertDispatcher;
    }

    public boolean isDarkload() {
        return darkload;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public boolean isTaskFailOnError() {
        return taskFailOnError;
    }

    public boolean isCheckTaskListings() {
        return checkTaskListings;
    }

    public boolean isFailOnTimeout() {
        return failOnTimeout;
    }

    public boolean isTrackDirectories() {
        return trackDirectories;
    }

    public boolean isDelistDeleteMarkedFiles() {
        return delistDeleteMarkedFiles;
    }

    public float getFileThreshold() {
        return fileThreshold;
    }

    public long getRecheckCount() {
        return recheckCount;
    }

    public long getRecheckPeriod() {
        return recheckPeriod;
    }

    public long getTaskRecheckCount() {
        return taskRecheckCount;
    }

    public long getTaskRecheckPeriod() {
        return taskRecheckPeriod;
    }

    public boolean isStatOnMissingFile() {
        return statOnMissingFile;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isMonitoring() {
        return monitoring;
    }
}
