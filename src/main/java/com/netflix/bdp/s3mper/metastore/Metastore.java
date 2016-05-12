package com.netflix.bdp.s3mper.metastore;

public class Metastore {

  public static FileSystemMetastore getFilesystemMetastore() {
    return null;//new DynamoDBMetastore();
  }

}
