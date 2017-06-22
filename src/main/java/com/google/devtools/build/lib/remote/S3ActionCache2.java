// Copyright 2016 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.remote;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.remote.RemoteProtocol.ContentDigest;
import com.google.devtools.build.lib.vfs.Path;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

/**
 * xcxc
 */
@ThreadSafe
public final class S3ActionCache2 {
    private final String bucketName;
    private final boolean debug;
    private String bucketOwner;

    private static volatile int numConsecutiveErrors;
    private static volatile long disableUntilTimeMillis;

    // xcxc add retry wrappers ...
    private static final AmazonS3 client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

    /**
     * Construct an action cache using JCache API.
     */
    public S3ActionCache2(RemoteOptions options) {
        this.bucketName = options.s3CacheBucket;
        this.debug = options.remoteCacheDebug;
    }

    private void recordCacheFailedOperation(Exception e) {
        final int MAX_CONSECUTIVE_ERRORS = 10;
        final int MINUTES_DISABLE_CACHE = 5;

        if (e instanceof com.amazonaws.AbortedException) {
            return; // usually this is because the user pressed ctrl-c or something
        }

        if (isCacheEnabled()) {
            System.err.println("S3 cache: " + e.toString());
            ++numConsecutiveErrors;
            if (numConsecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                System.err.println("S3 cache encountered multiple consecutive errors; disabling cache for " + MINUTES_DISABLE_CACHE + " minutes.");
                disableUntilTimeMillis = System.currentTimeMillis() + MINUTES_DISABLE_CACHE * 60 * 1000;
                numConsecutiveErrors = 0;
            }
        }
    }

    private void recordCacheSuccessfulOperation() {
        numConsecutiveErrors = 0;
    }

    private boolean isCacheEnabled() {
        if (disableUntilTimeMillis != 0) {
            if (System.currentTimeMillis() > disableUntilTimeMillis) {
                disableUntilTimeMillis = 0;
                numConsecutiveErrors = 0;
            }
        }
        return disableUntilTimeMillis == 0;
    }

    private static interface GetObject<T> {
        public T getObject() throws AmazonClientException, IOException;
    }

    public Path getFile(final String key, final Path dest, ContentDigest digest) throws CacheNotFoundException {
        Path result = getObject(key, dest.toString(), () -> {
            client.getObject(new GetObjectRequest(bucketName, key), dest.getPathFile());
            return dest;
        });
        if (result == null) {
            throw new CacheNotFoundException(digest);
        }
        return result;
    }

    public byte[] get(String key) {
        return getObject(key, null, () -> {
            S3Object obj = client.getObject(new GetObjectRequest(bucketName, key));
            InputStream stream = obj.getObjectContent();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[16384];

            while ((nRead = stream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            buffer.flush();
            stream.close();
            return buffer.toByteArray();
        });
    }

    private <T> T getObject(String key, String path, GetObject<T> get) {
        if (path == null) path = "";

        if (!isCacheEnabled()) {
            return null;
        }

        long t0 = System.currentTimeMillis();
        try {
            T t = get.getObject();
            recordCacheSuccessfulOperation();
            if (debug)
                System.err.println("S3 Cache Download: " + " key:" + key + " " + path + " (" + (System.currentTimeMillis() - t0) + "ms)");
            return t;
        }
        catch (AmazonClientException e) {
            // Some sort of Amazon error; degrade gracefully, assume we couldn't find the file
            String exceptionMessage;
            if (e instanceof AmazonS3Exception && ((AmazonS3Exception)e).getStatusCode() == 404 && "NoSuchKey".equals(((AmazonS3Exception)e).getErrorCode())) {
                exceptionMessage = "";
                recordCacheSuccessfulOperation(); // 404 is not a failed operation, it's normal
            } else {
                exceptionMessage = ": " + e.toString();
                recordCacheFailedOperation(e);
            }

            if (debug) {
                System.err.println("S3 Cache key not found key:"+ key +" " + path + " (" + (System.currentTimeMillis() - t0) + "ms)" + exceptionMessage);
            }
            return null;
        }
        catch (IOException e) {
            if (debug)
                System.err.println("S3 Cache IO Exception downloading key:" + key);
            recordCacheFailedOperation(e);
            return null;
        }
    }

    public boolean containsKey(String key) {
        if (!isCacheEnabled()) {
            return false;
        }

        boolean doesObjectExist;

        try {
            doesObjectExist = client.doesObjectExist(bucketName, key);

            if (doesObjectExist) {
                recordCacheSuccessfulOperation();
            } else {
                // If doesObjectExist() returned false, don't record a successful operation
                // or a failed operation. For example, it may have returned false if we passed
                // a bad bucket name.
            }
        } catch (AmazonClientException e) {
            // Some sort of Amazon error; degrade gracefully, assume we couldn't find the key
            doesObjectExist = false;
            recordCacheFailedOperation(e);
        }

        return doesObjectExist;
    }

    public void putFile(String key, Path file) {
        putObject(key, new PutObjectRequest(bucketName, key, file.getPathFile()));
    }

    public void put(String key, byte[] blob) {
        putObject(key, new PutObjectRequest(bucketName, key, new ByteArrayInputStream(blob), new ObjectMetadata()));
    }

    private synchronized String getBucketOwner() {
        if (bucketOwner == null) {
            try {
                bucketOwner = client.getBucketAcl(bucketName).getOwner().getId();
                recordCacheSuccessfulOperation();
            } catch (AmazonClientException e) {
                recordCacheFailedOperation(e);
            }
        }
        return bucketOwner;
    }

    private void putObject(String key, PutObjectRequest object) {
        if (!isCacheEnabled()) {
            return;
        }

        long t0 = System.currentTimeMillis();
        try {
            // Make sure the bucket owner has full control of the uploaded object
            Grantee grantee = new CanonicalGrantee(getBucketOwner());
            AccessControlList acl = new AccessControlList();
            acl.grantPermission(grantee, Permission.FullControl);
            object = object.withAccessControlList(acl);

            client.putObject(object);
            if (debug) {
                System.err.println("S3 Cache Upload: key:" + key + "  (" + (System.currentTimeMillis() - t0) + "ms)");
            }
            recordCacheSuccessfulOperation();
        } catch (AmazonClientException e) {
            if (debug) {
                System.err.println("S3 Cache Upload failed key:"+ key +" (" + (System.currentTimeMillis() - t0) + "ms): " + e.toString());
            }
            recordCacheFailedOperation(e);
        }
    }
}
