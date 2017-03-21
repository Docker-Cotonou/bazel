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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputFileCache;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.remote.RemoteProtocol.ContentDigest;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.util.Collection;

/**
 * xcxc
 */
@ThreadSafe
public final class S3ActionCache2 {
    private final String bucketName;
    private final boolean debug;

    // xcxc add retry wrappers ...
    private final AmazonS3 client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

    /**
     * Construct an action cache using JCache API.
     */
    public S3ActionCache2(RemoteOptions options) {
        this.bucketName = options.s3CacheBucket;
        this.debug = options.remoteCacheDebug;
    }

    public Path getFile(String key, Path dest, ContentDigest digest) throws CacheNotFoundException {
        long t0 = System.currentTimeMillis();
        try {
            if (debug) {
                System.err.println("Attempting to download key " + key + " for dest " + dest);
            }

            client.getObject(new GetObjectRequest(bucketName, key), dest.getPathFile());
            if (debug)
                System.err.println("S3 Cache Download: " + " key:" + key + " (" + (System.currentTimeMillis() - t0) + "ms)");
            return dest;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (debug)
                    System.err.println("S3 key not found key:" + key + " " + " (" + (System.currentTimeMillis() - t0) + "ms)");
                throw new CacheNotFoundException(digest);
            }
            throw e;
        } catch (Exception e) {
            if (debug) {
                System.err.println("ERROR: Downloading " + dest + " " + key);
            }
            throw e;
        }
    }

    public byte[] get(String key) {
        long t0 = System.currentTimeMillis();
        try {
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

            if (debug)
                System.err.println("S3 Cache Download: " + " key:" + key + " (" + (System.currentTimeMillis() - t0) + "ms)");
            return buffer.toByteArray();
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (debug)
                    System.err.println("S3 key not found key:" + key + " " + " (" + (System.currentTimeMillis() - t0) + "ms)");

                return null;
            }
            throw e;
        } catch (IOException e) {
            // Tyler not sure if we should return null or raise exception
            if (debug)
                System.err.println("IO Exception downloading key" + key);
            return null;
        }
    }

    public boolean containsKey(String key) {
        return client.doesObjectExist(bucketName, key);
    }

    public void putFile(String key, Path file) {
        long t0 = System.currentTimeMillis();
        client.putObject(new PutObjectRequest(bucketName, key, file.getPathFile()));
        if (debug) {
            System.err.println("S3 Cache Upload: key:" + key + "  (" + (System.currentTimeMillis() - t0) + "ms)");
        }
    }

    public void put(String key, byte[] blob) {
        long t0 = System.currentTimeMillis();
        client.putObject(new PutObjectRequest(bucketName, key, new ByteArrayInputStream(blob), new ObjectMetadata()));
        if (debug) {
            System.err.println("S3 Cache Upload: key:" + key + "  (" + (System.currentTimeMillis() - t0) + "ms)");
        }
    }
}
