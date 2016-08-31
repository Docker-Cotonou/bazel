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

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.model.*;
import com.google.common.hash.HashCode;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputFileCache;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.remote.RemoteProtocol.CacheEntry;
import com.google.devtools.build.lib.remote.RemoteProtocol.FileEntry;
import com.google.devtools.build.lib.util.Preconditions;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Constants;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/**
 * xcxc
 */
@ThreadSafe
public final class S3ActionCache implements RemoteActionCache {
  private final Path execRoot;
  private final String bucketName;

  // xcxc add retry wrappers ...
  private final AmazonS3 client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());


  /**
   * Construct an action cache using JCache API.
   */
  public S3ActionCache(Path execRoot, RemoteOptions options) {
    System.err.println("CREATING S3ActionCache");
    this.execRoot = execRoot;
    this.bucketName = options.s3CacheBucket;
  }

  @Override
  public String putFileIfNotExist(Path file) throws IOException {
    // HACK https://github.com/bazelbuild/bazel/issues/1413
    // Test cacheStatus output is generated after execution
    // so it doesn't exist in time for us to store it in the remote cache
    System.err.println("putFileIfNotExist - file: " + file.toString());
    if (!file.exists()) return null;
    String contentKey = HashCode.fromBytes(file.getMD5Digest()).toString();
    if (containsFile(contentKey)) {
      return contentKey;
    }
    putFile(contentKey, file);
    return contentKey;
  }

  @Override
  public String putFileIfNotExist(ActionInputFileCache cache, ActionInput input) throws IOException {
    System.err.println("putFileIfNotExist - action input: " + input.toString());
    // HACK https://github.com/bazelbuild/bazel/issues/1413
    // Test cacheStatus output is generated after execution
    // so it doesn't exist in time for us to store it in the remote cache
    Path file = execRoot.getRelative(input.getExecPathString());
    if (!file.exists()) return null;
    // PerActionFileCache already converted this to a lowercase ascii string.. it's not consistent!
    String contentKey = new String(cache.getDigest(input).toByteArray());
    if (containsFile(contentKey)) {
      return contentKey;
    }
    putFile(contentKey, file);
    return contentKey;
  }

  @Override
  public void writeFile(String key, Path dest, boolean executable)
      throws IOException, CacheNotFoundException {
    System.err.println("writeFile - key: " + key + ", dest: " + dest);
    InputStream data = getBlob(key);
    try (OutputStream stream = dest.getOutputStream()) {
      CacheEntry.parseFrom(data).getFileContent().writeTo(stream);
      dest.setExecutable(executable);
    }
  }

  private void putFile(String key, Path file) throws IOException {
    InputStream stream = file.getInputStream();
    putBlob(key, CacheEntry.newBuilder().setFileContent(ByteString.readFrom(stream)).build().toByteArray());
  }


  private boolean containsFile(String key) {
    // xcxc asdf
    System.err.println("rj xcxc containsFile - key: " + key);
    long t0 = System.currentTimeMillis();
    boolean r = client.doesObjectExist(bucketName, key);
    System.err.println("    > took: " + (System.currentTimeMillis() - t0));
    return r;
  }

  private InputStream getBlob(String key)
  {
    System.err.println("xcxc needs impl - getBlob - key: " + key);
    try {
      S3Object obj = client.getObject(new GetObjectRequest(bucketName, key));
      System.err.println("          >> FOUND!!:");
      return obj.getObjectContent();
    }
    catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        throw new CacheNotFoundException("File content cannot be found with key: " + key);

      }
      throw e;
    }
  }

  private void putBlob(String key, byte[] blob)
  {
    System.err.println("xcxc - putBlob - key: " + key);
    long t0 = System.currentTimeMillis();
    client.putObject(new PutObjectRequest(bucketName, key, new ByteArrayInputStream(blob), new ObjectMetadata()));
    System.err.println("    > took: " + (System.currentTimeMillis() - t0));
  }

  @Override
  public void writeActionOutput(String key, Path execRoot)
      throws IOException, CacheNotFoundException {
    System.err.println("writeActionOutput - key: " + key + ", execRoot: " + execRoot);
    InputStream data = getBlob(key);
    if (data == null) {
      throw new CacheNotFoundException("Action output cannot be found with key: " + key);
    }
    CacheEntry cacheEntry = CacheEntry.parseFrom(data);
    for (FileEntry file : cacheEntry.getFilesList()) {
      writeFile(file.getContentKey(), execRoot.getRelative(file.getPath()), file.getExecutable());
    }
  }

  @Override
  public void putActionOutput(String key, Collection<? extends ActionInput> outputs)
      throws IOException {
    System.err.println("writeActionOutput - key: " + key + ", bunch of outputs");
    CacheEntry.Builder actionOutput = CacheEntry.newBuilder();
    for (ActionInput output : outputs) {
      Path file = execRoot.getRelative(output.getExecPathString());
      addToActionOutput(file, output.getExecPathString(), actionOutput);
    }
    putBlob(key, actionOutput.build().toByteArray());
  }

  @Override
  public void putActionOutput(String key, Path execRoot, Collection<Path> files)
      throws IOException {
    System.err.println("writeActionOutput - key: " + key + ", execRoot : "+ execRoot +", bunch of outputs");
    CacheEntry.Builder actionOutput = CacheEntry.newBuilder();
    for (Path file : files) {
      addToActionOutput(file, file.relativeTo(execRoot).getPathString(), actionOutput);
    }
    putBlob(key, actionOutput.build().toByteArray());
  }

  /**
   * Add the file to action output cache entry. Put the file to cache if necessary.
   */
  private void addToActionOutput(Path file, String execPathString, CacheEntry.Builder actionOutput)
      throws IOException {
    System.err.println("addToActionOutput - file: " + file + ", execPathString : "+ execPathString);
    // HACK https://github.com/bazelbuild/bazel/issues/1413
    // Test cacheStatus output is generated after execution
    // so it doesn't exist in time for us to store it in the remote cache
    if (!file.exists()) return;
    if (file.isDirectory()) {
      // TODO(alpha): Implement this for directory.
      throw new UnsupportedOperationException("Storing a directory is not yet supported.");
    }
    // First put the file content to cache.
    String contentKey = putFileIfNotExist(file);
    // Add to protobuf.
    actionOutput
        .addFilesBuilder()
        .setPath(execPathString)
        .setContentKey(contentKey)
        .setExecutable(file.isExecutable());
  }
}
