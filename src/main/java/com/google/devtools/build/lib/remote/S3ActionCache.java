package com.google.devtools.build.lib.remote;

import com.google.devtools.build.lib.vfs.Path;

import java.io.IOException;
import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.remote.ContentDigests.ActionKey;
import com.google.devtools.build.lib.remote.RemoteProtocol.ActionResult;
import com.google.devtools.build.lib.remote.RemoteProtocol.ContentDigest;
import com.google.devtools.build.lib.remote.RemoteProtocol.FileMetadata;
import com.google.devtools.build.lib.remote.RemoteProtocol.FileNode;
import com.google.devtools.build.lib.remote.RemoteProtocol.Output;
import com.google.devtools.build.lib.remote.RemoteProtocol.Output.ContentCase;
import com.google.devtools.build.lib.remote.TreeNodeRepository.TreeNode;
import com.google.devtools.build.lib.util.Preconditions;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/**
 * Created by tylernisonoff on 3/16/17.
 */
public class S3ActionCache implements RemoteActionCache {
    private S3ActionCache2 cache;
    private final boolean debug;

    S3ActionCache(RemoteOptions options) {
        this.cache = new S3ActionCache2(options);
        this.debug = options.remoteCacheDebug;
    }

    @Override
    public void uploadTree(TreeNodeRepository repository, Path execRoot, TreeNode root)
            throws IOException, InterruptedException {
        repository.computeMerkleDigests(root);
        for (FileNode fileNode : repository.treeToFileNodes(root)) {
            // TODO blacklist here (before calling toByteArray()) if file is too big (but is it
            // okay to blacklist only _some_ of the files from a call to uploadTree? need to check)
            uploadBlob(fileNode.toByteArray());
        }
        for (TreeNode leaf : repository.leaves(root)) {
            uploadFileContents(execRoot.getRelative(leaf.getActionInput().getExecPathString()));
        }
    }

    @Override
    public void downloadTree(ContentDigest rootDigest, Path rootLocation)
            throws IOException, CacheNotFoundException {
        FileNode fileNode = FileNode.parseFrom(downloadBlob(rootDigest));
        if (debug)
            System.err.println("  " + rootLocation);
        if (fileNode.hasFileMetadata()) {
            FileMetadata meta = fileNode.getFileMetadata();
            downloadFileContents(meta.getDigest(), rootLocation, meta.getExecutable());
        }
        for (FileNode.Child child : fileNode.getChildList()) {
            downloadTree(child.getDigest(), rootLocation.getRelative(child.getPath()));
        }
    }

    @Override
    public void downloadAllResults(ActionResult result, Path execRoot)
            throws IOException, CacheNotFoundException {
        for (Output output : result.getOutputList()) {
            if (output.getContentCase() == ContentCase.FILE_METADATA) {
                FileMetadata m = output.getFileMetadata();
                downloadFileContents(
                        m.getDigest(), execRoot.getRelative(output.getPath()), m.getExecutable());
            } else {
                downloadTree(output.getDigest(), execRoot.getRelative(output.getPath()));
            }
        }
    }

    @Override
    public void uploadAllResults(Path execRoot, Collection<Path> files, ActionResult.Builder result)
            throws IOException, InterruptedException {
        for (Path file : files) {
            if (file.isDirectory()) {
                // TODO(olaola): to implement this for a directory, will need to create or pass a
                // TreeNodeRepository to call uploadTree.
                throw new UnsupportedOperationException("Storing a directory is not yet supported.");
            }
            // First put the file content to cache.
            ContentDigest digest = uploadFileContents(file);
            // Add to protobuf.
            // ##TestHack
            if (digest != null) {
                result
                        .addOutputBuilder()
                        .setPath(file.relativeTo(execRoot).getPathString())
                        .getFileMetadataBuilder()
                        .setDigest(digest)
                        .setExecutable(file.isExecutable());
            }
        }
    }

    @Override
    public ContentDigest uploadFileContents(Path file) throws IOException, InterruptedException {
        // This unconditionally reads the whole file into memory first!
        if(!file.exists()) {
            // #TestHack -- bazel tries to upload results before they exist. Uncomment below when debug logging is added here
            //System.err.println("Does not exist: " + file);
            return null;
        }
        ContentDigest digest = ContentDigests.computeDigest(file);
        if(!isBlacklisted(file)) {
            cache.putFile(ContentDigests.toHexString(digest), file);
        }
        return digest;
    }

    @Override
    public void downloadFileContents(ContentDigest digest, Path dest, boolean executable)
            throws IOException, CacheNotFoundException {
        // This unconditionally downloads the whole file into memory first!
        if(isBlacklisted(dest)) {
            throw new CacheNotFoundException(digest);
        }
        FileSystemUtils.createDirectoryAndParents(dest.getParentDirectory());
        Path getPath = cache.getFile(ContentDigests.toHexString(digest), dest, digest);
        if (debug)
            System.err.println("  " + dest);
        dest.setExecutable(executable);
    }

    @Override
    public ImmutableList<ContentDigest> uploadBlobs(Iterable<byte[]> blobs)
            throws InterruptedException {
        ArrayList<ContentDigest> digests = new ArrayList<>();
        for (byte[] blob : blobs) {
            digests.add(uploadBlob(blob));
        }
        return ImmutableList.copyOf(digests);
    }

    @Override
    public ContentDigest uploadBlob(byte[] blob) throws InterruptedException {
        // TODO blacklist if blob is too big
        ContentDigest digest = ContentDigests.computeDigest(blob);
        cache.put(ContentDigests.toHexString(digest), blob);
        return digest;
    }

    @Override
    public byte[] downloadBlob(ContentDigest digest) throws CacheNotFoundException {
        if (digest.getSizeBytes() == 0) {
            return new byte[0];
        }
        // This unconditionally downloads the whole blob into memory!
        byte[] data = cache.get(ContentDigests.toHexString(digest));
        if (data == null) {
            throw new CacheNotFoundException(digest);
        }
        return data;
    }

    @Override
    public ImmutableList<byte[]> downloadBlobs(Iterable<ContentDigest> digests)
            throws CacheNotFoundException {
        ArrayList<byte[]> blobs = new ArrayList<>();
        for (ContentDigest c : digests) {
            blobs.add(downloadBlob(c));
        }
        return ImmutableList.copyOf(blobs);
    }

    public boolean containsKey(ContentDigest digest) {
        return cache.containsKey(ContentDigests.toHexString(digest));
    }

    @Override
    public ActionResult getCachedActionResult(ContentDigests.ActionKey actionKey) {
        byte[] data = cache.get(ContentDigests.toHexString(actionKey.getDigest()));
        if (data == null) {
            return null;
        }
        try {
            return ActionResult.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    @Override
    public void setCachedActionResult(ContentDigests.ActionKey actionKey, ActionResult result) throws InterruptedException {
        cache.put(ContentDigests.toHexString(actionKey.getDigest()), result.toByteArray());
    }

    private boolean isBlacklisted(Path path) {
        // path can be null, in which case we choose not to blacklist
        if (path == null) {
            return false;
        }
        String pathString = path.toString();
        if (pathString.endsWith(".ts") && !pathString.endsWith(".d.ts")) {
            return true;
        }
        if (pathString.endsWith(".tar")) {
            return true;
        }
        return false;
    }
}
