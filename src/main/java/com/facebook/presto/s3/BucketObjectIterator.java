/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BucketObjectIterator
        implements Iterator<S3ObjectSummary> {
    private final AmazonS3 s3Client;

    private final String bucket;
    private final Iterator<String> objects;

    private ObjectListing listing;
    private Iterator<S3ObjectSummary> iterator;

    private final int subObjectSize;

    // for each object, it can be an actual object, or a prefix for a directory
    // the former, that is only object for the bucket
    // the latter, we have to list that prefix and return all of those matching objects
    // if object is empty string "" then we list the bucket itself
    public BucketObjectIterator(AmazonS3 s3Client, String bucket, List<String> objects) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.objects = objects.iterator();
        this.subObjectSize = objects.size();
    }

    private boolean advance() {
        if (listing != null && listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            iterator = listing.getObjectSummaries().iterator();
            Preconditions.checkState(iterator.hasNext());
            return true;
        }

        if (!objects.hasNext()) {
            return false;
        }

        String object = objects.next();

        if (object.isEmpty()) {
            // entire bucket
            // in this case there shouldn't be anything else given
            if (subObjectSize != 1) {
                throw new IllegalArgumentException("multiple sub objects given along with listing all");
            }
            listing = s3Client.listObjects(bucket);
            iterator = listing.getObjectSummaries().iterator();
            return iterator.hasNext();
        }

        // could be single object, or could be directory/

        String prefix = CharMatcher.is('/').trimTrailingFrom(object);
        if (prefix.startsWith("/")) {
            prefix = prefix.replaceFirst("/", "");
        }
        try {
            // object without trailing '/' will succeed
            long size = s3Client.getObjectMetadata(bucket, prefix).getContentLength();

            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setBucketName(bucket);
            summary.setKey(object);
            summary.setSize(size);
            listing = null;
            iterator = Collections.singletonList(summary).iterator();
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404 &&
                    e.getStatusCode() != 400) {
                throw e;
            }
            // fall through to bucket / listObjects handling
        }

        // directory, include trailing '/'
        listing = s3Client.listObjects(bucket, prefix + "/");
        iterator = listing.getObjectSummaries().iterator();

        return iterator.hasNext() || advance();
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext() || advance();
    }

    @Override
    public S3ObjectSummary next() {
        return iterator.next();
    }
}
