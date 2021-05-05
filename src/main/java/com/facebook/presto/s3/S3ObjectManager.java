/*
 * Copyright (c) Pravega Authors.
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

import com.facebook.airlift.log.Logger;

import javax.inject.Inject;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class S3ObjectManager
{
    private static final Logger log = Logger.get(S3ObjectManager.class);

    private final S3AccessObject s3AccessObject;

    @Inject
    public S3ObjectManager(S3AccessObject s3AccessObject) {
        this.s3AccessObject = requireNonNull(s3AccessObject, "s3AccessObject is null");
    }

    public Iterator<S3ObjectRange> getS3ObjectIterator(Map<String, List<String>> bucketObjectMap, int rangeMB)
    {
        requireNonNull(bucketObjectMap, "getS3ObjectIterator(): bucketObjectMap is null");
        List<S3ObjectRange> objectList = new Vector<>();
        int rangeBytes = rangeMB * 1024 * 1024;
        for (Map.Entry<String, List<String>> entry : expandBucketObjectMap(bucketObjectMap).entrySet()) {
            String bucket = entry.getKey();
            for (String key : entry.getValue()) {
                long objectLength = s3AccessObject.getObjectLength(bucket, key);
                if (objectLength == 0) {
                    log.warn("skipping 0 length object: " + bucket + "." + key);
                    continue;
                }
                if (rangeBytes == 0) {
                    objectList.add(new S3ObjectRange(bucket, key));
                } else {
                    long offset = 0;
                    while (offset == 0 || offset < objectLength) {
                        int length = (int) (offset + rangeBytes > objectLength
                                ? objectLength - offset
                                : rangeBytes);
                        objectList.add(new S3ObjectRange(bucket, key, offset, length));
                        offset += rangeBytes;
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug(Arrays.toString(objectList.toArray()));
        }

        return objectList.iterator();
    }

    private Map<String, List<String>> expandBucketObjectMap (Map<String, List<String>> bucketObjectsMap) {
        Map<String, List<String>> returnMap = new HashMap<>();
        for (String bucket : bucketObjectsMap.keySet()) {
            List<String> objects = new Vector<>();
            // First verify bucket exists
            if (!s3AccessObject.bucketExists(bucket)) {
                log.error("Bucket " + bucket + " does not exist");
                objects.add("unknown");
                returnMap.put (bucket, objects);
                continue;
            }
            for (String key : bucketObjectsMap.get(bucket)){
                // Next, verify key exists (file or dir)
                if (!s3AccessObject.objectOrDirExists(bucket, key)){
                    log.error("Object " + key + " in bucket " + bucket + " does not exist");
                    objects.add(key);
                    returnMap.put (bucket, objects);
                    continue;
                }
                for (String objectName : s3AccessObject.listObjects(bucket, key)) {
                    log.debug("Adding object " + objectName + " for bucket " + bucket);
                    objects.add(objectName);
                }
            }
            returnMap.put (bucket, objects);
        }
        return returnMap;
    }
}
