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

package com.facebook.presto.s3.integration;

import com.facebook.airlift.log.Level;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import com.facebook.presto.s3.*;

import com.facebook.airlift.log.Logging;
import com.facebook.airlift.log.LoggingConfiguration;

import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class S3QueryRunner {

    private S3QueryRunner() {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception {
        return createQueryRunner(false, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(boolean s3SelectEnabled)
            throws Exception {
        return createQueryRunner(s3SelectEnabled, ImmutableMap.of());
    }

    private static DistributedQueryRunner createQueryRunner(boolean s3SelectEnabled, Map<String, String> extraProperties)
            throws Exception {
        Logging logging = Logging.initialize();
        logging.configure(new LoggingConfiguration());
        logging.setLevel("com.facebook.presto", Level.DEBUG);

        Session session = testSessionBuilder()
                .setCatalog("s3")
                .setSchema("default")
                .setCatalogSessionProperty("s3", S3Const.SESSION_PROP_S3_SELECT_PUSHDOWN, String.valueOf(s3SelectEnabled).toLowerCase())
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 1, extraProperties);

        try {
            queryRunner.installPlugin(new S3Plugin());
            Map<String, String> s3Properties = ImmutableMap.<String, String>builder()
                    .put("s3.s3SchemaFileLocationDir", "src/test/resources")
                    .put("s3.s3Port", "9000")
                    .put("s3.s3UserKey", "AKIAIOSFODNN7EXAMPLE")
                    .put("s3.s3UserSecretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                    .put("s3.s3Nodes", "127.0.0.1")
                    .put("s3.schemaRegistryServerIP", "127.0.0.1")
                    .put("s3.schemaRegistryPort", "9092")
                    .put("s3.schemaRegistryNamespace", "s3-schemas")
                    .put("s3.maxConnections", "500")
                    .put("s3.s3SocketTimeout", "5000")
                    .put("s3.s3ConnectionTimeout", "5000")
                    .put("s3.s3ClientExecutionTimeout", "5000")
                    .build();
            queryRunner.createCatalog("s3", "s3", s3Properties);

            return queryRunner;
        } catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }
}

