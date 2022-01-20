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

import com.amazonaws.services.s3.model.Bucket;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static com.facebook.presto.s3.S3Const.*;

public class S3TableDescriptionSupplier implements Supplier<Map<SchemaTableName, S3Table>> {
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3TableDescriptionSupplier.class);

    private final JsonCodec<S3Table> objectDescriptionCodec;
    private final S3AccessObject accessObject;
    private S3SchemaRegistryManager schemaRegistryManager;
    private final String s3SchemaFileLocationDir;
    private Map<SchemaTableName, S3Table> currentTables;

    @Inject
    S3TableDescriptionSupplier(S3AccessObject accessObject,
                               JsonCodec<S3Table> objectDescriptionCodec, S3ConnectorConfig s3ConnectorConfig) {
        this.objectDescriptionCodec = requireNonNull(objectDescriptionCodec, "objectDescriptionCodec is null");

        this.accessObject = requireNonNull(accessObject, "S3AccessObject is null");

        this.schemaRegistryManager = new S3SchemaRegistryManager(s3ConnectorConfig);
        this.s3SchemaFileLocationDir = s3ConnectorConfig.getS3SchemaFileLocationDir();
    }

    @Override
    public Map<SchemaTableName, S3Table> get() {

        ImmutableMap.Builder<SchemaTableName, S3Table> builder = ImmutableMap.builder();
        List<Bucket> listOfBuckets = this.accessObject.listBuckets();
        for (Bucket bucket : listOfBuckets) {
            JSONObject sources = new JSONObject();
            S3Table table = this.objectDescriptionCodec.fromJson(
                    this.accessObject.loadColumnsFromMetaDataSearchKeys(bucket.getName()).
                                     put("sources", sources).
                                     put("objectDataFormat", DEFAULT_OBJECT_FILE_TYPE).
                                     put("hasHeaderRow", DEFAULT_HAS_HEADER_ROW).
                                     put("recordDelimiter", DEFAULT_RECORD_DELIMITER).
                                     put("fieldDelimiter", DEFAULT_FIELD_DELIMITER).
                                     put("tableBucketName", bucket.getName()).
                                     put("tableBucketPrefix", "/").
                                     toString()
            );
            log.info("CFMTEST -> Adding bucket " + bucket.getName() + " to s3_buckets schema");
            builder.put(new SchemaTableName("s3_buckets", bucket.getName()), table);
        }

        JSONObject objSchema = schemaRegistryManager.getSchemaRegistryConfig();
        if (!objSchema.isEmpty()) {
            log.debug("Including Schema Registry JSON schema: " + objSchema.toString());
            builder.putAll(getSchema(objSchema).build());
        }
        File schemaDir = new File(s3SchemaFileLocationDir);
        if (schemaDir.exists() && schemaDir.isDirectory()) {
            String[] schemaFilesInDir = schemaDir.list();
            for (int i = 0; i < schemaFilesInDir.length; i++) {
                if (schemaFilesInDir[i].endsWith(".json")) {
                    File jsonFile = new File(s3SchemaFileLocationDir + "/" + schemaFilesInDir[i]);
                    String jsonFileStr = getConfigJSONString(jsonFile.getAbsolutePath());
                    JSONObject fileObjSchema = new JSONObject(jsonFileStr);
                    log.info("Including Schema file " + jsonFile.getAbsolutePath()
                            + " with JSON schema: " + fileObjSchema.toString());
                    if (fileObjSchema.has("schemas")) {
                        builder.putAll(getSchema(fileObjSchema).build());
                    }
                }
            }
        } else {
            log.info("No JSON schema files found in " + s3SchemaFileLocationDir + " or directory doesn't exist");
        }
        currentTables = builder.build();
        return currentTables;
    }

    private ImmutableMap.Builder<SchemaTableName, S3Table> getSchema(JSONObject schemaJSON) {
        ImmutableMap.Builder<SchemaTableName, S3Table> returnBuilder = ImmutableMap.builder();

        JSONArray arrayOfObjects = schemaJSON.getJSONArray("schemas");
        for (int i = 0; i < arrayOfObjects.length(); i++) {
            String nameOfSchema = arrayOfObjects.getJSONObject(i).getJSONObject("schemaTableName").getString("schema_name");
            try {
                String nameOfTable = arrayOfObjects.getJSONObject(i).getJSONObject("schemaTableName").getString("table_name");
                S3Table table = this.objectDescriptionCodec.fromJson(arrayOfObjects.getJSONObject(i).getJSONObject("s3Table").toString());

                returnBuilder.put(new SchemaTableName(nameOfSchema, nameOfTable), table);
            } catch (org.json.JSONException e) {
                // DB/Schema with no table
                S3Table emptyTable = new S3Table(NO_TABLES);
                returnBuilder.put(new SchemaTableName(nameOfSchema, NO_TABLES), emptyTable);
            }
        }
        return returnBuilder;
    }

    private static String getConfigJSONString(String filepath) {

        String output = "";
        File configFile = new File(filepath);
        Scanner myReader = null;
        try {
            myReader = new Scanner(configFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            output += data;
        }
        myReader.close();

        return output;
    }
}
