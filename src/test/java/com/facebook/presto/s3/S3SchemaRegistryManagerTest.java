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

import com.facebook.presto.common.type.DateType;
import com.facebook.presto.s3.services.EmbeddedSchemaRegistry;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class S3SchemaRegistryManagerTest {

    private EmbeddedSchemaRegistry schemaRegistry;

    private S3SchemaRegistryManager schemaManager;

    @BeforeClass
    public void beforeClass() {
        schemaRegistry = new EmbeddedSchemaRegistry();
        schemaRegistry.start();

        S3ConnectorConfig config = new S3ConnectorConfig();
        config.setSchemaRegistryServerIP("127.0.0.1")
                .setSchemaRegistryServerPort(schemaRegistry.port());

        schemaManager = new S3SchemaRegistryManager(config);
    }

    @AfterClass
    public void afterClass() {
        schemaRegistry.stop();
    }

    @Test
    public void testCreateWithDate()
    {
        SchemaTableName schemaTableName = new SchemaTableName("test", "date");

        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("dateField", DateType.DATE));

        Map<String, Object> properties =
                ImmutableMap.<String, Object>builder()
                        .put("external_location", "s3a://test/date/")
                        .put("format", "json")
                        .build();

        createGroup("test");
        schemaManager.createTable(new ConnectorTableMetadata(schemaTableName, columns, properties));

        JSONArray cols = columns("test", "date");
        assertEquals(cols.length(), 1);

        JSONObject field = cols.getJSONObject(0);
        assertEquals(field.getString("name"), "datefield");
        assertEquals(field.getString("type"), "DATE");
    }

    JSONArray columns(String schema, String table) {
        JSONArray schemas = schemaManager.getSchemaRegistryConfig().getJSONArray("schemas");
        for (int i = 0; i < schemas.length(); i++) {
            JSONObject schemaTableName = schemas.getJSONObject(i).getJSONObject("schemaTableName");
            if (schemaTableName.getString("schema_name").equals(schema) &&
                    schemaTableName.getString("table_name").equals(table)) {
                return schemas.getJSONObject(i).getJSONObject("s3Table").getJSONArray("columns");
            }
        }
        return null;
    }

    void createGroup(String groupId) {
        try {
            schemaRegistry.client().getSchemas(groupId);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            schemaRegistry.client().addGroup(groupId, new GroupProperties(SerializationFormat.Json, Compatibility.allowAny(), false));
        }
    }
}

