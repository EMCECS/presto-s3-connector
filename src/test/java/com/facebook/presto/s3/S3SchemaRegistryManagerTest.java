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
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
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

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3SchemaRegistryManager.setDataType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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
    public void testCreateSchemaWithDate() {
        // create new table in presto will create schema in Schema Registry
        // SR will validate this schema when persisted.  ensures date fields formatted correctly

        SchemaTableName schemaTableName = new SchemaTableName("test", "date");

        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("dateField", DateType.DATE));
        columns.add(new ColumnMetadata("timeField", TimeType.TIME));
        columns.add(new ColumnMetadata("timestampField", TimestampType.TIMESTAMP));

        Map<String, Object> properties =
                ImmutableMap.<String, Object>builder()
                        .put("external_location", "s3a://test/date/")
                        .put("format", "json")
                        .build();

        createGroup("test");
        schemaManager.createTable(new ConnectorTableMetadata(schemaTableName, columns, properties));

        // get schema directly from SR
        JSONArray cols = columns("test", "date");
        assertEquals(cols.length(), 3);

        JSONObject field = cols.getJSONObject(0);
        assertEquals(field.getString("name"), "datefield");
        assertEquals(field.getString("type"), "DATE");
        assertEquals(field.getString("dataFormat"), "iso8601");

        field = cols.getJSONObject(1);
        assertEquals(field.getString("name"), "timefield");
        assertEquals(field.getString("type"), "TIME");
        assertEquals(field.getString("dataFormat"), "iso8601");

        field = cols.getJSONObject(2);
        assertEquals(field.getString("name"), "timestampfield");
        assertEquals(field.getString("type"), "TIMESTAMP");
        assertEquals(field.getString("dataFormat"), "iso8601");
    }

    @Test
    public void testSetDataType() {
        JSONObject out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_STRING));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_VARCHAR);
        assertFalse(out.has(JSON_PROP_DATA_FORMAT));

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_STRING)
                        .put(JSON_PROP_FORMAT, FORMAT_VALUE_DATE_TIME));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_TIMESTAMP);
        assertEquals(out.getString(JSON_PROP_DATA_FORMAT), JSON_VALUE_DATE_ISO);

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_STRING)
                        .put(JSON_PROP_FORMAT, FORMAT_VALUE_DATE));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_DATE);
        assertEquals(out.getString(JSON_PROP_DATA_FORMAT), JSON_VALUE_DATE_ISO);

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_STRING)
                        .put(JSON_PROP_FORMAT, FORMAT_VALUE_TIME));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_TIME);
        assertEquals(out.getString(JSON_PROP_DATA_FORMAT), JSON_VALUE_DATE_ISO);

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_NUMBER));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_DOUBLE);

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_INTEGER));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_BIGINT);

        out = new JSONObject();
        setDataType(out,"field1",
                new JSONObject().put(JSON_PROP_TYPE, JSON_TYPE_BOOLEAN));
        assertEquals(out.getString(JSON_PROP_TYPE), JSON_TYPE_BOOLEAN);
    }

    /**
     * extract columns from the S3Table for the given schema.table
     * @param schema
     * @param table
     * @return
     */
    private JSONArray columns(String schema, String table) {
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

    /**
     * create group in Schema Registry if it does not already exist
     * @param groupId
     */
    private void createGroup(String groupId) {
        try {
            schemaRegistry.client().getSchemas(groupId);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            schemaRegistry.client().addGroup(groupId,
                    new GroupProperties(SerializationFormat.Json,
                            Compatibility.allowAny(),
                            false));
        }
    }
}

