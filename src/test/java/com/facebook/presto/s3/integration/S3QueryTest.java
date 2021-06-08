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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.facebook.presto.s3.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.log.Level.DEBUG;
import static com.facebook.presto.s3.integration.S3QueryRunner.createQueryRunner;
import static org.testng.Assert.*;

@Test
public class S3QueryTest
{
    private Process p1 = null;
    private Process p2 = null;
    private QueryRunner queryRunner;
    private static final Logger log = Logger.get(S3QueryTest.class);


    @BeforeClass
    public void setUp()
            throws Exception {
    try {
        String[] cmd = { "sh", "src/test/bin/minio_start.sh" };
        System.out.println("Start minio server and load data");
        this.p1 = Runtime.getRuntime().exec(cmd);
        Thread.sleep(20000);
        queryRunner = createQueryRunner();
        BufferedReader output = new BufferedReader(new InputStreamReader(p1.getInputStream()));
            String cmdOut = output.readLine();
            while (cmdOut != null && cmdOut.length() > 0) {
                System.out.println(cmdOut);
                cmdOut = output.readLine();
            }
        } catch (Exception e) {
            System.out.println("Exception starting minio: " + e.toString());
            throw e;
        }

        try {
            String[] cmd = { "sh", "src/test/bin/schema_registry_start.sh" };
            System.out.println("Start schema registry server");
            this.p2 = Runtime.getRuntime().exec(cmd);
            Thread.sleep(5000);
            BufferedReader output = new BufferedReader(new InputStreamReader(p2.getInputStream()));
            String cmdOut = output.readLine();
            while (cmdOut != null && cmdOut.length() > 0) {
                System.out.println(cmdOut);
                cmdOut = output.readLine();
            }
        } catch (Exception e) {
            System.out.println("Exception starting schema registry server: " + e.toString());
            throw e;
        }

    Logging logging = Logging.initialize();
    logging.setLevel("com.facebook.presto.s3", DEBUG);
    }

    @Test
    public void testShowSchema(){
        log.info("Test: testShowSchema");
        assertEquals(queryRunner.execute("SHOW SCHEMAS FROM s3").getRowCount(), 7);
    }

    @Test
    public void testDescribeTable(){
        log.info("Test: testDescribeTable");
        assertEquals(queryRunner.execute("DESCRIBE s3.studentdb.medical").getRowCount(), 3);
    }

    @Test (expectedExceptions = { RuntimeException.class},
            expectedExceptionsMessageRegExp = ".*MetaData Search is not Enabled for this Bucket.*")
    public void testDescribeBucketTable(){
        log.info("Test: testDescribeBucketTable");
        log.info("S3_BUCKETS TABLES" + queryRunner.execute("SHOW TABLES FROM s3.s3_buckets").getMaterializedRows().toString());
        assertEquals(queryRunner.execute("DESCRIBE s3.s3_buckets.testbucket").getRowCount(), 3);
    }

    @Test
    public void testShowTables()
    {
        log.info("Test: testShowTables");
        assertEquals(queryRunner.execute("SHOW TABLES FROM s3.studentdb").getRowCount(), 3);
    }

    @Test
    public void testSelectStarFromCsvTable()
    {
        log.info("Test: testSelectStarFromCsvTable");
        assertFalse(s3SelectEnabledForSession(queryRunner.getDefaultSession()));
        assertEquals(queryRunner.execute("SELECT * FROM s3.studentdb.medical").getRowCount(), 6);
    }

    @Test
    public void testSelectStarFromJSONTable()
    {
        log.info("Test: testSelectStarFromJSONTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.cartoondb.addressTable").getRowCount(), 3);
    }

    @Test
    public void testSelectStarFromAvroTable()
    {
        log.info("Test: testSelectStarFromAvroTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.olympicdb.medaldatatable").getRowCount(), 100);
    }

    @Test
    public void testSelectStarFromParquetTable()
    {
        log.info("Test: testSelectStarFromParquetTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.customer").getRowCount(), 144000);
    }

    @Test
    public void testIndividualColumnParquetTable()
    {
        log.info("Test: testIndividualColumnParquetTable");
        assertEquals(queryRunner.execute("SELECT s_tax_percentage FROM s3.parquetdata.store").getRowCount(), 22);
    }

    @Test
    public void testShowTablesParquet()
    {
        log.info("Test: testShowTablesParquet");
        assertEquals(queryRunner.execute("SHOW tables FROM s3.parquetdata").getRowCount(), 2);
    }
    @Test
    public void testDescribeTablesParquet()
    {
        log.info("Test: testDescribeTablesParquet");
        assertEquals(queryRunner.execute("DESCRIBE s3.parquetdata.customer").getRowCount(), 18);
    }
    @Test
    public void testSelectStarWhereClauseParquet()
    {
        log.info("Test: testSelectStarWhereClauseParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test
    public void testSelectWhereClauseSingleColumnsParquet()
    {
        log.info("Test: testSelectWhereClauseSingleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test
    public void testSelectWhereClauseMultipleColumnsParquet()
    {
        log.info("Test: testSelectWhereClauseMultipleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test
    public void testSelectStarMultipleWhereClausesParquet()
    {
        log.info("Test: testSelectStarMultipleWhereClausesParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test
    public void testSelectMultipleWhereClausesSingleColumnParquet()
    {
        log.info("Test: testSelectMultipleWhereClausesSingleColumnParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test
    public void testSelectMultipleWhereClausesMultipleColumnsParquet()
    {
        log.info("Test: testSelectMultipleWhereClausesSingleColumnParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date,s_rec_end_date FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test
    public void testSelectStarWhereClauseWithOperatorsParquet()
    {
        log.info("Test: testSelectStarWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test
    public void testSelectWhereClauseWithOperatorsParquet()
    {
        log.info("Test: testSelectWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test
    public void testSelectWhereClauseWithOperatorsMultipleColumnsParquet()
    {
        log.info("Test: testSelectWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test
    public void testSelectMultipleWhereClauseWithOperatorsMultipleColumnsParquet()
    {
        log.info("Test: testSelectMultipleWhereClauseWithOperatorsMultipleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>10000 and s_rec_end_date>10664").getRowCount(), 3);
    }

    @Test
    public void testSelectStarWithNoEntriesParquet()
    {
        log.info("Test: testSelectStarWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }

    @Test
    public void testSelectSingleColumnWithNoEntriesParquet()
    {
        log.info("Test: testSelectSingleColumnWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }

    @Test
    public void testSelectMultipleColumnsWithNoEntriesParquet()
    {
        log.info("Test: testSelectMultipleColumnsWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }


    @Test (dependsOnMethods = "testShowSchema")
    public void testCreateDB() {
        log.info("Test: testCreateDB");

        int DBCountBefore =  queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("CREATE SCHEMA s3.newdb");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(DBCountBefore + 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test (dependsOnMethods = "testDropTable")
    public void testDropDB() {
        log.info("Test: testDropDB");

        int DBCountBefore =  queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("DROP SCHEMA s3.newdb");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(DBCountBefore - 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test(dependsOnMethods = "testCreateDB")
    public void testCreateTable() {
        log.info("Test: testCreateTable");
        queryRunner.execute("CREATE TABLE s3.newdb.table123 (id123 bigint, name123 varchar, balance123 double) WITH (FORMAT='CSV', has_header_row = 'false', external_location='s3a://testbucket/TestData/')");
        assertEquals(queryRunner.execute("DESCRIBE s3.newdb.table123").getMaterializedRows().size(), 3);
    }

    @Test(dependsOnMethods = "testCreateTable")
    public void testInsertRow() {
        log.info("Test: testInsertRow");
        queryRunner.execute("INSERT INTO s3.newdb.table123 VALUES (100, 'GEORGE', 20.0)");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newdb.table123").getMaterializedRows().size(), 1);
    }

    @Test(dependsOnMethods = "testInsertRow")
    public void testCTASInsertRow() {
        log.info("Test: testCTASInsertRow");
        queryRunner.execute("CREATE TABLE s3.newdb.table456 WITH (has_header_row='false', FORMAT='CSV', external_location='s3a://testbucket/TestData1') as select * from s3.newdb.table123");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newdb.table456").getMaterializedRows().size(), 1);
    }

    @Test (dependsOnMethods = "testCTASInsertRow")
    public void testDropTable() {
        log.info("Test: testDropTable");
        queryRunner.execute("DROP TABLE s3.newdb.table123");
        List<MaterializedRow> rows = queryRunner.execute("SHOW TABLES in s3.newdb").getMaterializedRows();
        boolean foundTable = false;
        for (MaterializedRow row : rows) {
            if (row.toString().contains("table123")) {
                foundTable = true;
            }
        }
        assertFalse(foundTable);
    }
    @Test (dependsOnMethods = "testShowSchema")
    public void testCreateDBJson() {
        log.info("Test: testCreateDBJson");

        int DBCountBefore =  queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("CREATE SCHEMA s3.newjsondb");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(DBCountBefore + 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test (dependsOnMethods = "testDropTableJson")
    public void testDropDBJson() {
        log.info("Test: testDropDBJson");

        int DBCountBefore =  queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("DROP SCHEMA s3.newjsondb");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(DBCountBefore - 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test(dependsOnMethods = "testCreateDBJson")
    public void testCreateTableJson() {
        log.info("Test: testCreateTableJson");
        queryRunner.execute("CREATE TABLE s3.newjsondb.table123 (id123 bigint, name123 varchar, balance123 double) WITH (FORMAT='JSON', has_header_row = 'false', external_location='s3a://testbucket/TestDataJson/')");
        assertEquals(queryRunner.execute("DESCRIBE s3.newjsondb.table123").getMaterializedRows().size(), 3);
    }

    @Test(dependsOnMethods = "testCreateTableJson")
    public void testInsertRowJson() {
        log.info("Test: testInsertRowJson");
        queryRunner.execute("INSERT INTO s3.newjsondb.table123 VALUES (100, 'GEORGE', 20.0)");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newjsondb.table123").getMaterializedRows().size(), 1);
    }

    @Test(dependsOnMethods = "testInsertRowJson")
    public void testCTASInsertRowJson() {
        log.info("Test: testCTASInsertRowJson");
        queryRunner.execute("CREATE TABLE s3.newjsondb.table456 WITH (has_header_row='false', FORMAT='JSON', external_location='s3a://testbucket/TestDataJson1') as select * from s3.newjsondb.table123");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newjsondb.table456").getMaterializedRows().size(), 1);
    }

    @Test (dependsOnMethods = "testCTASInsertRowJson")
    public void testDropTableJson() {
        log.info("Test: testDropTableJson");
        queryRunner.execute("DROP TABLE s3.newjsondb.table123");
        List<MaterializedRow> rows = queryRunner.execute("SHOW TABLES in s3.newjsondb").getMaterializedRows();
        boolean foundTable = false;
        for (MaterializedRow row : rows) {
            if (row.toString().contains("table123")) {
                foundTable = true;
            }
        }
        assertFalse(foundTable);
    }

    @Test
    public void testS3Select() throws Exception
    {
        // i think maybe there is some minio bug with s3 select where it always skips first line in file
        // maybe it always assumes that there is a header row even though we send IGNORE?
        // there are 3 records in this table here but only 2 would be returned
        // this query here works but just a note for any future tests added
        log.info("Test: testS3Select");
        QueryRunner s3SelectRunner = createQueryRunner(true);
        assertTrue(s3SelectEnabledForSession(s3SelectRunner.getDefaultSession()));
        assertEquals(s3SelectRunner.execute("SELECT * FROM s3.studentdb.names where first = 'joe'").getRowCount(), 1);
    }

    //
    @Test (expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = ".*Error Code: 404 Not Found.*")
    public void testSchemaConfigBadBucket() {
        log.info("Test: testSchemaConfigBadBucket");
        queryRunner.execute("SELECT * FROM s3.bogusdb.bogusBucketTable");

    }

    @Test (expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = ".*Error Code: 404 Not Found.*")
    public void testSchemaConfigBadObject() {
        log.info("Test: testSchemaConfigBadObject");
        queryRunner.execute("SELECT * FROM s3.bogusdb.bogusObjectTable");
    }

    @Test
    public void testS3BucketsShowTables() {
        log.info("Test: testS3BucketsShowTables");
        assertEquals(queryRunner.execute("SHOW tables in s3.s3_buckets").getRowCount(), 1);
    }

    @Test(expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = "MetaData Search is not Enabled for this Bucket")
    public void getSelectStarFromS3Buckets() {
        log.info("Test: getSelectStarFromS3Buckets");
        queryRunner.execute("select * from s3.s3_buckets.testbucket");
    }

    static boolean s3SelectEnabledForSession(Session session)
    {
        for (Map.Entry<String, String> entry : s3Props(session).entrySet()) {
            if (entry.getKey().equals(S3Const.SESSION_PROP_S3_SELECT_PUSHDOWN)) {
                return entry.getValue().equals(S3Const.LC_TRUE);
            }
        }
        return false;
    }

    static Map<String, String> s3Props(Session session)
    {
        return session.getUnprocessedCatalogProperties().get("s3");
    }

    @AfterClass
    public void shutdown()
            throws Exception {
        System.out.println("Stop query runners, schema registry and minio server");
        queryRunner.close();
        queryRunner= null;
        Process p3;
        Process p4;

        if (p1 != null) {
            try {
                String[] cmd = { "sh", "src/test/bin/minio_stop.sh" };
                p3 = Runtime.getRuntime().exec(cmd);
                p3.waitFor();
            } catch (Exception e) {
                System.out.println("Exception stopping query runner and minio: " + e.toString());
                throw e;
            }
        }
        if (p2 != null) {
            try {
                String[] cmd = { "sh", "src/test/bin/schema_registry_stop.sh" };
                p4 = Runtime.getRuntime().exec(cmd);
                p4.waitFor();
            } catch (Exception e) {
                System.out.println("Exception stopping schema registry: " + e.toString());
                throw e;
            }
        }
    }
}
