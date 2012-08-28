/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.     
 * 
 * The contents of this file are subject to the terms of the Common Development 
 * and Distribution License("CDDL") (the "License").  You may not use this file 
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at 
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing permissions and limitations 
 * under the License. 
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields 
 * enclosed by brackets [] replaced by your own identifying information: 
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.bundles.db.table;

import org.connid.bundles.db.table.DatabaseTableConfiguration;
import org.connid.bundles.db.table.DatabaseTableConnection;
import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.identityconnectors.common.ByteUtil.randomBytes;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.IOUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.connid.bundles.db.table.mapping.MappingStrategy;
import org.connid.bundles.db.table.security.AES;
import org.connid.bundles.db.common.ExpectProxy;
import org.connid.bundles.db.common.SQLParam;
import org.connid.bundles.db.common.SQLUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Attempts to test the Connector with the framework.
 */
public class DatabaseTableTests extends DatabaseTableTestBase {

    // Setup/Teardown
    /**
     * Creates a temporary database based on a SQL resource file.
     * @throws Exception 
     */
    @BeforeClass
    public static void createDatabase()
            throws Exception {

        final String url;

        if (StringUtil.isNotBlank(DRIVER) && DRIVER.contains("derby")) {
            if (StringUtil.isNotBlank(DB)) {
                final File f = new File(DB);
                // clear out the test database directory..
                IOUtil.delete(f);
            }

            url = URL + ";create=true";
        } else {
            url = URL;
        }

        // attempt to create the database in the directory..
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(url, USER, PASSWD);
            // create the database..
            stmt = conn.createStatement();

            final String purge = getResourceAsString(PURGE);
            if (StringUtil.isNotBlank(purge)) {
                stmt.execute(purge);
            }

            final String create = getResourceAsString(ACCOUNTS);
            assertNotNull(create);
            stmt.execute(create);

            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } finally {
            SQLUtil.closeQuietly(stmt);
            SQLUtil.closeQuietly(conn);
        }
    }

    /**
     * test method
     */
    @AfterClass
    public static void deleteDatabase() {
        try {
            if (StringUtil.isNotBlank(DRIVER) && DRIVER.contains("derby")) {
                DriverManager.getConnection(URL + ";shutdown=true");

                if (StringUtil.isNotBlank(DB)) {
                    final File f = new File(DB);
                    // clear out the test database directory..
                    IOUtil.delete(f);
                }

            }
        } catch (Exception ex) {
            //expected
        }
    }

    /**
     * Create the test configuration
     * @return the initialized configuration
     */
    @Override
    protected DatabaseTableConfiguration getConfiguration()
            throws Exception {
        DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        config.setJdbcDriver(DRIVER);
        config.setUser(USER);
        config.setPassword(new GuardedString(PASSWD.toCharArray()));
        config.setTable(DB_TABLE);
        config.setChangeLogColumn(CHANGELOG);
        config.setKeyColumn(KEYCOLUMN);
        config.setPasswordColumn(PASSWORDCOLUMN);
        config.setConnectorMessages(TestHelpers.createDummyMessages());
        config.setDatabase(DB);
        config.setJdbcUrlTemplate(URL);

        // status fields configuration
        config.setStatusColumn(STATUS);
        config.setEnabledStatusValue(ENABLEDSTATUS);
        config.setEnabledStatusValue(DISABLEDSTATUS);
        config.setDefaultStatusValue(DEFAULTSTATUS);

        // password managemnet configuration
        config.setCipherAlgorithm(AES.class.getName());
        config.setCipherKey("cipherkeytoencodeanddecodepassword");
        config.setRetrievePassword(true);

        return config;
    }

    /* (non-Javadoc)
     * @see org.identityconnectors.databasetable.DatabaseTableTestBase#getCreateAttributeSet()
     */
    @Override
    protected Set<Attribute> getCreateAttributeSet(DatabaseTableConfiguration cfg)
            throws Exception {
        final Set<Attribute> ret = new HashSet<Attribute>();

        // set __ENABLED__ attribute
        ret.add(AttributeBuilder.buildEnabled(true));

        ret.add(AttributeBuilder.build(Name.NAME, randomString(50)));

        if (StringUtil.isNotBlank(cfg.getPasswordColumn())) {
            ret.add(AttributeBuilder.buildPassword(
                    new GuardedString(randomString(50).toCharArray())));
        } else {
            ret.add(AttributeBuilder.build(PASSWORDCOLUMN, randomString(40)));
        }

        ret.add(AttributeBuilder.build(MANAGER, randomString(15)));
        ret.add(AttributeBuilder.build(MIDDLENAME, randomString(50)));
        ret.add(AttributeBuilder.build(FIRSTNAME, randomString(50)));
        ret.add(AttributeBuilder.build(LASTNAME, randomString(50)));
        ret.add(AttributeBuilder.build(EMAIL, randomString(50)));
        ret.add(AttributeBuilder.build(DEPARTMENT, randomString(50)));
        ret.add(AttributeBuilder.build(TITLE, randomString(50)));

        if (!cfg.getChangeLogColumn().equalsIgnoreCase(AGE)) {
            ret.add(AttributeBuilder.build(AGE, r.nextInt(100)));
        }

        if (!cfg.getChangeLogColumn().equalsIgnoreCase(ACCESSED)) {
            long v = r.nextLong();

            // Oracle conversion between long and BigDecimal is not exact 
            // in case of great longs (number of digits close to 19)
            ret.add(AttributeBuilder.build(
                    ACCESSED, v > 100000L || v < -100000L ? v / 10000L : v));
        }

        ret.add(AttributeBuilder.build(SALARY, new BigDecimal("360536.75")));
        ret.add(AttributeBuilder.build(JPEGPHOTO, randomBytes(r, 2000)));
        ret.add(AttributeBuilder.build(OPENTIME, new java.sql.Time(
                System.currentTimeMillis()).toString()));
        ret.add(AttributeBuilder.build(ACTIVATE, new java.sql.Date(
                System.currentTimeMillis()).toString()));
        ret.add(AttributeBuilder.build(CHANGED, new Timestamp(
                System.currentTimeMillis() / 1000 * 1000).toString()));

        if (!cfg.getChangeLogColumn().equalsIgnoreCase(CHANGELOG)) {
            ret.add(AttributeBuilder.build(CHANGELOG, new Timestamp(
                    System.currentTimeMillis()).getTime()));
        }

        return ret;
    }

    /* (non-Javadoc)
     * @see org.identityconnectors.databasetable.DatabaseTableTestBase#getModifyAttributeSet()
     */
    @Override
    protected Set<Attribute> getModifyAttributeSet(DatabaseTableConfiguration cfg)
            throws Exception {
        return getCreateAttributeSet(cfg);
    }

    /**
     * test method
     */
    @Override
    @Test
    public void testConfiguration() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        // check defaults..        
        config.setJdbcDriver(DRIVER);
        assertEquals(DRIVER, config.getJdbcDriver());
        config.setKeyColumn(KEYCOLUMN);
        assertEquals(KEYCOLUMN, config.getKeyColumn());
        config.setTable(DB_TABLE);
        assertEquals(DB_TABLE, config.getTable());
        config.setJdbcUrlTemplate(URL);
        assertEquals(URL, config.getJdbcUrlTemplate());
        config.setDatabase(DB);
        assertEquals(DB, config.getDatabase());
        config.setUser(KEYCOLUMN);
        assertEquals(KEYCOLUMN, config.getUser());
        config.setPassword(new GuardedString("".toCharArray()));
        assertEquals(KEYCOLUMN, config.getUser());
        config.validate();
    }

    /**
     * For testing purposes we creating connection an not the framework.
     * @throws Exception 
     */
    @Test
    public void testNoZeroSQLExceptions()
            throws Exception {
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setRethrowAllSQLExceptions(false);

        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse =
                new ExpectProxy<MappingStrategy>();

        final MappingStrategy sms = smse.getProxy(MappingStrategy.class);

        //Schema
        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }

        //Create fail
        smse.expectAndThrow(
                "setSQLParam", new SQLException("test reason", "0", 0));

        //Update fail
        smse.expectAndThrow(
                "setSQLParam", new SQLException("test reason", "0", 0));

        con.getConn().setSms(sms);
        Set<Attribute> expected = getCreateAttributeSet(cfg);

        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);

        con.update(ObjectClass.ACCOUNT, uid, expected, null);
        assertTrue("setSQLParam not called", smse.isDone());
    }

    /**
     * For testing purposes we creating connection an not the framework.
     * @throws Exception 
     */
    @Test(expected = ConnectorException.class)
    public void testNonZeroSQLExceptions()
            throws Exception {
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setRethrowAllSQLExceptions(false);

        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse =
                new ExpectProxy<MappingStrategy>();

        final MappingStrategy sms = smse.getProxy(MappingStrategy.class);

        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }

        smse.expectAndThrow(
                "setSQLParam", new SQLException("test reason", "411", 411));

        con.getConn().setSms(sms);

        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        con.create(ObjectClass.ACCOUNT, expected, null);

        assertTrue("setSQLParam not called", smse.isDone());
    }

    /**
     * For testing purposes we creating connection an not the framework.
     * @throws Exception 
     */
    @Test(expected = ConnectorException.class)
    public void testRethrowAllSQLExceptions()
            throws Exception {
        final DatabaseTableConfiguration cfg = getConfiguration();

        cfg.setRethrowAllSQLExceptions(true);

        con = getConnector(cfg);

        final ExpectProxy<MappingStrategy> smse = new ExpectProxy<MappingStrategy>();

        final MappingStrategy sms = smse.getProxy(MappingStrategy.class);

        for (int i = 0; i < 15; i++) {
            smse.expectAndReturn("getSQLAttributeType", String.class);
        }

        smse.expectAndThrow(
                "setSQLParam", new SQLException("test reason", "0", 0));

        con.getConn().setSms(sms);

        Set<Attribute> expected = getCreateAttributeSet(cfg);
        con.create(ObjectClass.ACCOUNT, expected, null);

        assertTrue("setSQLParam not called", smse.isDone());
    }

    /**
     * For testing purposes we creating connection an not the framework.
     * @throws Exception 
     */
    @Test
    public void testSchema()
            throws Exception {
        DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        // check if this works..
        Schema schema = con.schema();
        checkSchema(schema);
    }

    /**
     * check validity of the schema
     * 
     * @param schema
     *            the schema to be checked
     * @throws Exception 
     */
    void checkSchema(Schema schema)
            throws Exception {
        // Schema should not be null
        assertNotNull(schema);

        final Set<ObjectClassInfo> objectInfos = schema.getObjectClassInfo();

        assertNotNull(objectInfos);
        assertEquals(1, objectInfos.size());

        // get the fields from the test account
        final Set<Attribute> attributeSet =
                getCreateAttributeSet(getConfiguration());

        final Map<String, Attribute> expected = AttributeUtil.toMap(attributeSet);
        final Set<String> keys = CollectionUtil.newCaseInsensitiveSet();

        keys.addAll(expected.keySet());

        // iterate through ObjectClassInfo Set
        for (ObjectClassInfo objectInfo : objectInfos) {
            assertNotNull(objectInfo);
            // the object class has to ACCOUNT_NAME
            assertTrue(objectInfo.is(ObjectClass.ACCOUNT_NAME));

            // iterate through AttributeInfo Set
            for (AttributeInfo attInfo : objectInfo.getAttributeInfo()) {
                assertNotNull(attInfo);

                final String fieldName = attInfo.getName();

                keys.remove(fieldName);

                final Attribute fa = expected.get(fieldName);

                if (fa != null) {

                    final Object field = AttributeUtil.getSingleValue(fa);

                    final Class<?> valueClass =
                            field != null ? field.getClass() : null;

                    if (attInfo.getType().equals(BigDecimal.class)) {
                        // Oracle return BigDecimal instead of Integer or Long 
                        // or something else
                        assertTrue("field: " + fieldName,
                                valueClass == null
                                || valueClass.equals(Integer.class)
                                || valueClass.equals(Long.class)
                                || valueClass.equals(BigDecimal.class));
                    } else {
                        assertTrue("field: " + fieldName,
                                valueClass == null
                                || OperationalAttributes.ENABLE_NAME.equals(attInfo.
                                getName())
                                || valueClass.equals(attInfo.getType()));
                    }
                }
            }
            // all the attribute has to be removed
            assertTrue(
                    "Missing attributes in the schema: " + keys, keys.isEmpty());
        }
    }

    /**
     * Test creating of the connector object, searching using UID and delete
     * @throws Exception 
     * @throws SQLException 
     */
    @Test
    public void testGetLatestSyncToken()
            throws Exception {
        final String SQL_TEMPLATE =
                "UPDATE Accounts SET changelog = ? WHERE accountId = ?";

        final DatabaseTableConfiguration cfg = getConfiguration();

        con = getConnector(cfg);

        deleteAllFromAccounts(con.getConn());

        final Set<Attribute> expected = getCreateAttributeSet(cfg);

        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        final Long changelog = 9999999999999L; //Some really big value

        // update the last change
        PreparedStatement ps = null;
        DatabaseTableConnection conn = null;

        try {
            conn = DatabaseTableConnection.createDBTableConnection(
                    getConfiguration());

            List<SQLParam> values = new ArrayList<SQLParam>();
            values.add(new SQLParam("changelog", changelog, Types.INTEGER));
            values.add(new SQLParam("accountId", uid.getUidValue(),
                    Types.VARCHAR));
            ps = conn.prepareStatement(SQL_TEMPLATE, values);
            ps.execute();
            conn.commit();
        } finally {
            SQLUtil.closeQuietly(ps);
            SQLUtil.closeQuietly(conn);
        }

        // attempt to find the newly created object..
        final SyncToken latestSyncToken =
                con.getLatestSyncToken(ObjectClass.ACCOUNT);

        assertNotNull(latestSyncToken);

        final Object actual = latestSyncToken.getValue();
        assertEquals(changelog, actual);
    }

    static String getResourceAsString(String res) {
        return IOUtil.getResourceAsString(DatabaseTableTests.class, res);
    }

    private String randomString(int length) {
        final StringBuilder bld = new StringBuilder();

        for (int i = 0; i < length; i++) {
            bld.append((char) (r.nextInt(25) + 65));
        }

        return bld.toString();
    }

    /**
     * testTimestampColumn operates on the table 'bug17551table'
     * @throws Exception 
     */
    @Test
    public void testTimestampColumnNative()
            throws Exception {
        if (DRIVER.contains("mysql")) {
            // MySql doesn't permit to store microseconds. 
            // This test is not applicable for this DBMS.
            return;
        }

        log.ok("testCreateCall");
        DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setNativeTimestamps(true);
        con = getConnector(cfg);

        Set<Attribute> expected = getCreateAttributeSet(cfg);

        Attribute changed = AttributeUtil.find(CHANGED, expected);
        expected.remove(changed);

        expected.add(AttributeBuilder.build(CHANGED,
                Timestamp.valueOf("2005-12-07 10:29:01.5").toString()));

        Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);

        // attempt to get the record back..
        List<ConnectorObject> results = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));

        assertTrue("expect 1 connector object", results.size() == 1);

        final ConnectorObject co = results.get(0);
        assertNotNull(co);

        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);

        Attribute tmsAtr = AttributeUtil.find(CHANGED, actual);
        String timestampTest = AttributeUtil.getStringValue(tmsAtr);

        if (timestampTest == null || timestampTest.indexOf(".5") == -1) {
            fail("testcase for bug#17551 failed, "
                    + "expected 5 in the milli-seconds part, "
                    + "but got timestamp " + timestampTest);
        }
    }

    /**
     * testTimestampColumn operates on the table 'bug17551table'
     * @throws Exception 
     */
    @Test
    public void testTimestampColumnNotNative()
            throws Exception {
        if (DRIVER.contains("mysql")) {
            // MySql doesn't permit to store microseconds. 
            // This test is not applicable for this DBMS.
            return;
        }

        log.ok("testCreateCall");
        DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setNativeTimestamps(false);
        con = getConnector(cfg);

        Set<Attribute> expected = getCreateAttributeSet(cfg);

        Attribute changed = AttributeUtil.find(CHANGED, expected);
        expected.remove(changed);

        expected.add(AttributeBuilder.build(CHANGED,
                Timestamp.valueOf("2005-12-07 10:29:01.5").toString()));

        Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);

        // attempt to get the record back..
        List<ConnectorObject> results = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));

        assertTrue("expect 1 connector object", results.size() == 1);

        final ConnectorObject co = results.get(0);
        assertNotNull(co);

        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);

        Attribute tmsAtr = AttributeUtil.find(CHANGED, actual);
        String timestampTest = AttributeUtil.getStringValue(tmsAtr);

        if (timestampTest != null && timestampTest.indexOf(".5") == -1) {
            fail("testcase for bug#17551 failed, "
                    + "expected 5 in the milli-seconds part, "
                    + "but got timestamp " + timestampTest);
        }
    }
}
