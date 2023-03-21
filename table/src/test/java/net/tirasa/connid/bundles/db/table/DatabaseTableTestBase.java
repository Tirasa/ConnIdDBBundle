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
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 * Portions Copyrighted 2011 ConnId.
 */
package net.tirasa.connid.bundles.db.table;

import static net.tirasa.connid.bundles.db.table.util.DatabaseTableSQLUtil.tsAsLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import net.tirasa.connid.bundles.db.commons.SQLParam;
import net.tirasa.connid.bundles.db.commons.SQLUtil;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import net.tirasa.connid.bundles.db.table.security.MD5;
import net.tirasa.connid.bundles.db.table.security.PasswordEncodingException;
import net.tirasa.connid.bundles.db.table.security.UnsupportedPasswordCharsetException;
import org.identityconnectors.framework.api.operations.AuthenticationApiOp;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Attempts to test the Connector with the framework.
 */
public abstract class DatabaseTableTestBase {

    // Constants..
    protected static final String CHANGELOG = "changelog";

    protected static final String KEYCOLUMN = "accountId";

    protected static final String PASSWORDCOLUMN = "password";

    protected static final String PASSWORD_CHARSETNAME = "UTF-8";

    protected static final String MANAGER = "manager";

    protected static final String MIDDLENAME = "middlename";

    protected static final String FIRSTNAME = "firstname";

    protected static final String LASTNAME = "lastname";

    protected static final String EMAIL = "email";

    protected static final String DEPARTMENT = "department";

    protected static final String TITLE = "title";

    protected static final String AGE = "age";

    protected static final String SALARY = "salary";

    protected static final String JPEGPHOTO = "jpegphoto";

    protected static final String ACTIVATE = "activate";

    protected static final String ACCESSED = "accessed";

    protected static final String OPENTIME = "opentime";

    protected static final String CHANGED = "changed";

    protected static final String STATUS = "status";

    protected static final String ENABLEDSTATUS = "";

    protected static final String DISABLEDSTATUS = "disabled";

    protected static final String DEFAULTSTATUS = "";

    /**
     * Setup logging for the {@link DatabaseTableConnector}.
     */
    protected static final Log LOG = Log.getLog(DatabaseTableConnector.class);

    // always seed that same for results..
    protected static final Random RANDOM = new Random(17);

    /**
     *
     * Get configuration & connection properties.
     *
     */
    private final static Properties PROPS = new Properties();

    static {
        try {
            final InputStream is = DatabaseTableConfigurationTests.class.getResourceAsStream("/persistence.properties");
            PROPS.load(is);
        } catch (Throwable t) {
            LOG.error("Error retrieving configuration/connection properties", t);
        }
    }

    protected static final String DB = PROPS.getProperty("db");

    protected static final String URL = PROPS.getProperty("url");

    protected static final String USER = PROPS.getProperty("user", "");

    protected static final String PASSWD = PROPS.getProperty("password", "");

    protected static final String DRIVER = PROPS.getProperty("driver");

    protected static final String PASSWORD_CHARSET = PROPS.getProperty("password_charset", "UTF-8");

    protected static final Boolean IS_EMPTY_STRING_SUPPORT = Boolean.parseBoolean(
            PROPS.getProperty("isEmptyStringSupported", "false"));

    protected static final String ACCOUNTS = "accounts.sql";

    protected static final String PURGE = "purge.sql";

    //The tested table
    protected static final String DB_TABLE = "Accounts";

    /**
     *
     * The connector
     *
     */
    DatabaseTableConnector con = null;

    /**
     *
     * Create the test configuration
     *
     *
     *
     * @return the initialized configuration
     *
     * @throws Exception anything wrong
     *
     */
    protected abstract DatabaseTableConfiguration getConfiguration()
            throws Exception;

    /**
     *
     * Create the test attribute sets
     *
     *
     *
     * @param cfg
     *
     * @return the initialized attribute set
     *
     * @throws Exception anything wrong
     *
     */
    protected abstract Set<Attribute> getCreateAttributeSet(DatabaseTableConfiguration cfg)
            throws Exception;

    /**
     * Create the test modify attribute set
     *
     * @param cfg the configuration
     * @return the initialized attribute set
     * @throws Exception anything wrong
     */
    protected abstract Set<Attribute> getModifyAttributeSet(DatabaseTableConfiguration cfg)
            throws Exception;

    /**
     * The class load method
     *
     * @param conn
     * @throws Exception
     */
    protected void deleteAllFromAccounts(DatabaseTableConnection conn)
            throws Exception {
        // update the last change
        final String SQL_TEMPLATE = "DELETE FROM Accounts";
        LOG.ok(SQL_TEMPLATE);
        PreparedStatement ps = null;
        try {
            ps = conn.getConnection().prepareStatement(SQL_TEMPLATE);
            ps.execute();
        } finally {
            SQLUtil.closeQuietly(ps);
        }
        conn.commit();
    }

    /**
     * The close connector after test method
     */
    @AfterEach
    public void disposeConnector() {
        LOG.ok("disposeConnector");
        if (con != null) {
            con.dispose();
            con = null;
        }
    }

    /**
     * test method
     *
     * @throws Exception
     */
    @Test
    public void testConfiguration() throws Exception {
        // attempt to test driver info..
        LOG.ok("testConfiguration");
        DatabaseTableConfiguration config = getConfiguration();
        config.validate();
    }

    /**
     *
     * test method
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testTestMethod() throws Exception {
        LOG.ok("testTestMethod");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        con.test();
    }

    /**
     * For testing purposes we creating connection an not the framework.
     *
     * @throws Exception
     */
    @Test
    public void testInvalidConnectionQuery() throws Exception {
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setValidConnectionQuery("INVALID");
        con = getConnector(cfg);
        assertThrows(ConnectorException.class, () -> con.test());
    }

    /**
     *
     * Make sure the Create call works..
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateEnabledEntry() throws Exception {
        LOG.ok("create enabled entry");
        final DatabaseTableConfiguration cfg = getConfiguration();
        final DatabaseTableConnector c = getConnector(cfg);
        deleteAllFromAccounts(c.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = c.create(ObjectClass.ACCOUNT, expected, null);
        // attempt to get the record back..
        final List<ConnectorObject> results = TestHelpers.searchToList(
                c, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertNotNull(co);
        assertTrue(AttributeUtil.isEnabled(co));
        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);
        attributeSetsEquals(c.schema(), expected, actual);
    }

    @Test
    public void testCreateDisabledEntry() throws Exception {
        LOG.ok("create disabled entry");
        final DatabaseTableConfiguration cfg = getConfiguration();
        final DatabaseTableConnector c = getConnector(cfg);
        deleteAllFromAccounts(c.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute status = AttributeUtil.find(OperationalAttributes.ENABLE_NAME, expected);
        if (status != null) {
            expected.remove(status);
        }
        expected.add(AttributeBuilder.buildEnabled(false));
        final Uid uid = c.create(ObjectClass.ACCOUNT, expected, null);
        // attempt to get the record back..
        final List<ConnectorObject> results = TestHelpers.searchToList(
                c, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertNotNull(co);
        assertFalse(AttributeUtil.isEnabled(co));
        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);
        attributeSetsEquals(c.schema(), expected, actual);
    }

    @Test
    public void testCreateWithoutEnabledAttribute() throws Exception {
        LOG.ok("create without __ENABLED__ attribute");
        final DatabaseTableConfiguration cfg = getConfiguration();
        final DatabaseTableConnector c = getConnector(cfg);
        deleteAllFromAccounts(c.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute status = AttributeUtil.find(OperationalAttributes.ENABLE_NAME, expected);
        if (status != null) {
            expected.remove(status);
        }
        final Uid uid = c.create(ObjectClass.ACCOUNT, expected, null);
        // attempt to get the record back..
        final List<ConnectorObject> results = TestHelpers.searchToList(
                c, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertNotNull(co);
        assertTrue(AttributeUtil.isEnabled(co));
        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);
        attributeSetsEquals(c.schema(), expected, actual,
                new String[] { OperationalAttributes.ENABLE_NAME });
    }

    @Test
    public void testCreateWithoutEnabledAttributeInConf() throws Exception {
        LOG.ok("create without __ENABLED__ attribute in configuration");
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setStatusColumn(null);
        cfg.setEnabledStatusValue(null);
        cfg.setDisabledStatusValue(null);
        cfg.setDefaultStatusValue(null);
        final DatabaseTableConnector c = getConnector(cfg);
        deleteAllFromAccounts(c.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute status = AttributeUtil.find(OperationalAttributes.ENABLE_NAME, expected);
        if (status != null) {
            expected.remove(status);
        }
        // this message should be ignored
        expected.add(AttributeBuilder.buildEnabled(false));
        final Uid uid = c.create(ObjectClass.ACCOUNT, expected, null);
        // attempt to get the record back..
        final List<ConnectorObject> results = TestHelpers.searchToList(
                c, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertNotNull(co);
        assertNull(AttributeUtil.isEnabled(co));
    }

    /**
     *
     * Make sure the Create call works..
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateCallNotNull() throws Exception {
        LOG.ok("testCreateCallNotNull");
        final DatabaseTableConfiguration cfg = getConfiguration();
        final DatabaseTableConnector c = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create modified attribute set
        final Map<String, Attribute> chMap = new HashMap<>(AttributeUtil.toMap(expected));
        chMap.put(FIRSTNAME, AttributeBuilder.build(FIRSTNAME, (String) null));
        final Set<Attribute> changeSet = CollectionUtil.newSet(chMap.values());
        assertThrows(ConnectorException.class, () -> c.create(ObjectClass.ACCOUNT, changeSet, null));
    }

    /**
     *
     * Make sure the Create call works..
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateCallNotNullEnableEmptyString() throws Exception {
        if (IS_EMPTY_STRING_SUPPORT) {
            LOG.ok("testCreateCallNotNullEnableEmptyString");
            DatabaseTableConfiguration cfg = getConfiguration();
            cfg.setEnableEmptyString(true);
            final DatabaseTableConnector c = getConnector(cfg);
            final Set<Attribute> expected = getCreateAttributeSet(cfg);
            // create modified attribute set
            final Map<String, Attribute> chMap = new HashMap<>(
                    AttributeUtil.toMap(expected));
            chMap.put(FIRSTNAME,
                    AttributeBuilder.build(FIRSTNAME, (String) null));
            chMap.put(LASTNAME, AttributeBuilder.build(LASTNAME, (String) null));
            final Set<Attribute> changeSet = CollectionUtil.newSet(
                    chMap.values());
            Uid uid = c.create(ObjectClass.ACCOUNT, changeSet, null);
            // attempt to get the record back..
            List<ConnectorObject> results = TestHelpers.searchToList(c,
                    ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
            assertEquals(1, results.size());
            final ConnectorObject co = results.get(0);
            assertNotNull(co);
            final Set<Attribute> actual = co.getAttributes();
            assertNotNull(actual);
            attributeSetsEquals(c.schema(), changeSet, actual, FIRSTNAME, LASTNAME);
        }
    }

    /**
     *
     * Make sure the Create call works..
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateUnsuported() throws Exception {
        LOG.ok("testCreateUnsuported");
        DatabaseTableConfiguration cfg = getConfiguration();
        DatabaseTableConnector c = getConnector(cfg);
        ObjectClass objClass = new ObjectClass("NOTSUPPORTED");
        assertThrows(IllegalArgumentException.class, () -> c.create(objClass, getCreateAttributeSet(cfg), null));
    }

    /**
     *
     * test method
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateWithName() throws Exception {
        LOG.ok("testCreateWithName");
        DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> attributes = getCreateAttributeSet(cfg);
        Name name = AttributeUtil.getNameFromAttributes(attributes);
        final Uid uid = con.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(name.getNameValue(), uid.getUidValue());
    }

    /**
     *
     * Test creating of the connector object, searching using UID and delete
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateAndDelete() throws Exception {
        LOG.ok("testCreateAndDelete");
        final String ERR1 = "Could not find new object.";
        final String ERR2 = "Found object that should not be there.";
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        try {
            // attempt to find the newly created object..
            List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
            assertEquals(1, list.size());
            //Test the created attributes are equal the searched
            final ConnectorObject co = list.get(0);
            assertNotNull(co);
            final Set<Attribute> actual = co.getAttributes();
            assertNotNull(actual);
            attributeSetsEquals(con.schema(), expected, actual);
        } finally {
            // attempt to delete the object..
            con.delete(ObjectClass.ACCOUNT, uid, null);
            // attempt to find it again to make sure
            // it actually deleted the object..
            // attempt to find the newly created object..
            List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
            assertTrue(list.isEmpty());
            try {
                // now attempt to delete an object that is not there..
                con.delete(ObjectClass.ACCOUNT, uid, null);
                fail("Should have thrown an execption.");
            } catch (UnknownUidException exp) {
                // should get here..
            }
        }
    }

    /**
     *
     * Test creating of the connector object, searching using UID and delete
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testDeleteUnsupported() throws Exception {
        LOG.ok("testDeleteUnsupported");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        try {
            // attempt to find the newly created object..
            List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
            assertEquals(1, list.size());
        } finally {
            // attempt to delete the object..
            ObjectClass objc = new ObjectClass("UNSUPPORTED");
            assertThrows(IllegalArgumentException.class, () -> con.delete(objc, uid, null));
        }
    }

    /**
     *
     * Test creating of the connector object, searching using UID and update
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testUpdateUnsupported() throws Exception {
        LOG.ok("testUpdateUnsupported");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        // create updated connector object
        Set<Attribute> changeSet = getModifyAttributeSet(cfg);
        ObjectClass objClass = new ObjectClass("NOTSUPPORTED");
        assertThrows(IllegalArgumentException.class, () -> con.update(objClass, uid, changeSet, null));
    }

    /**
     *
     * Test creating of the connector object, searching using UID and update
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testUpdateNull()
            throws Exception {
        LOG.ok("testUpdateNull");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        List<ConnectorObject> list = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        // create updated connector object
        Map<String, Attribute> chMap = new HashMap<String, Attribute>(AttributeUtil.toMap(expected));
        chMap.put(SALARY, AttributeBuilder.build(SALARY, (Integer) null));
        // do the update
        final Set<Attribute> changeSet = CollectionUtil.newSet(chMap.values());
        con.update(ObjectClass.ACCOUNT, uid, changeSet, null);
        // retrieve the object
        List<ConnectorObject> list2 = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertNotNull(list2);
        assertTrue(list2.size() == 1);
        final Set<Attribute> actual = list2.get(0).getAttributes();
        attributeSetsEquals(con.schema(), changeSet, actual, SALARY);
    }

    /**
     *
     * Test creating of the connector object, searching using UID and update
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testCreateAndUpdate()
            throws Exception {
        LOG.ok("testCreateAndUpdate");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        List<ConnectorObject> list = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        // create updated connector object
        final Set<Attribute> changeSet = getModifyAttributeSet(cfg);
        uid = con.update(ObjectClass.ACCOUNT, uid, changeSet, null);
        // retrieve the object
        List<ConnectorObject> list2 = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertNotNull(list2);
        assertTrue(list2.size() == 1);
        final Set<Attribute> actual = list2.get(0).getAttributes();
        attributeSetsEquals(con.schema(), changeSet, actual);
    }

    /**
     * Test method for Test creating of the connector object, searching using UID and update
     *
     * @throws Exception
     */
    @Test
    public void testAuthenticateOriginal() throws Exception {
        LOG.ok("testAuthenticateOriginal");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        // check if authenticate operation is present (it should)
        final Schema schema = con.schema();
        Set<ObjectClassInfo> oci = schema.getSupportedObjectClassesByOperation(AuthenticationApiOp.class);
        assertTrue(oci.size() >= 1);
        // this should not throw any RuntimeException, on invalid authentication
        final Name name = AttributeUtil.getNameFromAttributes(expected);
        final GuardedString passwordValue = AttributeUtil.getPasswordValue(expected);
        final Uid auid = con.authenticate(ObjectClass.ACCOUNT, name.getNameValue(), passwordValue, null);
        assertEquals(uid, auid);
        // cleanup (should not throw any exception.)
        con.delete(ObjectClass.ACCOUNT, uid, null);
    }

    /**
     * Test method for Test creating of the connector object, searching using UID and update
     *
     * @throws Exception
     */
    @Test
    public void testResolveUsernameOriginal()
            throws Exception {
        LOG.ok("testAuthenticateOriginal");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        // check if authenticate operation is present (it should)
        Schema schema = con.schema();
        Set<ObjectClassInfo> oci = schema.getSupportedObjectClassesByOperation(AuthenticationApiOp.class);
        assertTrue(oci.size() >= 1);
        // this should not throw any RuntimeException, on invalid authentication
        final Name name = AttributeUtil.getNameFromAttributes(expected);
        final Uid auid = con.resolveUsername(ObjectClass.ACCOUNT, name.getNameValue(), null);
        assertEquals(uid, auid);
        // cleanup (should not throw any exception.)
        con.delete(ObjectClass.ACCOUNT, uid, null);
    }

    @Test
    public void testAuthenticateWrongOriginal() throws Exception {
        LOG.ok("testAuthenticateOriginal");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        // this should throw InvalidCredentials exception, as we query a non-existing user
        assertThrows(
                InvalidCredentialException.class,
                () -> con.authenticate(ObjectClass.ACCOUNT, "NON", new GuardedString("MOM".toCharArray()), null));
    }

    @Test
    public void testResolveUsernameWrongOriginal()
            throws Exception {
        LOG.ok("testAuthenticateOriginal");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        // this should throw InvalidCredentials exception, as we query a
        // non-existing user
        assertThrows(InvalidCredentialException.class, () -> con.resolveUsername(ObjectClass.ACCOUNT, "WRONG", null));
    }

    @Test
    public void testNoPassColumnAutenticate() throws Exception {
        LOG.ok("testNoPassColumnAutenticate");
        final DatabaseTableConfiguration cfg = getConfiguration();
        // Erasing password column from the configuration (it will be no longer treated as special attribute).
        cfg.setPasswordColumn(null);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        con = getConnector(cfg);
        // note: toAttributeSet(false), where false means, password will not be
        // treated as special attribute.
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // check if authenticate operation is present (it should NOT!)
        Schema schema = con.schema();
        Set<ObjectClassInfo> oci = schema.getSupportedObjectClassesByOperation(AuthenticationApiOp.class);
        assertTrue(oci.isEmpty());
        // authentication should not be allowed -- will throw an
        // IllegalArgumentException
        // this should not throw any RuntimeException, on invalid authentication
        final Name name = AttributeUtil.getNameFromAttributes(expected);
        final GuardedString passwordValue = AttributeUtil.getPasswordValue(expected);
        assertThrows(
                UnsupportedOperationException.class,
                () -> con.authenticate(ObjectClass.ACCOUNT, name.getNameValue(), passwordValue, null));
        // cleanup (should not throw any exception.)
        con.delete(ObjectClass.ACCOUNT, uid, null);
    }

    @Test
    public void testSearchByName() throws Exception {
        LOG.ok("testSearchByName");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // retrieve the object
        final List<ConnectorObject> list = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid));
        assertEquals(1, list.size());
        final ConnectorObject actual = list.get(0);
        assertNotNull(actual);
        attributeSetsEquals(con.schema(), expected, actual.getAttributes());
    }

    /**
     * Test method to issue #238
     *
     * @throws Exception
     */
    @Test
    public void testSearchWithNullPassword() throws Exception {
        LOG.ok("testSearchWithNullPassword");
        final String SQL_TEMPLATE = "UPDATE {0} SET password = null WHERE {1} = ?";
        final DatabaseTableConfiguration cfg = getConfiguration();
        final String sql = MessageFormat.format(SQL_TEMPLATE, cfg.getTable(), cfg.getKeyColumn());
        con = getConnector(cfg);
        PreparedStatement ps = null;
        final DatabaseTableConnection conn = DatabaseTableConnection.createDBTableConnection(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        //set password to null
        //expected.setPassword((String) null);
        try {
            final List<SQLParam> values = new ArrayList<>();
            values.add(new SQLParam("user", uid.getUidValue(), Types.VARCHAR));
            ps = conn.prepareStatement(sql, values);
            ps.execute();
            conn.commit();
        } finally {
            SQLUtil.closeQuietly(ps);
        }
        // attempt to get the record back..
        List<ConnectorObject> results = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
        assertEquals(1, results.size());
        final Set<Attribute> attributes = results.get(0).getAttributes();
        attributeSetsEquals(con.schema(), expected, attributes);
    }

    /**
     * Test method, issue #186
     *
     * @throws Exception
     */
    @Test
    public void testSearchByNameAttributesToGet() throws Exception {
        LOG.ok("testSearchByNameAttributesToGet");
        // create connector
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // attempt to get the record back..
        final OperationOptionsBuilder opOption = new OperationOptionsBuilder();
        opOption.setAttributesToGet(FIRSTNAME, LASTNAME, MANAGER, CHANGELOG);
        final List<ConnectorObject> results = TestHelpers.searchToList(
                con,
                ObjectClass.ACCOUNT,
                FilterBuilder.equalTo(uid),
                opOption.build());
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertEquals(uid.getUidValue(), co.getUid().getUidValue());
        assertEquals(uid.getUidValue(), co.getName().getNameValue());
        final Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);
        assertNull(AttributeUtil.find(AGE, actual));
        assertNull(AttributeUtil.find(DEPARTMENT, actual));
        assertNull(AttributeUtil.find(EMAIL, actual));
        assertNotNull(AttributeUtil.find(FIRSTNAME, actual));
        assertNotNull(AttributeUtil.find(LASTNAME, actual));
        assertNotNull(AttributeUtil.find(MANAGER, actual));
        assertNull(AttributeUtil.find(MIDDLENAME, actual));
        assertNull(AttributeUtil.find(SALARY, actual));
        assertNull(AttributeUtil.find(TITLE, actual));
        assertNull(AttributeUtil.find(JPEGPHOTO, actual));
        assertNull(AttributeUtil.find(CHANGED, actual));
        // https://connid.atlassian.net/browse/DB-10
        assertNotNull(AttributeUtil.find(CHANGELOG, actual));
    }

    /**
     *
     * Test method, issue #186
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testSearchByNameAttributesToGetExtended()
            throws Exception {
        LOG.ok("testSearchByNameAttributesToGetExtended");
        // create connector
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        deleteAllFromAccounts(con.getConn());
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        // attempt to get the record back..
        OperationOptionsBuilder opOption = new OperationOptionsBuilder();
        opOption.setAttributesToGet(FIRSTNAME, LASTNAME, MANAGER, JPEGPHOTO);
        List<ConnectorObject> results = TestHelpers.searchToList(con,
                ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid),
                opOption.build());
        assertEquals(1, results.size());
        final ConnectorObject co = results.get(0);
        assertEquals(uid.getUidValue(), co.getUid().getUidValue());
        assertEquals(uid.getUidValue(), co.getName().getNameValue());
        Set<Attribute> actual = co.getAttributes();
        assertNotNull(actual);
        assertNull(AttributeUtil.find(AGE, actual));
        assertNull(AttributeUtil.find(DEPARTMENT, actual));
        assertNull(AttributeUtil.find(EMAIL, actual));
        assertNotNull(AttributeUtil.find(FIRSTNAME, actual));
        assertNotNull(AttributeUtil.find(LASTNAME, actual));
        assertNotNull(AttributeUtil.find(MANAGER, actual));
        assertNull(AttributeUtil.find(MIDDLENAME, actual));
        assertNull(AttributeUtil.find(SALARY, actual));
        assertNull(AttributeUtil.find(TITLE, actual));
        assertNotNull(AttributeUtil.find(JPEGPHOTO, actual));
        assertEquals(AttributeUtil.find(JPEGPHOTO, expected),
                AttributeUtil.find(JPEGPHOTO, actual));
    }

    // TEest SYNCmethod    
    /**
     *
     * Test creating of the connector object, searching using UID and delete
     *
     *
     *
     * @throws Exception
     *
     */
    @Test
    public void testSyncFull() throws Exception {
        // create connector
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, expected);
        if (password != null) {
            expected.remove(password);
        }
        expected.add(AttributeBuilder.buildPassword(new GuardedString("password".toCharArray())));
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        try {
            FindUidSyncHandler handler = new FindUidSyncHandler(uid);
            // attempt to find the newly created object..
            con.sync(ObjectClass.ACCOUNT, null, handler, null);
            assertTrue(handler.found);
            assertEquals(0L, handler.token.getValue());
            //Test the created attributes are equal the searched
            assertNotNull(handler.attributes);
            // ------------------------
            // https://connid.atlassian.net/browse/DB-10
            // ------------------------
            final Attribute clAttr = AttributeUtil.find(CHANGELOG, handler.attributes);
            assertNotNull(clAttr);
            final Set<Attribute> res = new HashSet<>(handler.attributes);
            res.remove(clAttr);
            // ------------------------
            attributeSetsEquals(con.schema(), expected, res);
            // --------------------------------------------
            // Verify password synchronization
            // --------------------------------------------
            final Attribute pwd = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, handler.attributes);
            assertNotNull(pwd);
            assertNotNull(pwd.getValue());
            assertEquals(1, pwd.getValue().size());
            final GuardedString guarded = (GuardedString) pwd.getValue().get(0);
            guarded.access(clearChars -> assertEquals("password", new String(clearChars)));
            // --------------------------------------------
        } finally {
            // attempt to delete the object..
            con.delete(ObjectClass.ACCOUNT, uid, null);
            // attempt to find it again to make sure
            // attempt to find the newly created object..
            List<ConnectorObject> results =
                    TestHelpers.searchToList(con, ObjectClass.ACCOUNT, FilterBuilder.equalTo(uid));
            assertNotEquals(1, results.size());
            try {
                // now attempt to delete an object that is not there..
                con.delete(ObjectClass.ACCOUNT, uid, null);
                fail("Should have thrown an execption.");
            } catch (UnknownUidException exp) {
                // should get here..
            }
        }
    }

    /**
     * Test creating of the connector object, searching using UID and delete
     *
     * @throws Exception
     * @throws SQLException
     */
    @Test
    public void testSyncIncremental() throws Exception {
        final String SQL_TEMPLATE = "UPDATE Accounts SET changelog = ? WHERE accountId = ?";
        // create connector
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        // create the object
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        assertNotNull(uid);
        final Long changelog = 10L;
        // update the last change
        PreparedStatement ps = null;
        DatabaseTableConnection conn = DatabaseTableConnection.createDBTableConnection(cfg);
        try {
            final List<SQLParam> values = new ArrayList<>();
            final int sqlType = con.getColumnType("changelog");
            Object tokenVal;
            try {
                tokenVal = SQLUtil.attribute2jdbcValue(changelog.toString(), sqlType);
            } catch (Exception e) {
                tokenVal = new Timestamp(tsAsLong(changelog.toString()));
            }
            values.add(new SQLParam("changelog", tokenVal, sqlType));
            values.add(new SQLParam("accountId", uid.getUidValue(), Types.VARCHAR));
            ps = conn.prepareStatement(SQL_TEMPLATE, values);
            ps.execute();
            conn.commit();
        } finally {
            SQLUtil.closeQuietly(ps);
        }
        FindUidSyncHandler ok = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, new SyncToken(changelog - 1), ok, null);
        assertTrue(ok.found);
        // Test the created attributes are equal the searched
        assertNotNull(ok.attributes);
        // ------------------------
        // https://connid.atlassian.net/browse/DB-10
        // ------------------------
        final Attribute clAttr = AttributeUtil.find(CHANGELOG, ok.attributes);
        assertNotNull(clAttr);
        final Set<Attribute> res = new HashSet<>(ok.attributes);
        res.remove(clAttr);
        // ------------------------
        attributeSetsEquals(con.schema(), expected, res);
        //Not in the next result
        FindUidSyncHandler empt = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, ok.token, empt, null);
        assertFalse(empt.found);
    }

    /**
     *
     * Test creating of the connector object, searching using UID and delete
     *
     *
     *
     * @throws Exception
     *
     * @throws SQLException
     *
     */
    @Test
    public void testSyncUsingIntegerColumn() throws Exception {
        final String SQL_TEMPLATE = "UPDATE Accounts SET age = ? WHERE accountId = ?";
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setChangeLogColumn(AGE);
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        // update the last change
        PreparedStatement ps = null;
        final DatabaseTableConnection conn = DatabaseTableConnection.createDBTableConnection(cfg);
        final Integer changed = Long.valueOf(System.currentTimeMillis()).intValue();
        try {
            final List<SQLParam> values = new ArrayList<>();
            values.add(new SQLParam("age", changed, Types.INTEGER));
            values.add(new SQLParam("accountId", uid.getUidValue(), Types.VARCHAR));
            ps = conn.prepareStatement(SQL_TEMPLATE, values);
            ps.execute();
            conn.commit();
        } finally {
            SQLUtil.closeQuietly(ps);
        }
        FindUidSyncHandler ok = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, new SyncToken(changed - 1000), ok, null);
        assertTrue(ok.found);
        // Test the created attributes are equal the searched
        assertNotNull(ok.attributes);
        attributeSetsEquals(con.schema(), expected, ok.attributes, AGE);
        FindUidSyncHandler empt = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, ok.token, empt, null);
        assertFalse(empt.found);
    }

    /**
     *
     * Test creating of the connector object, searching using UID and delete
     *
     *
     *
     * @throws Exception
     *
     * @throws SQLException
     *
     */
    @Test
    public void testSyncUsingLongColumn() throws Exception {
        final String SQL_TEMPLATE = "UPDATE Accounts SET accessed = ? WHERE accountId = ?";
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setChangeLogColumn(ACCESSED);
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        // update the last change
        PreparedStatement ps = null;
        final DatabaseTableConnection conn = DatabaseTableConnection.createDBTableConnection(cfg);
        final Integer changed = Long.valueOf(System.currentTimeMillis()).intValue();
        try {
            final List<SQLParam> values = new ArrayList<>();
            values.add(new SQLParam("accessed", changed, Types.INTEGER));
            values.add(new SQLParam("accountId", uid.getUidValue(), Types.VARCHAR));
            ps = conn.prepareStatement(SQL_TEMPLATE, values);
            ps.execute();
            conn.commit();
        } finally {
            SQLUtil.closeQuietly(ps);
        }
        FindUidSyncHandler ok = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, new SyncToken(changed - 1000), ok, null);
        assertTrue(ok.found);
        // Test the created attributes are equal the searched
        assertNotNull(ok.attributes);
        attributeSetsEquals(con.schema(), expected, ok.attributes, ACCESSED);
        FindUidSyncHandler empt = new FindUidSyncHandler(uid);
        // attempt to find the newly created object..
        con.sync(ObjectClass.ACCOUNT, ok.token, empt, null);
        assertFalse(empt.found);
    }

    @Test
    public void testPwdNotReversibleAlgorithm()
            throws Exception {
        LOG.ok("testPasswordManagement");
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setCipherAlgorithm("MD5");
        cfg.setCipherKey(null);
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, expected);
        if (password != null) {
            expected.remove(password);
        }
        expected.add(AttributeBuilder.buildPassword(
                new GuardedString("password".toCharArray())));
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        try {
            final OperationOptionsBuilder op = new OperationOptionsBuilder();
            op.setAttributesToGet(OperationalAttributes.PASSWORD_NAME);
            List<ConnectorObject> rs = TestHelpers.searchToList(
                    con, ObjectClass.ACCOUNT, new EqualsFilter(uid), op.build());
            assertNotNull(rs);
            assertEquals(1, rs.size());
            //Test the created attributes are equal the searched
            ConnectorObject co = rs.get(0);
            assertNotNull(co);
            Attribute actual = co.getAttributeByName(OperationalAttributes.PASSWORD_NAME);
            assertNotNull(actual);
            assertNotNull(actual.getValue());
            assertEquals(1, actual.getValue().size());
            final GuardedString guarded = (GuardedString) actual.getValue().get(0);
            guarded.access(clearChars -> {
                String md5str = null;
                try {
                    md5str = new MD5().encode("password", PASSWORD_CHARSETNAME);
                    assertFalse("password".equalsIgnoreCase(md5str));
                } catch (PasswordEncodingException ex) {
                    assertFalse(true);
                } catch (UnsupportedPasswordCharsetException upce) {
                    assertFalse(true);
                }
                assertNotNull(md5str);
                assertEquals(md5str, new String(clearChars));
            });
            final Set<Attribute> changeSet = new HashSet<>();
            changeSet.add(AttributeBuilder.buildPassword("123pwd".toCharArray()));
            con.update(ObjectClass.ACCOUNT, uid, changeSet, null);
            rs = TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid), op.build());
            co = rs.get(0);
            actual = co.getAttributeByName(OperationalAttributes.PASSWORD_NAME);
            ((GuardedString) actual.getValue().get(0)).access(clearChars -> {
                String md5str = null;
                try {
                    md5str = new MD5().encode("123pwd", PASSWORD_CHARSETNAME);
                    assertFalse("123pwd".equalsIgnoreCase(md5str));
                } catch (PasswordEncodingException ex) {
                    assertFalse(true);
                } catch (UnsupportedPasswordCharsetException upce) {
                    assertFalse(true);
                }
                assertNotNull(md5str);
                assertEquals(md5str, new String(clearChars));
            });
        } finally {
            con.delete(ObjectClass.ACCOUNT, uid, null);
        }
    }

    @Test
    public void testPwdEncodeUpperCase()
            throws Exception {
        LOG.ok("testPasswordEncodeToUpperCase");
        final DatabaseTableConfiguration cfg = getConfiguration();
        cfg.setCipherAlgorithm("MD5");
        cfg.setCipherKey(null);
        cfg.setPwdEncodeToUpperCase(true);
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, expected);
        if (password != null) {
            expected.remove(password);
        }
        expected.add(AttributeBuilder.buildPassword(new GuardedString("password".toCharArray())));
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        try {
            final OperationOptionsBuilder op = new OperationOptionsBuilder();
            op.setAttributesToGet(OperationalAttributes.PASSWORD_NAME);
            List<ConnectorObject> rs =
                    TestHelpers.searchToList(con, ObjectClass.ACCOUNT, new EqualsFilter(uid), op.build());
            assertNotNull(rs);
            assertEquals(1, rs.size());
            //Test the created attributes are equal the searched
            ConnectorObject co = rs.get(0);
            assertNotNull(co);
            Attribute actual = co.getAttributeByName(OperationalAttributes.PASSWORD_NAME);
            assertNotNull(actual);
            assertNotNull(actual.getValue());
            assertEquals(1, actual.getValue().size());
            final GuardedString guarded = (GuardedString) actual.getValue().get(0);
            guarded.access(clearChars -> {
                String md5str = null;
                try {
                    md5str = new MD5().encode("password", PASSWORD_CHARSETNAME);
                    assertFalse("password".equalsIgnoreCase(md5str));
                } catch (PasswordEncodingException ex) {
                    assertFalse(true);
                } catch (UnsupportedPasswordCharsetException upce) {
                    assertFalse(true);
                }
                assertNotNull(md5str);
                assertFalse(md5str.equals(new String(clearChars)));
                assertEquals(md5str.toUpperCase(), new String(clearChars));
            });
        } finally {
            con.delete(ObjectClass.ACCOUNT, uid, null);
        }
    }

    @Test
    public void testPwdReversibleAlgorithm()
            throws Exception {
        LOG.ok("testPasswordManagement");
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Set<Attribute> expected = getCreateAttributeSet(cfg);
        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, expected);
        if (password != null) {
            expected.remove(password);
        }
        expected.add(AttributeBuilder.buildPassword(
                new GuardedString("password".toCharArray())));
        final Uid uid = con.create(ObjectClass.ACCOUNT, expected, null);
        try {
            final OperationOptionsBuilder op = new OperationOptionsBuilder();
            op.setAttributesToGet(OperationalAttributes.PASSWORD_NAME);
            List<ConnectorObject> rs = TestHelpers.searchToList(
                    con, ObjectClass.ACCOUNT, new EqualsFilter(uid), op.build());
            assertNotNull(rs);
            assertEquals(1, rs.size());
            //Test the created attributes are equal the searched
            ConnectorObject co = rs.get(0);
            assertNotNull(co);
            Attribute actual = co.getAttributeByName(OperationalAttributes.PASSWORD_NAME);
            assertNotNull(actual);
            assertNotNull(actual.getValue());
            assertEquals(1, actual.getValue().size());
            final GuardedString guarded = (GuardedString) actual.getValue().get(0);
            guarded.access(clearChars -> assertEquals("password", new String(clearChars)));
            final Set<Attribute> changeSet = new HashSet<>();
            changeSet.add(AttributeBuilder.buildPassword("123pwd".toCharArray()));
            con.update(ObjectClass.ACCOUNT, uid, changeSet, null);
            rs = TestHelpers.searchToList(
                    con, ObjectClass.ACCOUNT, new EqualsFilter(uid), op.build());
            co = rs.get(0);
            actual = co.getAttributeByName(OperationalAttributes.PASSWORD_NAME);
            ((GuardedString) actual.getValue().get(0)).
                    access(clearChars -> assertEquals("123pwd", new String(clearChars)));
        } finally {
            con.delete(ObjectClass.ACCOUNT, uid, null);
        }
    }

    // Helper Methods/Classes
    /**
     * @param cfg
     * @return the connector
     */
    protected DatabaseTableConnector getConnector(DatabaseTableConfiguration cfg) {
        con = new DatabaseTableConnector();
        con.init(cfg);
        return con;
    }

    /**
     * @param schema a schema
     * @param expected an expected value
     * @param actual an actual value
     * @param ignore ignore list
     */
    protected void attributeSetsEquals(
            final Schema schema,
            final Set<Attribute> expected,
            final Set<Attribute> actual,
            final String... ignore) {
        attributeSetsEquals(
                schema,
                AttributeUtil.toMap(expected),
                AttributeUtil.toMap(actual),
                ignore);
    }

    /**
     * @param schema a schema
     * @param expMap an expected value map
     * @param actMap an actual value map
     * @param ignore ignore list
     */
    protected void attributeSetsEquals(
            final Schema schema,
            final Map<String, Attribute> expMap,
            final Map<String, Attribute> actMap,
            final String... ignore) {

        LOG.ok("attributeSetsEquals");
        final Set<String> ignoreSet = new HashSet<>(Arrays.asList(ignore));
        if (schema != null) {
            final ObjectClassInfo oci = schema.findObjectClassInfo(ObjectClass.ACCOUNT_NAME);
            final Set<AttributeInfo> ais = oci.getAttributeInfo();
            for (AttributeInfo ai : ais) {
                //ignore not returned by default
                if (!ai.isReturnedByDefault()) {
                    ignoreSet.add(ai.getName());
                }
                //ignore not readable attributes
                if (!ai.isReadable()) {
                    ignoreSet.add(ai.getName());
                }
            }
        }
        final Set<String> names = CollectionUtil.newCaseInsensitiveSet();
        names.addAll(expMap.keySet());
        names.addAll(actMap.keySet());
        names.removeAll(ignoreSet);
        names.remove(Uid.NAME);
        //names.remove(KEYCOLUMN);
        int missing = 0;
        final List<String> mis = new ArrayList<>();
        final List<String> extra = new ArrayList<>();
        for (String attrName : names) {
            final Attribute expAttr = expMap.get(attrName);
            final Attribute actAttr = actMap.get(attrName);
            if (expAttr != null && actAttr != null) {
                // This check is necessary to have the possibility to assert 
                // equality between timestamp field actual and expected values.
                // For instance, MySql and PostgreSQL handle timestamp fields
                // with a little difference about milliseconds.
                if (CHANGED.equalsIgnoreCase(attrName) || CHANGELOG.equalsIgnoreCase(attrName)) {
                    assertEquals(
                            SQLUtil.string2Timestamp(AttributeUtil.getSingleValue(expAttr).toString()),
                            SQLUtil.string2Timestamp(AttributeUtil.getSingleValue(actAttr).toString()));
                } else if (OPENTIME.equalsIgnoreCase(attrName) || ACTIVATE.equalsIgnoreCase(attrName)) {
                    assertEquals(
                            tsAsLong(AttributeUtil.getStringValue(expAttr)),
                            tsAsLong(AttributeUtil.getStringValue(actAttr)));
                } else {
                    assertEquals(
                            AttributeUtil.getSingleValue(expAttr).toString(),
                            AttributeUtil.getSingleValue(actAttr).toString());
                }
            } else {
                missing = missing + 1;
                if (expAttr != null) {
                    mis.add(expAttr.getName());
                }
                if (actAttr != null) {
                    extra.add(actAttr.getName());
                }
            }
        }
        assertEquals(0, missing);
        LOG.ok("attributeSets are equal!");
    }

    protected static class FindUidSyncHandler implements SyncResultsHandler {

        /**
         * Determines if found..
         */
        public boolean found = false;

        /**
         * Uid to find.
         */
        public final Uid uid;

        public SyncDeltaType deltaType;

        /**
         * Sync token to find
         */
        public SyncToken token;

        /**
         * Attribute set to find
         */
        public Set<Attribute> attributes = null;

        /**
         * @param uid
         */
        public FindUidSyncHandler(Uid uid) {
            this.uid = uid;
        }

        /*
         * (non-Javadoc) @see
         * org.identityconnectors.framework.common.objects.SyncResultsHandler#handle(org.identityconnectors.framework.common.objects.SyncDelta)
         */
        @Override
        public boolean handle(SyncDelta delta) {
            if (delta.getUid().equals(uid)) {
                found = true;
                this.attributes = delta.getObject().getAttributes();
                this.deltaType = delta.getDeltaType();
                this.token = delta.getToken();
                return false;
            }
            return true;
        }
    }

    @Test
    public void schema() throws Exception {
        final DatabaseTableConfiguration cfg = getConfiguration();
        con = getConnector(cfg);
        final Schema schema = con.schema();
        assertNotNull(schema);
        final ObjectClassInfo info = schema.findObjectClassInfo(ObjectClass.ACCOUNT_NAME);
        assertNotNull(info);
        assertNotNull(info.getAttributeInfo());
        assertFalse(info.getAttributeInfo().isEmpty());
        assertNotNull(schema.getOperationOptionInfo());
        boolean changelog = false;
        for (AttributeInfo attrInfo : info.getAttributeInfo()) {
            if (CHANGELOG.equalsIgnoreCase(attrInfo.getName())) {
                changelog = true;
                assertEquals(String.class, attrInfo.getType());
            }
        }
        assertFalse(changelog);
    }
}
