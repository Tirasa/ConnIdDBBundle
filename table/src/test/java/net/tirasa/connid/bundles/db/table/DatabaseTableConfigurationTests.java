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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import net.tirasa.connid.bundles.db.table.security.SupportedAlgorithm;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.Test;

/**
 * Attempts to test the Connector with the framework.
 */
public class DatabaseTableConfigurationTests {

    /**
     * Derby's embedded driver.
     */
    static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    static final String USER = "tstUser";

    static final GuardedString PASSWORD = new GuardedString("tstPAssword".toCharArray());

    static final String DBTABLE = "tstTable";

    static final String HOST = "tstHost";

    static final String PORT = "8000";

    static final String DATABASE = "tstDatabase";

    static final String KEYCOLUMN = "tstKeyColumn";

    static final String PASSDCOLUMN = "tstPasswordColumn";

    final static String PASSWORD_CHARSETNAME = "UTF-8";

    static final String CHANGELOG = "tstChangelogColumn";

    static final String URL = "jdbc:derby:@tstHost:8000:tstDatabase";

    static final String URLTEMPLATE = "jdbc:derby:@%h:%p:%d";

    /**
     * The configuration
     *
     * @return see {@link DatabaseTableConfiguration}
     */
    protected DatabaseTableConfiguration getConfiguration() {
        DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        config.setJdbcDriver(DRIVER);
        config.setUser(USER);
        config.setPassword(PASSWORD);
        config.setPasswordCharset(PASSWORD_CHARSETNAME);
        config.setTable(DBTABLE);
        config.setHost(HOST);
        config.setPort(PORT);
        config.setDatabase(DATABASE);
        config.setKeyColumn(KEYCOLUMN);
        config.setPasswordColumn(PASSDCOLUMN);
        config.setChangeLogColumn(CHANGELOG);
        config.setJdbcUrlTemplate(URLTEMPLATE);
        config.setConnectorMessages(TestHelpers.createDummyMessages());
        return config;
    }

    /**
     * test method
     */
    @Test
    public void testConfiguration() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.validate();
    }

    /**
     * test method
     */
    @Test
    public void testFormatUrl() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        final String url = config.formatUrlTemplate();
        assertEquals(URL, url);
    }

    /**
     * test method
     */
    @Test
    public void testGetSetTheProperties() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setAllNative(true);
        assertEquals(true, config.isAllNative());
        config.setChangeLogColumn("TST");
        assertEquals("TST", config.getChangeLogColumn());
        config.setDatabase("DB");
        assertEquals("DB", config.getDatabase());
        config.setDatasource("DS");
        assertEquals("DS", config.getDatasource());
        config.setEnableEmptyString(true);
        assertEquals(true, config.isEnableEmptyString());
        config.setHost("HS");
        assertEquals("HS", config.getHost());
        config.setJdbcDriver("DRV");
        assertEquals("DRV", config.getJdbcDriver());
        config.setJdbcUrlTemplate("TMP");
        assertEquals("TMP", config.getJdbcUrlTemplate());
        config.setKeyColumn("KEY");
        assertEquals("KEY", config.getKeyColumn());
        config.setNativeTimestamps(true);
        assertEquals(true, config.isNativeTimestamps());
        config.setPasswordColumn("PWC");
        assertEquals("PWC", config.getPasswordColumn());
        config.setPort("80");
        assertEquals("80", config.getPort());
        config.setQuoting("double");
        assertEquals("double", config.getQuoting());
        config.setRethrowAllSQLExceptions(false);
        assertEquals(false, config.isRethrowAllSQLExceptions());
        config.setTable("TB");
        assertEquals("TB", config.getTable());
        config.setUser("USR");
        assertEquals("USR", config.getUser());
        config.setValidConnectionQuery("VALID");
        assertEquals("VALID", config.getValidConnectionQuery());
    }

    @Test
    public void testConfigurationEmptyHost() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        config.setHost("");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationEmptyPort() {
        DatabaseTableConfiguration config = getConfiguration();
        config.setPort("");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationEmptyDatabase() {
        DatabaseTableConfiguration config = getConfiguration();
        config.setDatabase("");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationKeyEQPassword() {
        DatabaseTableConfiguration config = getConfiguration();
        config.setKeyColumn("a");
        config.setPasswordColumn("a");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationKeyEQChange() {
        DatabaseTableConfiguration config = getConfiguration();
        config.setKeyColumn("a");
        config.setChangeLogColumn("a");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationPasswdEQChange() {
        DatabaseTableConfiguration config = getConfiguration();
        config.setPasswordColumn("a");
        config.setChangeLogColumn("a");
        // check defaults..
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationDataSource() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setDatasource("DS");
        config.validate();
    }

    @Test
    public void testConfigurationJndi() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setDatasource("DS");
        assertEquals("DS", config.getDatasource());
        final String[] tstpr = { "a=A", "b=B" };
        config.setJndiProperties(tstpr);
        assertEquals(tstpr[0], config.getJndiProperties()[0]);
        assertEquals(tstpr[1], config.getJndiProperties()[1]);
        config.validate();
    }

    @Test
    public void testConfigurationInvalidJndi() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setDatasource("DS");
        final String[] tstpr = { "a=A", "b" };
        config.setJndiProperties(tstpr);
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }

    @Test
    public void testConfigurationEmptyCipherAlgorithm() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.validate();
        assertEquals(SupportedAlgorithm.CLEARTEXT.name(), config.getCipherAlgorithm());
    }

    /**
     * test method password default charset
     */
    @Test
    public void testConfigurationEmptyPasswordCharset() {
        // attempt to test driver info..
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.validate();
        assertEquals("UTF-8", config.getPasswordCharset());
    }

    /**
     * test allowed password charset
     */
    @Test
    public void testConfigurationSupportedCharset() {
        // attempt to test password Charset
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setPasswordCharset("ISO-8859-15");
        config.validate();
    }

    /**
     * test method unsupported charset
     */
    @Test
    public void testConfigurationUnsupportedCharset() {
        // attempt to test password Charset
        DatabaseTableConfiguration config = getConfiguration();
        // check defaults..
        config.setPasswordCharset("UTF-9");
        assertThrows(IllegalArgumentException.class, () -> config.validate());
    }
}
