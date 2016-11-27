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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.sql.DataSource;
import net.tirasa.connid.bundles.db.table.security.AES;
import org.identityconnectors.test.common.TestHelpers;

/**
 * Attempts to test the Connector with the framework.
 */
public class DatabaseTableDSTests extends DatabaseTableTests {

    /**
     *
     * Derby's embedded ds.
     *
     */
    private static final String TEST_DS = "testDS";

    //jndi for datasource
    private static final String[] jndiProperties = new String[] {
        "java.naming.factory.initial=" + MockContextFactory.class.getName() };

    /**
     *
     * Create the test configuration
     *
     * @return the initialized configuration
     *
     */
    @Override
    protected DatabaseTableConfiguration getConfiguration() throws Exception {
        final DatabaseTableConfiguration config = new DatabaseTableConfiguration();
        config.setJdbcDriver(DRIVER);
        config.setDatasource(TEST_DS);
        config.setTable(DB_TABLE);
        config.setJndiProperties(jndiProperties);
        config.setChangeLogColumn(CHANGELOG);
        config.setKeyColumn(KEYCOLUMN);
        config.setPasswordColumn(PASSWORDCOLUMN);
        config.setPasswordCharset(PASSWORD_CHARSETNAME);
        config.setConnectorMessages(TestHelpers.createDummyMessages());
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

    /**
     * Context is set in jndiProperties.
     */
    public static class MockContextFactory implements InitialContextFactory {

        @Override
        public Context getInitialContext(final @SuppressWarnings("rawtypes") java.util.Hashtable environment)
                throws NamingException {
            return (Context) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] { Context.class },
                    new ContextIH());
        }
    }

    /**
     *
     * MockContextFactory create the ContextIH
     *
     * The lookup method will return DataSourceIH
     *
     */
    static class ContextIH implements InvocationHandler {

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            if (method.getName().equals("lookup")) {
                return Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] { DataSource.class },
                        new DataSourceIH());
            }
            return null;
        }
    }

    /**
     * ContextIH create DataSourceIH.
     * The getConnection method will return ConnectionIH
     */
    static class DataSourceIH implements InvocationHandler {

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args)
                throws Throwable {
            if (method.getName().equals("getConnection")) {
                return DriverManager.getConnection(URL, USER, PASSWD);
            }
            throw new IllegalArgumentException("DataSource, invalid method:" + method.getName());
        }
    }
}
