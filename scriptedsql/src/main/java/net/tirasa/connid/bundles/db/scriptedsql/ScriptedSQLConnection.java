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
package net.tirasa.connid.bundles.db.scriptedsql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import net.tirasa.connid.bundles.db.common.JNDIUtil;
import net.tirasa.connid.bundles.db.common.SQLUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectionBrokenException;
import org.identityconnectors.framework.common.objects.ConnectorMessages;

/**
 * Class to represent a ScriptedJDBC Connection.
 */
public class ScriptedSQLConnection {

    /**
     * Setup logging for the {@link ScriptedSQLConnection}.
     */
    private static final Log LOG = Log.getLog(ScriptedSQLConnection.class);

    private ScriptedSQLConfiguration _configuration;

    private Connection sqlConn = null;

    public ScriptedSQLConnection(final ScriptedSQLConfiguration configuration) {
        _configuration = configuration;
    }

    /**
     * @param config
     * @return
     */
    private static Connection connect(final ScriptedSQLConfiguration config) {
        Connection connection;
        final String login = config.getUser();
        final GuardedString password = config.getPassword();

        final String datasource = config.getDatasource();
        if (StringUtil.isNotBlank(datasource)) {
            LOG.info("Get a new connection using datasource {0}", datasource);
            final String[] jndiProperties = config.getJndiProperties();
            final ConnectorMessages connectorMessages = config.getConnectorMessages();
            final Properties prop = JNDIUtil.arrayToProperties(jndiProperties, connectorMessages);
            if (StringUtil.isNotBlank(login) && password != null) {
                connection = SQLUtil.getDatasourceConnection(datasource, login, password, prop);
            } else {
                connection = SQLUtil.getDatasourceConnection(datasource, prop);
            }
            LOG.ok("The new connection using datasource {0} is created", datasource);
        } else {
            final String driver = config.getJdbcDriver();
            final String connectionUrl = config.formatUrlTemplate();
            LOG.info("Getting a new connection using connection url {0} and user {1}", connectionUrl, login);
            connection = SQLUtil.getDriverMangerConnection(driver, connectionUrl, login, password);
            LOG.ok("The new connection using connection url {0} and user {1} is created", connectionUrl, login);
        }

        //Set auto-commit mode 
        try {
            if (config.isAutoCommit()) {
                LOG.info("Setting AutoCommit to true");
                connection.setAutoCommit(true);
            } else {
                LOG.info("Setting AutoCommit to false");
                connection.setAutoCommit(false);
            }
        } catch (SQLException expected) {
            LOG.error(expected, "setAutoCommit() exception");
        }
        return connection;
    }

    /**
     * Release internal resources
     */
    public void dispose() {
        SQLUtil.closeQuietly(sqlConn);
    }

    /**
     * If internal connection is not usable, throw IllegalStateException
     */
    public void test() {
        try {
            if (null == getSqlConnection() || sqlConn.isClosed() || !sqlConn.isValid(2)) {
                throw new ConnectionBrokenException("JDBC connection is broken");
            }
        } catch (SQLException e) {
            throw ConnectionBrokenException.wrap(e);
        }
    }

    /**
     * Get the internal JDBC connection.
     *
     * @return the connection
     */
    public Connection getSqlConnection() {
        if (sqlConn == null) {
            sqlConn = connect(_configuration);
        }
        return this.sqlConn;
    }

    /**
     * Set the internal JDBC connection.
     *
     * @param connection
     */
    public void setSqlConnection(Connection connection) {
        this.sqlConn = connection;
    }
}
