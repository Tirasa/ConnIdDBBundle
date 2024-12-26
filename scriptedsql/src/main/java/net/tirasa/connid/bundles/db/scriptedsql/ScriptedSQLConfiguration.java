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

import static net.tirasa.connid.commons.db.Constants.MSG_DATABASE_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_HOST_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_DRIVER_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_DRIVER_NOT_FOUND;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_TEMPLATE_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_PASSWORD_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_PORT_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_USER_BLANK;

import net.tirasa.connid.commons.db.JNDIUtil;
import net.tirasa.connid.commons.scripted.AbstractScriptedConfiguration;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

/**
 * Extends the {@link AbstractConfiguration} class to provide all the necessary parameters to initialize the
 * ScriptedJDBC Connector.
 */
public class ScriptedSQLConfiguration extends AbstractScriptedConfiguration {

    public static final String EMPTY_STR = "";

    // =======================================================================
    // DatabaseTableConfiguration
    // =======================================================================
    /**
     * How to quote a column in SQL statements. Possible values can be NONE,
     * SINGLE, DOUBLE, BRACKETS, BACKSLASH
     */
    private String quoting = EMPTY_STR;  // GAEL check with DBTAble connector constantsEMPTY_STR;

    /**
     * NameQuote getter
     *
     * @return quoting value
     */
    @ConfigurationProperty(order = -15, displayMessageKey = "QUOTING_DISPLAY", helpMessageKey = "QUOTING_HELP")
    public String getQuoting() {
        return this.quoting;
    }

    /**
     * NameQuote Setter
     *
     * @param value value
     */
    public void setQuoting(String value) {
        this.quoting = value;
    }

    /**
     * The host value
     */
    private String host = "localhost";

    /**
     * host getter
     *
     * @return host value
     */
    @ConfigurationProperty(order = -14, displayMessageKey = "HOST_DISPLAY", helpMessageKey = "HOST_HELP")
    public String getHost() {
        return this.host;
    }

    /**
     * host Setter
     *
     * @param value value
     */
    public void setHost(String value) {
        this.host = value;
    }

    /**
     * The port value
     */
    private String port = "3306";

    /**
     * port getter
     *
     * @return port value
     */
    @ConfigurationProperty(order = -13, displayMessageKey = "PORT_DISPLAY", helpMessageKey = "PORT_HELP")
    public String getPort() {
        return this.port;
    }

    /**
     * port Setter
     *
     * @param value value
     */
    public void setPort(String value) {
        this.port = value;
    }

    /**
     * Database Login User name. This user name is used to connect to database.
     * The provided user name and password should have rights to
     * insert/update/delete the rows in the configured identity holder table.
     * Required configuration property, and should be validated
     */
    private String user = EMPTY_STR;

    /**
     * @return user value
     */
    @ConfigurationProperty(order = -12, displayMessageKey = "USER_DISPLAY", helpMessageKey = "USER_HELP")
    public String getUser() {
        return this.user;
    }

    public void setUser(String value) {
        this.user = value;
    }

    /**
     * Database access Password. This password is used to connect to database.
     * The provided user name and password should have rights to
     * insert/update/delete the rows in the configured identity holder table.
     * Required configuration property, and should be validated
     */
    private GuardedString password;

    /**
     * @return password value
     */
    @ConfigurationProperty(order = -11, confidential = true,
            displayMessageKey = "PASSWORD_DISPLAY", helpMessageKey = "PASSWORD_HELP")
    public GuardedString getPassword() {
        return this.password;
    }

    public void setPassword(GuardedString value) {
        this.password = value;
    }

    /**
     * Database name.
     */
    private String database = EMPTY_STR;

    /**
     * @return user value
     */
    @ConfigurationProperty(order = -10, displayMessageKey = "DATABASE_DISPLAY", helpMessageKey = "DATABASE_HELP")
    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String value) {
        this.database = value;
    }

    /**
     * The Driver class. The jdbcDriver is located by connector framework to
     * connect to database. Required configuration property, and should be
     * validated.
     * Oracle: oracle.jdbc.driver.OracleDriver
     * MySQL: com.mysql.jdbc.Driver db2: COM.ibm.db2.jdbc.net.DB2Driver
     * MSSQL: com.microsoft.sqlserver.jdbc.SQLServerDriver
     * Sybase: com.sybase.jdbc2.jdbc.SybDriver
     * Derby: org.apache.derby.jdbc.ClientDriver
     * Derby embedded: org.apache.derby.jdbc.EmbeddedDriver
     *
     */
    private String jdbcDriver = "com.mysql.jdbc.Driver";

    /**
     * @return jdbcDriver value
     */
    @ConfigurationProperty(order = -9, displayMessageKey = "JDBC_DRIVER_DISPLAY", helpMessageKey = "JDBC_DRIVER_HELP")
    public String getJdbcDriver() {
        return this.jdbcDriver;
    }

    public void setJdbcDriver(String value) {
        this.jdbcDriver = value;
    }

    /**
     * Database connection URL. The url is used to connect to database. Required
     * configuration property, and should be validated.
     * Oracle: jdbc:oracle:thin:
     *
     * @%h:%p:%d MySQL jdbc:mysql://%h:%p/%d
     * Sybase: jdbc:sybase:Tds:%h:%p/%d DB2 jdbc:db2://%h:%p/%d
     * SQL: jdbc:sqlserver://%h:%p;DatabaseName=%d
     * Derby: jdbc:derby://%h:%p/%d
     * Derby: embedded jdbc:derby:%d
     */
    private String jdbcUrlTemplate = "jdbc:mysql://%h:%p/%d";

    /**
     * Return the jdbcUrlTemplate
     *
     * @return url value
     */
    @ConfigurationProperty(order = -8, displayMessageKey = "URL_TEMPLATE_DISPLAY", helpMessageKey = "URL_TEMPLATE_HELP")
    public String getJdbcUrlTemplate() {
        return jdbcUrlTemplate;
    }

    public void setJdbcUrlTemplate(String value) {
        this.jdbcUrlTemplate = value;
    }

    /**
     * With autoCommit set to true, each SQL statement is treated as a
     * transaction. It is automatically committed right after it is executed.
     */
    private boolean autoCommit = true;

    /**
     * Accessor for the autoCommit property
     *
     * @return the autoCommit
     */
    @ConfigurationProperty(order = -7, displayMessageKey = "AUTO_COMMIT_DISPLAY", helpMessageKey = "AUTO_COMMIT_HELP")
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Setter for the autoCommit property.
     *
     * @param autoCommit auto commit
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * The empty string setting allow conversion of a null into an empty string
     * for not-null char columns
     */
    private boolean enableEmptyString = false;

    /**
     * Accessor for the enableEmptyString property
     *
     * @return the enableEmptyString
     */
    @ConfigurationProperty(order = -6,
            displayMessageKey = "ENABLE_EMPTY_STRING_DISPLAY", helpMessageKey = "ENABLE_EMPTY_STRING_HELP")
    public boolean isEnableEmptyString() {
        return enableEmptyString;
    }

    /**
     * Setter for the enableEmptyString property.
     *
     * @param enableEmptyString the enableEmptyString to set
     */
    public void setEnableEmptyString(boolean enableEmptyString) {
        this.enableEmptyString = enableEmptyString;
    }

    /**
     * Some database drivers will throw the SQLError when setting the parameters
     * to the statement with zero ErrorCode. This mean no error. This switch
     * allow to switch off ignoring this SQLError
     */
    public boolean rethrowAllSQLExceptions = true;

    /**
     * Accessor for the rethrowAllSQLExceptions property
     *
     * @return the rethrowAllSQLExceptions
     */
    @ConfigurationProperty(order = -5,
            displayMessageKey = "RETHROW_ALL_SQLEXCEPTIONS_DISPLAY", helpMessageKey = "RETHROW_ALL_SQLEXCEPTIONS_HELP")
    public boolean isRethrowAllSQLExceptions() {
        return rethrowAllSQLExceptions;
    }

    /**
     * Setter for the rethrowAllSQLExceptions property.
     *
     * @param rethrowAllSQLExceptions the rethrowAllSQLExceptions to set
     */
    public void setRethrowAllSQLExceptions(boolean rethrowAllSQLExceptions) {
        this.rethrowAllSQLExceptions = rethrowAllSQLExceptions;
    }

    /**
     * Some JDBC drivers (ex: Oracle) may not be able to get correct string
     * representation of TIMESTAMP data type of the column from the database
     * table. To get correct value , one needs to use rs.getTimestamp() rather
     * rs.getString().
     */
    public boolean nativeTimestamps = false;

    /**
     * Accessor for the nativeTimestamps property
     *
     * @return the nativeTimestamps
     */
    @ConfigurationProperty(order = -4,
            displayMessageKey = "NATIVE_TIMESTAMPS_DISPLAY", helpMessageKey = "NATIVE_TIMESTAMPS_HELP")
    public boolean isNativeTimestamps() {
        return nativeTimestamps;
    }

    /**
     * Setter for the nativeTimestamps property.
     *
     * @param nativeTimestamps the nativeTimestamps to set
     */
    public void setNativeTimestamps(boolean nativeTimestamps) {
        this.nativeTimestamps = nativeTimestamps;
    }

    /**
     * Some JDBC drivers (ex: DerbyDB) may need to access all the datatypes with
     * native types to get correct value.
     */
    public boolean allNative = false;

    /**
     * Accessor for the allNativeproperty
     *
     * @return the allNative
     */
    @ConfigurationProperty(order = -3, displayMessageKey = "ALL_NATIVE_DISPLAY", helpMessageKey = "ALL_NATIVE_HELP")
    public boolean isAllNative() {
        return allNative;
    }

    /**
     * Setter for the allNative property.
     *
     * @param allNative the allNative to set
     */
    public void setAllNative(boolean allNative) {
        this.allNative = allNative;
    }

    /**
     * The new connection validation query. The query can be empty. Then the
     * auto commit true/false command is applied by default. This can be
     * insufficient on some database drivers because of caching Then the
     * validation query is required.
     */
    private String validConnectionQuery;

    /**
     * connection validation query getter
     *
     * @return validConnectionQuery value
     */
    @ConfigurationProperty(order = -2,
            displayMessageKey = "VALID_CONNECTION_QUERY_DISPLAY", helpMessageKey = "VALID_CONNECTION_QUERY_HELP")
    public String getValidConnectionQuery() {
        return this.validConnectionQuery;
    }

    /**
     * Connection validation query setter
     *
     * @param value value
     */
    public void setValidConnectionQuery(String value) {
        this.validConnectionQuery = value;
    }
    // =======================================================================
    // DataSource
    // =======================================================================

    /**
     * The datasource name is used to connect to database.
     */
    private String datasource = EMPTY_STR;

    /**
     * Return the datasource
     *
     * @return datasource value
     */
    @ConfigurationProperty(order = -1, displayMessageKey = "DATASOURCE_DISPLAY", helpMessageKey = "DATASOURCE_HELP")
    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String value) {
        this.datasource = value;
    }

    /**
     * The jndiFactory name is used to connect to database.
     */
    private String[] jndiProperties;

    /**
     * Return the jndiFactory
     *
     * @return jndiFactory value
     */
    @ConfigurationProperty(order = 0,
            displayMessageKey = "JNDI_PROPERTIES_DISPLAY", helpMessageKey = "JNDI_PROPERTIES_HELP")
    public String[] getJndiProperties() {
        return jndiProperties;
    }

    public void setJndiProperties(String[] value) {
        this.jndiProperties = value;
    }

    // =======================================================================
    // Configuration Interface
    // =======================================================================
    /**
     * Attempt to validate the arguments added to the Configuration.
     */
    @Override
    public void validate() {
        LOG.info("Validate " + getClass().getName());

        // check the url is configured
        if (StringUtil.isBlank(getJdbcUrlTemplate())) {
            throw new IllegalArgumentException(getMessage(MSG_JDBC_TEMPLATE_BLANK));
        }
        // check that there is not a datasource
        if (StringUtil.isBlank(getDatasource())) {
            LOG.info("Validate driver configuration.");

            // determine if you can get a connection to the database..
            if (getUser() == null) {
                throw new IllegalArgumentException(getMessage(MSG_USER_BLANK));
            }
            // check that there is a pwd to query..
            if (getPassword() == null) {
                throw new IllegalArgumentException(getMessage(MSG_PASSWORD_BLANK));
            }
            // host required
            if (getJdbcUrlTemplate().contains("%h")) {
                if (StringUtil.isBlank(getHost())) {
                    throw new IllegalArgumentException(getMessage(MSG_HOST_BLANK));
                }
            }
            // port required
            if (getJdbcUrlTemplate().contains("%p")) {
                if (StringUtil.isBlank(getPort())) {
                    throw new IllegalArgumentException(getMessage(MSG_PORT_BLANK));
                }
            }
            // database required
            if (getJdbcUrlTemplate().contains("%d")) {
                if (StringUtil.isBlank(getDatabase())) {
                    throw new IllegalArgumentException(getMessage(MSG_DATABASE_BLANK));
                }
            }
            // make sure the jdbcDriver is in the class path..
            if (StringUtil.isBlank(getJdbcDriver())) {
                throw new IllegalArgumentException(getMessage(MSG_JDBC_DRIVER_BLANK));
            }
            try {
                Class.forName(getJdbcDriver());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(getMessage(MSG_JDBC_DRIVER_NOT_FOUND));
            }
            LOG.ok("driver configuration is ok");
        } else {
            LOG.info("Validate datasource configuration");
            //Validate the JNDI properties
            JNDIUtil.arrayToProperties(getJndiProperties(), getConnectorMessages());
            LOG.ok("datasource configuration is ok");
        }

        super.validate();

        LOG.ok("Configuration is valid");
    }

    /**
     * Format a URL given a template. Recognized template characters are: %
     * literal % h host p port d database
     *
     * @return the database url
     */
    public String formatUrlTemplate() {
        LOG.info("format UrlTemplate");
        final StringBuilder b = new StringBuilder();
        final String url = getJdbcUrlTemplate();
        final int len = url.length();
        for (int i = 0; i < len; i++) {
            char ch = url.charAt(i);
            if (ch != '%') {
                b.append(ch);
            } else if (i + 1 < len) {
                i++;
                ch = url.charAt(i);
                switch (ch) {
                    case '%':
                        b.append(ch);
                        break;
                    case 'h':
                        b.append(getHost());
                        break;
                    case 'p':
                        b.append(getPort());
                        break;
                    case 'd':
                        b.append(getDatabase());
                        break;
                    default:
                        break;
                }
            }
        }
        String formattedURL = b.toString();
        LOG.ok("UrlTemplate is formated to {0}", formattedURL);
        return formattedURL;
    }

    /**
     * Format the connector message
     *
     * @param key key of the message
     * @return return the formated message
     */
    public String getMessage(String key) {
        final String fmt = getConnectorMessages().format(key, key);
        LOG.ok("Get for a key {0} connector message {1}", key, fmt);
        return fmt;
    }
}
