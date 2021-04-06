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

import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.DEFAULT_PASSWORD_CHARSET;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.EMPTY_STR;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_INVALID_QUOTING;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_KEY_COLUMN_BLANK;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_KEY_COLUMN_EQ_CHANGE_LOG_COLUMN;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_PASSWD_COLUMN_EQ_CHANGE_LOG_COLUMN;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_PASSWD_COLUMN_EQ_KEY_COLUMN;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_PWD_ENCODING_UNSUPPORTED;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_TABLE_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_DATABASE_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_HOST_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_DRIVER_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_DRIVER_NOT_FOUND;
import static net.tirasa.connid.commons.db.Constants.MSG_JDBC_TEMPLATE_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_PASSWORD_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_PORT_BLANK;
import static net.tirasa.connid.commons.db.Constants.MSG_USER_BLANK;

import java.nio.charset.Charset;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import net.tirasa.connid.bundles.db.table.util.DatabaseTableSQLUtil;
import net.tirasa.connid.bundles.db.table.security.SupportedAlgorithm;
import net.tirasa.connid.commons.db.JNDIUtil;
import org.identityconnectors.framework.common.serializer.SerializerUtil;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import org.identityconnectors.framework.spi.operations.SyncOp;

/**
 *
 * Implements the {@link Configuration} interface to provide all the necessary parameters to initialize the JDBC
 *
 * Connector.
 *
 */
public class DatabaseTableConfiguration extends AbstractConfiguration {

    /**
     *
     * Setup logging for the {@link DatabaseTableConfiguration}.
     *
     */
    private static final Log LOG = Log.getLog(DatabaseTableConfiguration.class);

    // =======================================================================
    // DatabaseTableConfiguration
    // =======================================================================
    /**
     * How to quote a column in SQL statements. Possible values can be NONE, SINGLE, DOUBLE, BRACKETS, BACKSLASH
     */
    private String quoting = EMPTY_STR;

    /**
     * NameQoute getter
     *
     * @return quoting value
     */
    @ConfigurationProperty(order = 1,
            displayMessageKey = "QUOTING_DISPLAY",
            helpMessageKey = "QUOTING_HELP")
    public String getQuoting() {
        return this.quoting;
    }

    /**
     * NameQuote Setter
     *
     * @param value value
     */
    public void setQuoting(final String value) {
        this.quoting = value;
    }

    /**
     * The host value
     */
    private String host = EMPTY_STR;

    /**
     * @return quoting value
     */
    @ConfigurationProperty(order = 2,
            displayMessageKey = "HOST_DISPLAY",
            helpMessageKey = "HOST_HELP")
    public String getHost() {
        return this.host;
    }

    public void setHost(final String value) {
        this.host = value;
    }

    /**
     * The port value
     */
    private String port = EMPTY_STR;

    /**
     * NameQuote getter
     *
     * @return quoting value
     */
    @ConfigurationProperty(order = 3,
            displayMessageKey = "PORT_DISPLAY",
            helpMessageKey = "PORT_HELP")
    public String getPort() {
        return this.port;
    }

    /**
     * NameQuote Setter
     *
     * @param value value
     */
    public void setPort(final String value) {
        this.port = value;
    }

    /**
     * Database Login User name. This user name is used to connect to database. The provided user name and password
     * should have rights to insert/update/delete the rows in the configured identity holder table. Required
     * configuration property, and should be validated
     */
    private String user = EMPTY_STR;

    /**
     * @return user value
     */
    @ConfigurationProperty(order = 4,
            displayMessageKey = "USER_DISPLAY",
            helpMessageKey = "USER_HELP")
    public String getUser() {
        return this.user;
    }

    public void setUser(final String value) {
        this.user = value;
    }

    /**
     * Database access Password. This password is used to connect to database. The provided user name and password
     * should have rights to insert/update/delete the rows in the configured identity holder table. Required
     * configuration property, and should be validated
     */
    private GuardedString password;

    /**
     * @return password value
     */
    @ConfigurationProperty(order = 5, confidential = true,
            displayMessageKey = "PASSWORD_DISPLAY",
            helpMessageKey = "PASSWORD_HELP")
    public GuardedString getPassword() {
        return this.password;
    }

    public void setPassword(final GuardedString value) {
        this.password = value;
    }

    /**
     * Database name.
     */
    private String database = EMPTY_STR;

    /**
     * @return user value
     */
    @ConfigurationProperty(order = 6,
            displayMessageKey = "DATABASE_DISPLAY",
            helpMessageKey = "DATABASE_HELP")
    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(final String value) {
        this.database = value;
    }

    /**
     * Database Table name. The name of the identity holder table (Integration table).
     */
    private String table = EMPTY_STR;

    /**
     * The table name
     *
     * @return the user account table name Please notice, there are used non default message keys
     */
    @ConfigurationProperty(order = 7, required = true,
            displayMessageKey = "TABLE_DISPLAY",
            helpMessageKey = "TABLE_HELP")
    public String getTable() {
        return this.table;
    }

    /**
     * Table setter
     *
     * @param table name value
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Key Column, The name of the key column is required This non empty value must be validated
     */
    private String keyColumn = EMPTY_STR;

    /**
     * Key Column getter
     *
     * @return keyColumn value
     */
    @ConfigurationProperty(order = 8, required = true,
            displayMessageKey = "KEY_COLUMN_DISPLAY",
            helpMessageKey = "KEY_COLUMN_HELP")

    public String getKeyColumn() {
        return this.keyColumn;
    }

    /**
     * Key Column setter
     *
     * @param keyColumn value
     */
    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;

    }

    /**
     * Password Column. If non empty, password is supported in the schema empty password column means, the password is
     * not supported and also should not be in the schema
     */
    private String passwordColumn = EMPTY_STR;

    /**
     * Password Column getter
     *
     * @return passwordColumn value
     */
    @ConfigurationProperty(order = 9,
            displayMessageKey = "PASSWORD_COLUMN_DISPLAY",
            helpMessageKey = "PASSWORD_COLUMN_HELP")

    public String getPasswordColumn() {
        return this.passwordColumn;
    }

    /**
     * Password Column setter
     *
     * @param value value
     */
    public void setPasswordColumn(String value) {
        this.passwordColumn = value;
    }

    /**
     * Status column. If not empty it indicates table column used to describe entry status. Disabled status value are
     * given by property 'disabledStatusValue'. Enabled status value are given by property 'enabledStatusValue'. Default
     * value will be 'defaultStatusValue'.
     */
    private String statusColumn = EMPTY_STR;

    /**
     * Status Column getter
     *
     * @return statusColumn value
     */
    @ConfigurationProperty(order = 10,
            displayMessageKey = "STATUS_COLUMN_DISPLAY",
            helpMessageKey = "STATUS_COLUMN_HELP")
    public String getStatusColumn() {
        return statusColumn;
    }

    /**
     * Status Column setter
     *
     * @param statusColumn value
     */
    public void setStatusColumn(final String statusColumn) {
        this.statusColumn = statusColumn;
    }

    /**
     * Value for 'statusColumn' field to indicate disabled entries. Default 'false'.
     */
    private String disabledStatusValue = "false";

    /**
     * Disabled status value getter.
     *
     * @return disabled status value.
     */
    @ConfigurationProperty(order = 11,
            displayMessageKey = "DISABLED_STATUS_VALUE_DISPLAY",
            helpMessageKey = "DISABLED_STATUS_VALUE_HELP")

    public String getDisabledStatusValue() {
        return disabledStatusValue;
    }

    /**
     * Disabled status value setter.
     *
     * @param disabledStatusValue status value.
     */
    public void setDisabledStatusValue(final String disabledStatusValue) {
        this.disabledStatusValue = disabledStatusValue;
    }

    /**
     * Value for 'statusColumn' field to indicate disabled entries. Default 'true'.
     */
    private String enabledStatusValue = "true";

    /**
     * Enabled status value getter.
     *
     * @return enabled status value.
     */
    @ConfigurationProperty(order = 12,
            displayMessageKey = "ENABLED_STATUS_VALUE_DISPLAY",
            helpMessageKey = "ENABLED_STATUS_VALUE_HELP")
    public String getEnabledStatusValue() {
        return enabledStatusValue;
    }

    /**
     * Enabled status value setter.
     *
     * @param enabledStatusValue status value.
     */
    public void setEnabledStatusValue(final String enabledStatusValue) {
        this.enabledStatusValue = enabledStatusValue;
    }

    /**
     * Default value for 'statusColumn' field. Default 'true';
     */
    private String defaultStatusValue = "true";

    /**
     * Default status value getter.
     *
     * @return default status value.
     */
    @ConfigurationProperty(order = 13,
            displayMessageKey = "DEFAULT_STATUS_VALUE_DISPLAY",
            helpMessageKey = "DEFAULT_STATUS_VALUE_HELP")
    public String getDefaultStatusValue() {
        return defaultStatusValue;
    }

    /**
     * Default status value setter.
     *
     * @param defaultStatusValue status value.
     */
    public void setDefaultStatusValue(final String defaultStatusValue) {
        this.defaultStatusValue = defaultStatusValue;
    }

    /**
     * The Driver class. The jdbcDriver is located by connector framework to connect to database. *
     * Required configuration property (when not using a Datasource), and should be validated
     */
    private String jdbcDriver = EMPTY_STR;

    /**
     * @return jdbcDriver value
     */
    @ConfigurationProperty(order = 14,
            displayMessageKey = "JDBC_DRIVER_DISPLAY",
            helpMessageKey = "JDBC_DRIVER_HELP")
    public String getJdbcDriver() {
        return this.jdbcDriver;
    }

    public void setJdbcDriver(String value) {
        this.jdbcDriver = value;
    }

    /**
     * Database connection URL. The url is used to connect to database. Required configuration *
     * property (when not using a Datasource), and should be validated
     */
    private String jdbcUrlTemplate = EMPTY_STR;

    /**
     * Return the jdbcUrlTemplate
     *
     * @return url value
     */
    @ConfigurationProperty(order = 15,
            displayMessageKey = "URL_TEMPLATE_DISPLAY",
            helpMessageKey = "URL_TEMPLATE_HELP")
    public String getJdbcUrlTemplate() {
        return jdbcUrlTemplate;
    }

    public void setJdbcUrlTemplate(String value) {
        this.jdbcUrlTemplate = value;
    }

    /**
     * The empty string setting allow conversion of a null into an empty string for not-null char columns
     */
    public boolean enableEmptyString = false;

    /**
     * Accessor for the enableEmptyString property
     *
     * @return the enableEmptyString
     */
    @ConfigurationProperty(order = 16,
            displayMessageKey = "ENABLE_EMPTY_STRING_DISPLAY",
            helpMessageKey = "ENABLE_EMPTY_STRING_HELP")
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
     * Some database drivers will throw the SQLError when setting the parameters to the statement with zero ErrorCode.
     * This mean no error. This switch allow to switch off ignoring this SQLError
     */
    public boolean rethrowAllSQLExceptions = true;

    /**
     * Accessor for the rethrowAllSQLExceptions property
     *
     * @return the rethrowAllSQLExceptions
     */
    @ConfigurationProperty(order = 17,
            displayMessageKey = "RETHROW_ALL_SQLEXCEPTIONS_DISPLAY",
            helpMessageKey = "RETHROW_ALL_SQLEXCEPTIONS_HELP")
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
     * Some JDBC drivers (ex: Oracle) may not be able to get correct string representation of TIMESTAMP data type of the
     * column from the database table. To get correct value , one needs to use rs.getTimestamp() rather rs.getString().
     */
    public boolean nativeTimestamps = false;

    /**
     * Accessor for the nativeTimestamps property
     *
     * @return the nativeTimestamps
     */
    @ConfigurationProperty(order = 18,
            displayMessageKey = "NATIVE_TIMESTAMPS_DISPLAY",
            helpMessageKey = "NATIVE_TIMESTAMPS_HELP")
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
     * Some JDBC drivers (ex: DerbyDB) may need to access all the datatypes with native types to get correct value.
     */
    public boolean allNative = false;

    /**
     * Accessor for the allNativeproperty
     *
     * @return the allNative
     */
    @ConfigurationProperty(order = 19,
            displayMessageKey = "ALL_NATIVE_DISPLAY",
            helpMessageKey = "ALL_NATIVE_HELP")
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
     * The new connection validation query. The query can be empty. Then the auto commit true/false command is applied
     * by default. This can be insufficient on some database drivers because of caching Then the validation query is
     * required.
     */
    private String validConnectionQuery;

    /**
     * connection validation query getter
     *
     * @return validConnectionQuery value
     */
    @ConfigurationProperty(order = 20,
            displayMessageKey = "VALID_CONNECTION_QUERY_DISPLAY",
            helpMessageKey = "VALID_CONNECTION_QUERY_HELP")
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

    /**
     * Change Log Column (should automatically add ORDER BY) If the value is non empty, the SyncOp should be supported
     * It could be nativeTimestamps.
     */
    private String changeLogColumn = EMPTY_STR;

    /**
     * Log Column is required be SyncOp
     *
     * @return Log Column
     */
    @ConfigurationProperty(order = 21, operations = SyncOp.class,
            displayMessageKey = "CHANGE_LOG_COLUMN_DISPLAY",
            helpMessageKey = "CHANGE_LOG_COLUMN_HELP")
    public String getChangeLogColumn() {
        return this.changeLogColumn;
    }

    public void setChangeLogColumn(String value) {
        this.changeLogColumn = value;
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
    @ConfigurationProperty(order = 22,
            displayMessageKey = "DATASOURCE_DISPLAY",
            helpMessageKey = "DATASOURCE_HELP")
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
    @ConfigurationProperty(order = 23,
            displayMessageKey = "JNDI_PROPERTIES_DISPLAY",
            helpMessageKey = "JNDI_PROPERTIES_HELP")
    public String[] getJndiProperties() {
        return (String[]) SerializerUtil.cloneObject(jndiProperties);
    }

    public void setJndiProperties(String[] value) {
        this.jndiProperties = (String[]) SerializerUtil.cloneObject(value);
    }

    private String cipherAlgorithm = SupportedAlgorithm.CLEARTEXT.name();

    @ConfigurationProperty(order = 24,
            displayMessageKey = "CIPHER_ALGORITHM_DISPLAY",
            helpMessageKey = "CIPHER_ALGORITHM_HELP")
    public String getCipherAlgorithm() {
        return StringUtil.isBlank(cipherAlgorithm) ? SupportedAlgorithm.CLEARTEXT.name() : cipherAlgorithm;
    }

    public void setCipherAlgorithm(String cipherAlgorithm) {
        this.cipherAlgorithm = cipherAlgorithm;
    }

    private String cipherKey;

    @ConfigurationProperty(order = 25, required = false,
            displayMessageKey = "CIPHER_KEY_DISPLAY",
            helpMessageKey = "CIPHER_KEY_HELP")
    public String getCipherKey() {
        return cipherKey;
    }

    public void setCipherKey(String cipherKey) {
        this.cipherKey = cipherKey;
    }

    private boolean pwdEncodeToUpperCase;

    @ConfigurationProperty(order = 25, required = false,
            displayMessageKey = "PWD_ENCODE_UPPERCASE_DISPLAY",
            helpMessageKey = "PWD_ENCODE_UPPERCASE_HELP")
    public boolean isPwdEncodeToUpperCase() {
        return pwdEncodeToUpperCase;
    }

    public void setPwdEncodeToUpperCase(boolean pwdEncodeToUpperCase) {
        this.pwdEncodeToUpperCase = pwdEncodeToUpperCase;
    }

    private boolean pwdEncodeToLowerCase;

    @ConfigurationProperty(order = 26, required = false,
            displayMessageKey = "PWD_ENCODE_LOWERCASE_DISPLAY",
            helpMessageKey = "PWD_ENCODE_LOWERCASE_HELP")

    public boolean isPwdEncodeToLowerCase() {
        return pwdEncodeToLowerCase;
    }

    public void setPwdEncodeToLowerCase(boolean pwdEncodeToLowerCase) {
        this.pwdEncodeToLowerCase = pwdEncodeToLowerCase;
    }

    private boolean retrievePassword;

    @ConfigurationProperty(order = 27,
            displayMessageKey = "RETRIEVE_PASSWORD_DISPLAY",
            helpMessageKey = "RETRIEVE_PASSWORD_HELP")

    public boolean isRetrievePassword() {
        return retrievePassword;
    }

    public void setRetrievePassword(boolean retrievePassword) {
        this.retrievePassword = retrievePassword;
    }

    /**
     * clear password character set used by resource
     */
    private String passwordCharset = DEFAULT_PASSWORD_CHARSET;

    /**
     * Return password character set used by resource to encode clear password specified as required by java.nio.Charset
     * http://docs.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html
     *
     * @return password character set used by resource to encode clear password
     */
    @ConfigurationProperty(order = 28, required = false,
            displayMessageKey = "PASSWORD_CHARSET_DISPLAY",
            helpMessageKey = "PASSWORD_CHARSET_HELP")
    public String getPasswordCharset() {
        return passwordCharset;
    }

    public void setPasswordCharset(String passwordCharset) {
        this.passwordCharset = passwordCharset;
    }

    // =======================================================================
    // Configuration Interface
    // =======================================================================
    /**
     * Attempt to validate the arguments added to the Configuration.
     */
    @Override

    public void validate() {
        LOG.info("Validate DatabaseTableConfiguration");

        // check that there is a table to query..
        if (StringUtil.isBlank(getTable())) {
            throw new IllegalArgumentException(getMessage(MSG_TABLE_BLANK));
        }

        // determine if you can get a key column
        if (StringUtil.isBlank(getKeyColumn())) {
            throw new IllegalArgumentException(getMessage(MSG_KEY_COLUMN_BLANK));
        } else if (getKeyColumn().equalsIgnoreCase(getChangeLogColumn())) {
            throw new IllegalArgumentException(getMessage(MSG_KEY_COLUMN_EQ_CHANGE_LOG_COLUMN));
        }

        // key column, password column
        if (StringUtil.isNotBlank(getPasswordColumn())) {
            if (getPasswordColumn().equalsIgnoreCase(getKeyColumn())) {
                throw new IllegalArgumentException(getMessage(MSG_PASSWD_COLUMN_EQ_KEY_COLUMN));
            }

            if (getPasswordColumn().equalsIgnoreCase(getChangeLogColumn())) {
                throw new IllegalArgumentException(getMessage(MSG_PASSWD_COLUMN_EQ_CHANGE_LOG_COLUMN));
            }
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

            // check the url is configured
            if (StringUtil.isBlank(getJdbcUrlTemplate())) {
                throw new IllegalArgumentException(getMessage(MSG_JDBC_TEMPLATE_BLANK));
            }

            // host required
            if (getJdbcUrlTemplate().contains("%h") && StringUtil.isBlank(getHost())) {
                throw new IllegalArgumentException(getMessage(MSG_HOST_BLANK));
            }

            // port required
            if (getJdbcUrlTemplate().contains("%p") && StringUtil.isBlank(getPort())) {

                throw new IllegalArgumentException(getMessage(MSG_PORT_BLANK));

            }

            // database required            
            if (getJdbcUrlTemplate().contains("%d") && StringUtil.isBlank(getDatabase())) {
                throw new IllegalArgumentException(getMessage(MSG_DATABASE_BLANK));
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

            LOG.ok("Driver configuration is ok");
        } else {
            LOG.info("Validate datasource configuration");

            //Validate the JNDI properties
            JNDIUtil.arrayToProperties(getJndiProperties(), getConnectorMessages());

            LOG.ok("Datasource configuration is ok");
        }

        try {
            DatabaseTableSQLUtil.quoteName(getQuoting(), "test");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(getMessage(MSG_INVALID_QUOTING, getQuoting()));
        }

        // check there if specified password encoding is supported         
        if (StringUtil.isNotBlank(getPasswordCharset())) {
            if (!Charset.availableCharsets().keySet().contains(getPasswordCharset())) {
                throw new IllegalArgumentException(getMessage(MSG_PWD_ENCODING_UNSUPPORTED));
            }
        }

        LOG.ok("Configuration is valid");
    }

    /**
     * Format a URL given a template. Recognized template characters are: % literal % h host p port d database
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
                if (ch == '%') {
                    b.append(ch);
                } else if (ch == 'h') {
                    b.append(getHost());
                } else if (ch == 'p') {
                    b.append(getPort());
                } else if (ch == 'd') {
                    b.append(getDatabase());
                }
            }
        }

        final String formattedURL = b.toString();
        LOG.ok("UrlTemplate is formated to {0}", formattedURL);
        return formattedURL;
    }

    /**
     * Format the connector message
     *
     * @param key key of the message
     * @return return the formated message
     */
    public String getMessage(final String key) {
        final String fmt = getConnectorMessages().format(key, key);
        LOG.ok("Get for a key {0} connector message {1}", key, fmt);
        return fmt;
    }

    /**
     * Format message with arguments
     *
     * @param key key of the message
     * @param objects arguments
     * @return the localized message string
     */
    public String getMessage(final String key, final Object... objects) {
        final String fmt = getConnectorMessages().format(key, key, objects);
        LOG.ok("Get for a key {0} connector message {1}", key, fmt);
        return fmt;
    }

}
