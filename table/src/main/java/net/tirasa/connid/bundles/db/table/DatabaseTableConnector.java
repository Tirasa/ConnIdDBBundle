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

import static net.tirasa.connid.bundles.db.commons.Constants.MSG_ACCOUNT_OBJECT_CLASS_REQUIRED;
import static net.tirasa.connid.bundles.db.commons.Constants.MSG_INVALID_ATTRIBUTE_SET;
import static net.tirasa.connid.bundles.db.commons.Constants.MSG_PASSWORD_BLANK;
import static net.tirasa.connid.bundles.db.commons.Constants.MSG_RESULT_HANDLER_NULL;
import static net.tirasa.connid.bundles.db.commons.Constants.MSG_UID_BLANK;
import static net.tirasa.connid.bundles.db.commons.Constants.MSG_USER_BLANK;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_AUTHENTICATE_OP_NOT_SUPPORTED;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_AUTH_FAILED;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_CAN_NOT_CREATE;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_CAN_NOT_DELETE;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_CAN_NOT_READ;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_CAN_NOT_UPDATE;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_CHANGELOG_COLUMN_BLANK;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_INVALID_SYNC_TOKEN_VALUE;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_MORE_USERS_DELETED;
import static net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants.MSG_NAME_BLANK;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.tirasa.connid.bundles.db.commons.DatabaseQueryBuilder;
import net.tirasa.connid.bundles.db.commons.DatabaseQueryBuilder.OrderBy;
import net.tirasa.connid.bundles.db.commons.FilterWhereBuilder;
import net.tirasa.connid.bundles.db.commons.InsertIntoBuilder;
import net.tirasa.connid.bundles.db.commons.OperationBuilder;
import net.tirasa.connid.bundles.db.commons.SQLParam;
import net.tirasa.connid.bundles.db.commons.SQLUtil;
import net.tirasa.connid.bundles.db.commons.UpdateSetBuilder;
import org.identityconnectors.common.Assertions;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import net.tirasa.connid.bundles.db.table.security.EncodeAlgorithm;
import net.tirasa.connid.bundles.db.table.security.PasswordDecodingException;
import net.tirasa.connid.bundles.db.table.security.PasswordEncodingException;
import net.tirasa.connid.bundles.db.table.security.SupportedAlgorithm;
import net.tirasa.connid.bundles.db.table.security.UnsupportedPasswordCharsetException;
import net.tirasa.connid.bundles.db.table.util.DatabaseTableConstants;
import net.tirasa.connid.bundles.db.table.util.DatabaseTableSQLUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributeInfos;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.AuthenticateOp;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.ResolveUsernameOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

/**
 * The database table {@link DatabaseTableConnector} is a basic, but easy to use {@link DatabaseTableConnector} for
 * accounts in a relational database.
 * It supports create, update, search, and delete operations. It can also be used
 * for pass-thru authentication, although it assumes the password is in clear text in the database.
 * This connector assumes that all account data is stored in a single database table. The delete action is implemented
 * to simply remove the row from the table.
 *
 * @author Will Droste
 * @author Keith Yarbrough
 * @version $Revision $
 * @since 1.0
 */
@ConnectorClass(displayNameKey = "DBTABLE_CONNECTOR", configurationClass = DatabaseTableConfiguration.class)
public class DatabaseTableConnector implements
        PoolableConnector, CreateOp, SearchOp<FilterWhereBuilder>,
        DeleteOp, UpdateOp, SchemaOp, TestOp, AuthenticateOp, SyncOp,
        ResolveUsernameOp {

    /**
     * Setup logging for the {@link DatabaseTableConnector}.
     */
    private static final Log LOG = Log.getLog(DatabaseTableConnector.class);

    /**
     * A "hashed password" attribute. If this attribute is "true" then the value supplied for
     * the password attribute is assumed to be hashed according to the defined password digest
     * algorithm, and hence is not hashed (again).
     */
    private static final String HASHED_PASSWORD_ATTRIBUTE = AttributeUtil.createSpecialName("HASHED_PASSWORD");

    /**
     * Place holder for the {@link Connection} passed into the callback.
     */
    private DatabaseTableConnection conn;

    /**
     * Place holder for the {@link Configuration} passed into the callback.
     */
    private DatabaseTableConfiguration config;

    /**
     * Schema cache is used. The schema creation need a jdbc query.
     */
    private Schema schema;

    /**
     * Default attributes to get, created and cached from the schema.
     */
    private Set<String> defaultAttributesToGet;

    /**
     * Same of the data types must be converted.
     */
    private Map<String, Integer> columnSQLTypes;

    /**
     * Cached value for required columns.
     */
    private Set<String> stringColumnRequired;

    // =======================================================================
    // Initialize/dispose methods..
    // =======================================================================
    @Override
    public Configuration getConfiguration() {
        return this.config;
    }

    @Override
    public void init(final Configuration cfg) {
        LOG.info("init DatabaseTable connector");
        this.config = (DatabaseTableConfiguration) cfg;
        this.schema = null;
        this.defaultAttributesToGet = null;
        this.columnSQLTypes = null;
        LOG.ok("init DatabaseTable connector ok, connection is valid");
    }

    @Override
    public void checkAlive() {
        LOG.info("checkAlive DatabaseTable connector");
        try {
            if (StringUtil.isNotBlank(config.getDatasource())) {
                openConnection();
            } else {
                getConn().test();
                commit();
            }
        } catch (SQLException e) {
            LOG.error(e, "error in checkAlive");
            throw ConnectorException.wrap(e);
        }

        //Check alive will not close the connection, the next API call is expected
        LOG.ok("checkAlive DatabaseTable connector ok");
    }

    /**
     * The connector connection access method.
     *
     * @return connection
     */
    protected DatabaseTableConnection getConn() {
        //Lazy initialize the connection
        if (conn == null) {
            this.config.validate();
            //Validate first to minimize wrong resource access
            this.conn = DatabaseTableConnection.createDBTableConnection(
                    this.config);
        }
        return conn;
    }

    /**
     * Disposes of the {@link DatabaseTableConnector}'s resources.
     */
    @Override
    public void dispose() {
        LOG.info("dispose DatabaseTable connector");
        if (conn != null) {
            conn.dispose();
            conn = null;
        }
        this.defaultAttributesToGet = null;
        this.schema = null;
        this.columnSQLTypes = null;
    }

    @Override
    public Uid create(final ObjectClass oclass, final Set<Attribute> attrs, final OperationOptions options) {
        LOG.info("create account, check the ObjectClass");

        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("Object class ok");

        if (attrs == null || attrs.isEmpty()) {
            throw new IllegalArgumentException(config.getMessage(MSG_INVALID_ATTRIBUTE_SET));
        }

        LOG.ok("Attribute set is not empty");

        //Name must be present in attribute set or must be generated UID set on
        final Name name = AttributeUtil.getNameFromAttributes(attrs);

        if (name == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_NAME_BLANK));
        }

        final String accountName = name.getNameValue();

        LOG.ok("Required Name attribure value {0} for create", accountName);

        final String tblname = config.getTable();

        // start the insert statement
        final InsertIntoBuilder bld = new InsertIntoBuilder();

        LOG.info("Creating account: {0}", accountName);

        final Set<String> missingRequiredColumns = CollectionUtil.newCaseInsensitiveSet();

        if (config.isEnableEmptyString()) {
            final Set<String> mrc = getStringColumnReguired();
            LOG.info("Add missing required columns {0}", mrc);
            missingRequiredColumns.addAll(mrc);
        }

        LOG.info("process and check the Attribute Set");

        final Set<Attribute> attrToBeProcessed = new HashSet<Attribute>(attrs);

        // If status column is specified attribute __ENABLED__ must be specified.
        // If attribute __ENABLED__ is not specified a default value must be
        // provided.
        if (StringUtil.isNotBlank(config.getStatusColumn())) {
            final Attribute enabled = AttributeUtil.find(OperationalAttributes.ENABLE_NAME, attrToBeProcessed);
            if (enabled == null) {
                attrToBeProcessed.add(AttributeBuilder.build(
                        OperationalAttributes.ENABLE_NAME,
                        isEnabled(config.getDefaultStatusValue())));
            }
        }

        // Find out whether the supplied password should be hashed or not
        boolean hashedPassword = false;

        final Attribute hashedPasswordAttribute = AttributeUtil.find(HASHED_PASSWORD_ATTRIBUTE, attrToBeProcessed);

        if (hashedPasswordAttribute != null && hashedPasswordAttribute.getValue() != null
                && !hashedPasswordAttribute.getValue().isEmpty()
                && hashedPasswordAttribute.getValue().get(0) instanceof Boolean) {
            hashedPassword = (Boolean) hashedPasswordAttribute.getValue().get(0);
            attrToBeProcessed.remove(hashedPasswordAttribute);
        }

        //All attribute names should be in create columns statement 
        for (Attribute attr : attrToBeProcessed) {
            // quoted column name
            if (!attr.getName().equals(config.getKeyColumn())) {
                final String columnName = getColumnName(attr.getName());

                if (StringUtil.isNotBlank(columnName)) {
                    handleAttribute(bld, attr, hashedPassword, columnName);
                    missingRequiredColumns.remove(columnName);
                    LOG.ok("Attribute {0} was added to insert", attr.getName());
                } else {
                    LOG.ok("Attribute {0} ignored. Missing internal mapping", attr.getName());
                }
            }
        }

        // Bind empty string for not-null columns which are not in attribute set list
        if (config.isEnableEmptyString()) {
            LOG.info("Some columns should be empty");
            for (String mCol : missingRequiredColumns) {
                bld.addBind(new SQLParam(quoteName(mCol), DatabaseTableConstants.EMPTY_STR, getColumnType(mCol)));
                LOG.ok("Required empty value to column {0} added", mCol);
            }
        }

        final String SQL_INSERT = "INSERT INTO {0} ( {1} ) VALUES ( {2} )";

        // create the prepared statement..
        final String sql = MessageFormat.format(SQL_INSERT, tblname, bld.getInto(), bld.getValues());

        PreparedStatement pstmt = null;

        try {
            openConnection();
            pstmt = getConn().prepareStatement(sql, bld.getParams());
            // execute the SQL statement
            pstmt.execute();
            LOG.info("Create account {0} commit", accountName);
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Create account ''{0}'' error", accountName);
            if (throwIt(e.getErrorCode())) {
                SQLUtil.rollbackQuietly(getConn());
                throw new ConnectorException(config.getMessage(MSG_CAN_NOT_CREATE, accountName), e);
            }
        } finally {
            // clean up...
            SQLUtil.closeQuietly(pstmt);
            closeConnection();
        }

        LOG.ok("Account {0} created", accountName);

        // create and return the uid..
        return new Uid(accountName);
    }

    /**
     * Test to throw the exception.
     *
     * @param e exception
     * @return true/false
     */
    private boolean throwIt(int errorCode) {
        return config.isRethrowAllSQLExceptions() || errorCode != 0;
    }

    /**
     * Test is value is null and must be empty
     *
     * @param columnName the column name
     * @param value the value to tests
     * @return true/false
     */
    private boolean isToBeEmpty(final String columnName, Object value) {
        return config.isEnableEmptyString() && getStringColumnReguired().contains(columnName) && value == null;
    }

    @Override
    public void delete(final ObjectClass oclass, final Uid uid, final OperationOptions options) {
        LOG.info("delete account, check the ObjectClass");

        final String SQL_DELETE = "DELETE FROM {0} WHERE {1} = ?";
        PreparedStatement stmt = null;
        // create the SQL string..
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The ObjectClass is ok");
        if (uid == null || (uid.getUidValue() == null)) {
            throw new IllegalArgumentException(config.getMessage(MSG_UID_BLANK));
        }

        final String accountUid = uid.getUidValue();
        LOG.ok("The Uid is present");
        final String tblname = config.getTable();
        final String keycol = quoteName(config.getKeyColumn());
        final String sql = MessageFormat.format(SQL_DELETE, tblname, keycol);
        try {
            LOG.info("delete account SQL {0}", sql);
            openConnection();
            // create a prepared call..
            stmt = getConn().getConnection().prepareStatement(sql);

            // set object to delete..
            stmt.setString(1, accountUid);

            // uid to delete..
            LOG.info("Deleting account Uid: {0}", accountUid);
            final int dr = stmt.executeUpdate();

            if (dr < 1) {
                LOG.error("No account Uid: {0} found", accountUid);
                SQLUtil.rollbackQuietly(getConn());
                throw new UnknownUidException();
            }

            if (dr > 1) {
                LOG.error("More then one account Uid: {0} found", accountUid);
                SQLUtil.rollbackQuietly(getConn());
                throw new IllegalArgumentException(config.getMessage(MSG_MORE_USERS_DELETED, accountUid));
            }

            LOG.info("Delete account {0} commit", accountUid);
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Delete account ''{0}'' SQL error", accountUid);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_DELETE, accountUid), e);
        } finally {
            // clean up..
            SQLUtil.closeQuietly(stmt);
            closeConnection();
        }

        LOG.ok("Account Uid {0} deleted", accountUid);
    }

    @Override
    public Uid update(
            final ObjectClass oclass,
            final Uid uid,
            final Set<Attribute> attrs,
            final OperationOptions options) {

        LOG.info("update account, check the ObjectClass");
        final String SQL_TEMPLATE = "UPDATE {0} SET {1} WHERE {2} = ?";

        // create the sql statement..
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The ObjectClass is ok");

        if (attrs == null || attrs.isEmpty()) {
            throw new IllegalArgumentException(config.getMessage(MSG_INVALID_ATTRIBUTE_SET));
        }

        LOG.ok("Attribute set is not empty");

        final String accountUid = uid.getUidValue();
        Assertions.nullCheck(accountUid, "accountUid");
        LOG.ok("Account uid {0} is present", accountUid);

        // Find out whether the supplied password should be hashed or not
        boolean hashedPassword = false;

        final Attribute hashedPasswordAttribute = AttributeUtil.find(HASHED_PASSWORD_ATTRIBUTE, attrs);

        if (hashedPasswordAttribute != null && hashedPasswordAttribute.getValue() != null
                && !hashedPasswordAttribute.getValue().isEmpty()
                && hashedPasswordAttribute.getValue().get(0) instanceof Boolean) {
            hashedPassword = (Boolean) hashedPasswordAttribute.getValue().get(0);
            attrs.remove(hashedPasswordAttribute);
        }

        Uid ret = uid;

        // The update is changing name. The oldUid is a key and the name will become new uid.
        final Name name = AttributeUtil.getNameFromAttributes(attrs);
        String accountName = accountUid;

        if (name != null && !accountUid.equals(name.getNameValue())) {
            accountName = name.getNameValue();
            Assertions.nullCheck(accountName, "accountName");
            LOG.info("Account name {0} is present and is not the same as uid", accountName);
            ret = new Uid(accountName);
            LOG.ok("Renaming account uid {0} to name {1}", accountUid, accountName);
        }

        LOG.info("process and check the Attribute Set");

        UpdateSetBuilder updateSet = new UpdateSetBuilder();
        for (Attribute attr : attrs) {

            // All attributes needs to be updated except the UID
            if (!attr.is(Uid.NAME) && !attr.getName().equals(config.getKeyColumn())) {
                final String attributeName = attr.getName();
                final String columnName = getColumnName(attributeName);
                if (StringUtil.isNotBlank(columnName)) {
                    handleAttribute(updateSet, attr, hashedPassword, columnName);
                    LOG.ok("Attribute {0} was added to update", attr.getName());
                } else {
                    LOG.ok("Attribute {0} ignored. Missing internal mapping", attr.getName());
                }
            }
        }

        LOG.info("Update account {1}", accountName);
        // Format the update query
        final String tblname = config.getTable();
        final String keycol = quoteName(config.getKeyColumn());
        updateSet.addValue(new SQLParam(keycol, accountUid, getColumnType(config.getKeyColumn())));
        final String sql = MessageFormat.format(SQL_TEMPLATE, tblname, updateSet.getSQL(), keycol);
        PreparedStatement stmt = null;
        try {
            openConnection();
            // create the prepared statement..
            stmt = getConn().prepareStatement(sql, updateSet.getParams());
            stmt.executeUpdate();
            // commit changes
            LOG.info("Update account {0} commit", accountName);
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Update account {0} error", accountName);
            if (throwIt(e.getErrorCode())) {
                SQLUtil.rollbackQuietly(getConn());
                throw new ConnectorException(config.getMessage(MSG_CAN_NOT_UPDATE, accountName), e);
            }
        } finally {
            // clean up..
            SQLUtil.closeQuietly(stmt);
            closeConnection();
        }
        LOG.ok("Account {0} updated", accountName);
        return ret;
    }

    @Override
    public FilterTranslator<FilterWhereBuilder> createFilterTranslator(
            final ObjectClass oclass, final OperationOptions options) {

        LOG.info("check the ObjectClass");
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }
        LOG.ok("The ObjectClass is ok");
        return new DatabaseTableFilterTranslator(this, oclass, options);
    }

    @Override
    public void executeQuery(
            final ObjectClass oclass,
            final FilterWhereBuilder where,
            final ResultsHandler handler,
            final OperationOptions options) {

        LOG.info("check the ObjectClass and result handler");

        // Contract tests
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        if (handler == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_RESULT_HANDLER_NULL));
        }

        LOG.ok("The ObjectClass and result handler is ok");

        //Names
        final String tblname = config.getTable();
        final Set<String> columnNamesToGet = resolveColumnNamesToGet(options);

        LOG.ok("Column Names {0} To Get", columnNamesToGet);

        // For all account query there is no need to replace or quote anything
        final DatabaseQueryBuilder query = new DatabaseQueryBuilder(tblname, columnNamesToGet);
        query.setWhere(where);

        ResultSet result = null;
        PreparedStatement statement = null;

        try {
            openConnection();
            statement = getConn().prepareStatement(query);
            result = statement.executeQuery();

            LOG.ok("executeQuery {0} on {1}", query.getSQL(), oclass);

            while (result.next()) {
                final Map<String, SQLParam> columnValues = getConn().getColumnValues(result);
                LOG.ok("Column values {0} from result set ", columnValues);

                // create the connector object
                final ConnectorObjectBuilder bld = buildConnectorObject(columnValues);

                if (!handler.handle(bld.build())) {
                    LOG.ok("Stop processing of the result set");
                    break;
                }
            }

            // commit changes
            LOG.info("commit executeQuery account");
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Query {0} on {1} error", query.getSQL(), oclass);
            SQLUtil.rollbackQuietly(getConn());
            if (throwIt(e.getErrorCode())) {
                throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, tblname), e);
            }
        } finally {
            SQLUtil.closeQuietly(result);
            SQLUtil.closeQuietly(statement);
            closeConnection();
        }

        LOG.ok("Query Account commited");
    }

    @Override
    public void sync(
            final ObjectClass oclass,
            final SyncToken token,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        LOG.info("check the ObjectClass and result handler");

        // Contract tests    
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The object class is ok");

        if (handler == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_RESULT_HANDLER_NULL));
        }

        LOG.ok("The result handles is not null");

        // Check if changelog column is defined in the config
        if (StringUtil.isBlank(config.getChangeLogColumn())) {
            throw new IllegalArgumentException(config.getMessage(MSG_CHANGELOG_COLUMN_BLANK));
        }

        LOG.ok("The change log column is ok");

        // Names
        final String tblname = config.getTable();
        final String changeLogColumnName = quoteName(config.getChangeLogColumn());

        LOG.ok("Change log attribute {0} map to column name {1}", config.getChangeLogColumn(), changeLogColumnName);

        final Set<String> columnNames = resolveColumnNamesToGet(options);
        LOG.ok("Column Names {0} To Get", columnNames);

        final List<OrderBy> orderBy = new ArrayList<>();

        //Add also the token column
        columnNames.add(changeLogColumnName);

        orderBy.add(new OrderBy(changeLogColumnName, true));
        LOG.ok("OrderBy {0}", orderBy);

        // The first token is not null set the FilterWhereBuilder
        final FilterWhereBuilder where = new FilterWhereBuilder();

        if (token != null && token.getValue() != null) {
            LOG.info("Sync token is {0}", token.getValue());
            final Integer sqlType = getColumnType(config.getChangeLogColumn());

            Object tokenVal;
            try {
                tokenVal = SQLUtil.attribute2jdbcValue(token.getValue().toString(), sqlType);
            } catch (Exception e) {
                tokenVal = token.getValue();
            }

            where.addBind(new SQLParam(changeLogColumnName, tokenVal, sqlType), ">", false);
        }

        final DatabaseQueryBuilder query = new DatabaseQueryBuilder(tblname, columnNames);
        query.setWhere(where);
        query.setOrderBy(orderBy);

        ResultSet result = null;
        PreparedStatement statement = null;

        try {
            openConnection();
            statement = getConn().prepareStatement(query);
            result = statement.executeQuery();
            LOG.info("execute sync query {0} on {1}", query.getSQL(), oclass);

            while (result.next()) {
                final Map<String, SQLParam> columnValues = getConn().getColumnValues(result);
                LOG.ok("Column values {0} from sync result set ", columnValues);

                // create the connector object..
                final SyncDeltaBuilder sdb = buildSyncDelta(columnValues);
                if (!handler.handle(sdb.build())) {
                    LOG.ok("Stop processing of the sync result set");
                    break;
                }
            }

            // commit changes
            LOG.info("commit sync account");
            commit();
        } catch (SQLException e) {
            LOG.error(e, "sync {0} on {1} error", query.getSQL(), oclass);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, tblname), e);
        } finally {
            SQLUtil.closeQuietly(result);
            SQLUtil.closeQuietly(statement);
            closeConnection();
        }

        LOG.ok("Sync Account commited");
    }

    @Override
    public SyncToken getLatestSyncToken(final ObjectClass oclass) {
        LOG.info("check the ObjectClass");

        // Contract tests    
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The object class is ok");

        // Check if changelog column is defined in the config
        if (StringUtil.isBlank(config.getChangeLogColumn())) {
            throw new IllegalArgumentException(config.getMessage(MSG_CHANGELOG_COLUMN_BLANK));
        }

        LOG.ok("The change log column is ok");

        // Format the update query
        final String SQL_SELECT = "SELECT MAX( {0} ) FROM {1}";
        final String tblname = config.getTable();
        final String chlogName = quoteName(config.getChangeLogColumn());
        final String sql = MessageFormat.format(SQL_SELECT, chlogName, tblname);

        SyncToken ret = null;

        LOG.info("getLatestSyncToken on {0}", oclass);

        PreparedStatement stmt = null;
        ResultSet rset = null;

        try {
            openConnection();

            // create the prepared statement..
            stmt = getConn().getConnection().prepareStatement(sql);
            rset = stmt.executeQuery();

            LOG.ok("The statement {0} executed", sql);

            if (rset.next()) {
                if (rset.getString(1) != null) {
                    final String value = rset.getString(1);
                    LOG.ok("New token value {0}", value);

                    int sqlType = getColumnType(chlogName);

                    // Parse as string to be independent of DBMS.                    
                    final boolean isDate = sqlType == 91 || sqlType == 93 || sqlType == 92;

                    if (isDate) {
                        ret = new SyncToken(SQLUtil.jdbc2AttributeValue(DatabaseTableSQLUtil.tsAsLong(value)));
                    } else {
                        try {
                            ret = new SyncToken(SQLUtil.jdbc2AttributeValue(Long.valueOf(value)));
                        } catch (NumberFormatException nfe) {
                            LOG.warn(nfe, "Invalid token value {0}", rset.getString(1));
                        }
                    }
                }

                LOG.ok("getLatestSyncToken", ret);
                // commit changes
                LOG.info("commit getLatestSyncToken");
                commit();
            }
        } catch (SQLException e) {
            LOG.error(e, "getLatestSyncToken sql {0} on {1} error", sql, oclass);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, tblname), e);
        } finally {
            // clean up..
            SQLUtil.closeQuietly(rset);
            SQLUtil.closeQuietly(stmt);
            closeConnection();
        }

        LOG.ok("getLatestSyncToken commited");
        return ret;
    }

    // =======================================================================
    // Schema..
    // =======================================================================
    @Override
    public Schema schema() {
        try {
            openConnection();
            if (schema == null) {
                LOG.info("cache schema");
                cacheSchema();
            }
            assert schema != null;
            commit();
        } catch (SQLException e) {
            LOG.error(e, "error in schema");
            throw ConnectorException.wrap(e);
        } finally {
            closeConnection();
        }

        LOG.ok("schema");
        return schema;
    }

    @Override
    public void test() {
        LOG.info("test");

        try {
            openConnection();
            getConn().test();
            commit();
        } catch (SQLException e) {
            LOG.error(e, "error in test");
            throw ConnectorException.wrap(e);
        } finally {
            closeConnection();
        }
        LOG.ok("connector test ok");
    }

    private void closeConnection() {
        getConn().closeConnection();
    }

    private void openConnection() throws SQLException {
        getConn().openConnection();
    }

    private void commit() throws SQLException {
        getConn().getConnection().commit();
    }

    @Override
    public Uid authenticate(
            final ObjectClass oclass,
            final String username,
            final GuardedString password,
            final OperationOptions options) {

        final String SQL_AUTH_QUERY = "SELECT {0} FROM {1} WHERE ( {0} = ? ) AND ( {2} = ? )";

        LOG.info("check the ObjectClass");

        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The object class is ok");

        if (StringUtil.isBlank(config.getPasswordColumn())) {
            throw new UnsupportedOperationException(config.getMessage(MSG_AUTHENTICATE_OP_NOT_SUPPORTED));
        }

        LOG.ok("The Password Column is ok");

        // determine if you can get a connection to the database..
        if (StringUtil.isBlank(username)) {
            throw new IllegalArgumentException(config.getMessage(MSG_USER_BLANK));
        }

        LOG.ok("The username is ok");

        // check that there is a pwd to query..
        if (password == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_PASSWORD_BLANK));
        }

        LOG.ok("The password is ok");

        GuardedString encodedPwd;

        try {
            encodedPwd = encodePassword(password);
        } catch (PasswordEncodingException e) {
            LOG.error(e, "Error encoding provided password");
            encodedPwd = password;
        }

        final String keyColumnName = quoteName(config.getKeyColumn());
        final String passwordColumnName = quoteName(config.getPasswordColumn());
        final String sql = MessageFormat.format(SQL_AUTH_QUERY, keyColumnName, config.getTable(), passwordColumnName);
        final List<SQLParam> values = new ArrayList<>();

        values.add(new SQLParam(keyColumnName, username, getColumnType(config.getKeyColumn()))); // real username
        values.add(new SQLParam(passwordColumnName, encodedPwd)); // real password

        PreparedStatement stmt = null;
        ResultSet result = null;
        Uid uid = null;

        //No passwordExpired capability
        try {
            // replace the ? in the SQL_AUTH statement with real data
            LOG.info("authenticate Account: {0}", username);

            openConnection();
            stmt = getConn().prepareStatement(sql, values);
            result = stmt.executeQuery();
            LOG.ok("authenticate query for account {0} executed ", username);

            //No PasswordExpired capability
            if (!result.next()) {
                LOG.error("authenticate query for account {0} has no result ", username);
                throw new InvalidCredentialException(config.getMessage(MSG_AUTH_FAILED, username));
            }

            uid = new Uid(result.getString(1));

            // commit changes
            LOG.info("commit authenticate");
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Account: {0} authentication failed ", username);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, config.getTable()), e);
        } finally {
            SQLUtil.closeQuietly(result);
            SQLUtil.closeQuietly(stmt);
            closeConnection();
        }

        LOG.info("Account: {0} authenticated ", username);
        return uid;
    }

    @Override
    public Uid resolveUsername(final ObjectClass oclass, final String username, final OperationOptions options) {
        final String SQL_AUTH_QUERY = "SELECT {0} FROM {1} WHERE ( {0} = ? )";

        LOG.info("check the ObjectClass");

        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException(config.getMessage(MSG_ACCOUNT_OBJECT_CLASS_REQUIRED));
        }

        LOG.ok("The object class is ok");

        if (StringUtil.isBlank(config.getPasswordColumn())) {
            throw new UnsupportedOperationException(config.getMessage(MSG_AUTHENTICATE_OP_NOT_SUPPORTED));
        }

        LOG.ok("The Password Column is ok");

        // determine if you can get a connection to the database..
        if (StringUtil.isBlank(username)) {
            throw new IllegalArgumentException(config.getMessage(MSG_USER_BLANK));
        }

        LOG.ok("The username is ok");

        final String keyColumnName = quoteName(config.getKeyColumn());
        final String passwordColumnName = quoteName(config.getPasswordColumn());
        final String sql = MessageFormat.format(SQL_AUTH_QUERY, keyColumnName, config.getTable(), passwordColumnName);
        final List<SQLParam> values = new ArrayList<SQLParam>();
        values.add(new SQLParam(keyColumnName, username, getColumnType(config.getKeyColumn()))); // real username

        PreparedStatement stmt = null;
        ResultSet result = null;
        Uid uid = null;

        //No passwordExpired capability
        try {
            // replace the ? in the SQL_AUTH statement with real data
            LOG.info("authenticate Account: {0}", username);

            openConnection();

            stmt = getConn().prepareStatement(sql, values);
            result = stmt.executeQuery();

            LOG.ok("authenticate query for account {0} executed ", username);

            //No PasswordExpired capability
            if (!result.next()) {
                LOG.error("authenticate query for account {0} has no result ", username);
                throw new InvalidCredentialException(config.getMessage(MSG_AUTH_FAILED, username));
            }

            uid = new Uid(result.getString(1));

            // commit changes
            LOG.info("commit authenticate");
            commit();
        } catch (SQLException e) {
            LOG.error(e, "Account: {0} authentication failed ", username);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, config.getTable()), e);
        } finally {
            SQLUtil.closeQuietly(result);
            SQLUtil.closeQuietly(stmt);
            closeConnection();
        }

        LOG.info("Account: {0} authenticated ", username);
        return uid;
    }

    /**
     * Used to escape the table or column name.
     *
     * @param value Value to be quoted
     * @return the quoted column name
     */
    public String quoteName(final String value) {
        return DatabaseTableSQLUtil.quoteName(config.getQuoting(), value);
    }

    /**
     * The required type is cached
     *
     * @param columnName the column name
     * @return the cached column type
     */
    public int getColumnType(final String columnName) {
        if (columnSQLTypes == null) {
            cacheSchema();
        }

        // no null here :)
        assert columnSQLTypes != null;

        Integer columnType = columnSQLTypes.get(columnName);

        if (columnType == null) {
            // throw new IllegalArgumentException("Invalid column name: "+columnName);
            columnType = Types.NULL;
        }

        return columnType;
    }

    /**
     * Convert the attribute name to resource specific columnName
     *
     * @param attributeName attribute bane
     * @return the Column Name value
     */
    public String getColumnName(final String attributeName) {
        if (Name.NAME.equalsIgnoreCase(attributeName)) {
            LOG.ok("attribute name {0} map to key column", attributeName);
            return config.getKeyColumn();
        }

        if (Uid.NAME.equalsIgnoreCase(attributeName)) {
            LOG.ok("attribute name {0} map to key column", attributeName);
            return config.getKeyColumn();
        }

        if (!StringUtil.isBlank(config.getPasswordColumn())
                && OperationalAttributes.PASSWORD_NAME.equalsIgnoreCase(attributeName)) {
            LOG.ok("attribute name {0} map to password column", attributeName);
            return config.getPasswordColumn();
        }

        if (OperationalAttributes.ENABLE_NAME.equalsIgnoreCase(attributeName)) {
            LOG.ok("attribute __ENABLE__ {0} map to key column", attributeName);
            return config.getStatusColumn();
        }

        return attributeName;
    }

    /**
     * Cache schema, defaultAtributesToGet, columnClassNames.
     */
    private void cacheSchema() {
        /*
         * First, compute the account attributes based on the database schema
         */
        final Set<AttributeInfo> attrInfoSet = buildSelectBasedAttributeInfos();

        LOG.info("cacheSchema on {0}", attrInfoSet);

        // Cache the attributes to get
        defaultAttributesToGet = new HashSet<String>();

        for (AttributeInfo info : attrInfoSet) {
            final String name = info.getName();
            if (info.isReturnedByDefault()
                    || (name.equals(OperationalAttributes.PASSWORD_NAME) && config.isRetrievePassword())) {

                defaultAttributesToGet.add(name);
            }
        }

        /*
         * Add any other operational attributes to the attrInfoSet
         */
        // attrInfoSet.add(OperationalAttributeInfos.ENABLE);

        /*
         * Use SchemaBuilder to build the schema. Currently, only ACCOUNT type is supported.
         */
        final SchemaBuilder schemaBld = new SchemaBuilder(getClass());
        final ObjectClassInfoBuilder ociB = new ObjectClassInfoBuilder();
        ociB.setType(ObjectClass.ACCOUNT_NAME);
        ociB.addAllAttributeInfo(attrInfoSet);
        final ObjectClassInfo oci = ociB.build();
        schemaBld.defineObjectClass(oci);

        /*
         * Note: AuthenticateOp, and all the 'SPIOperation'-s are by default added by Reflection API to the Schema.
         *
         * See for details: SchemaBuilder.defineObjectClass() --> FrameworkUtil.getDefaultSupportedOperations()
         * ReflectionUtil.getAllInterfaces(connector); is the line that *does* acquire the implemented interfaces by the
         * connector class.
         */
        if (StringUtil.isBlank(config.getPasswordColumn())) { // remove the AuthenticateOp
            LOG.info("no password column, remove the AuthenticateOp");
            schemaBld.removeSupportedObjectClass(AuthenticateOp.class, oci);
        }

        if (StringUtil.isBlank(config.getChangeLogColumn())) { // remove the SyncOp
            LOG.info("no changeLog column, remove the SyncOp");
            schemaBld.removeSupportedObjectClass(SyncOp.class, oci);
        }

        schema = schemaBld.build();
        LOG.ok("schema built");
    }

    /**
     * Get the schema using a SELECT query.
     *
     * @return Schema based on a empty SELECT query.
     */
    private Set<AttributeInfo> buildSelectBasedAttributeInfos() {
        /**
         * Template for a empty query to get the columns of the table.
         */
        final String SCHEMA_QUERY = "SELECT * FROM {0} WHERE {1} IS NULL";

        LOG.info("get schema from the table");

        final Set<AttributeInfo> attrInfo;
        final String sql = MessageFormat.format(SCHEMA_QUERY, config.getTable(), quoteName(config.getKeyColumn()));

        // check out the result etc..
        ResultSet rset = null;
        Statement stmt = null;

        try {
            // create the query..
            stmt = getConn().getConnection().createStatement();

            LOG.info("executeQuery ''{0}''", sql);
            rset = stmt.executeQuery(sql);

            LOG.ok("query executed");

            // get the results queued..
            attrInfo = buildAttributeInfoSet(rset);

            // commit changes
            LOG.info("commit get schema");
            commit();
        } catch (SQLException ex) {
            LOG.error(ex, "buildSelectBasedAttributeInfo in SQL: ''{0}''", sql);
            SQLUtil.rollbackQuietly(getConn());
            throw new ConnectorException(config.getMessage(MSG_CAN_NOT_READ, config.getTable()), ex);
        } finally {
            SQLUtil.closeQuietly(rset);
            SQLUtil.closeQuietly(stmt);
        }

        LOG.ok("schema created");
        return attrInfo;
    }

    /**
     * Return the set of AttributeInfo based on the database query meta-data.
     *
     * @param rset result set
     * @return attribute info
     * @throws SQLException if anything goes wrong
     */
    private Set<AttributeInfo> buildAttributeInfoSet(final ResultSet rset) throws SQLException {
        LOG.info("build AttributeInfoSet");

        final Set<AttributeInfo> attrInfo = new HashSet<AttributeInfo>();

        columnSQLTypes = CollectionUtil.<Integer>newCaseInsensitiveMap();
        stringColumnRequired = CollectionUtil.newCaseInsensitiveSet();

        final ResultSetMetaData meta = rset.getMetaData();
        int count = meta.getColumnCount();

        for (int i = 1; i <= count; i++) {
            final String name = meta.getColumnName(i);

            final AttributeInfoBuilder attrBld = new AttributeInfoBuilder();

            final Integer columnType = meta.getColumnType(i);

            LOG.ok("column name {0} has type {1}", name, columnType);

            columnSQLTypes.put(name, columnType);

            if (name.equalsIgnoreCase(config.getPasswordColumn())) {
                // Password attribute
                attrInfo.add(OperationalAttributeInfos.PASSWORD);
                LOG.ok("password column in password attribute in the schema");
            } else if (name.equalsIgnoreCase(config.getChangeLogColumn())) {
                // skip changelog column from the schema.
                LOG.ok("skip changelog column from the schema");
            } else if (name.equalsIgnoreCase(config.getStatusColumn())) {
                // status attribute
                final Class<?> dataType = getConn().getSms().getSQLAttributeType(columnType);
                attrBld.setType(dataType);

                attrBld.setName(OperationalAttributes.ENABLE_NAME);

                final boolean required = meta.isNullable(i) == ResultSetMetaData.columnNoNulls;
                attrBld.setRequired(required);

                attrBld.setReturnedByDefault(true);
                attrInfo.add(attrBld.build());
            } else {
                // All other attributed taken from the table
                final Class<?> dataType = getConn().getSms().getSQLAttributeType(columnType);
                attrBld.setType(dataType);
                attrBld.setName(name);

                final boolean required = meta.isNullable(i) == ResultSetMetaData.columnNoNulls;

                attrBld.setRequired(required);
                if (required && dataType.isAssignableFrom(String.class)) {
                    LOG.ok("the column name {0} is string type and required", name);
                    stringColumnRequired.add(name);
                }
                attrBld.setReturnedByDefault(isReturnedByDefault(dataType));
                attrInfo.add(attrBld.build());
                LOG.ok("the column name {0} has data type {1}", name, dataType);

                if (name.equalsIgnoreCase(config.getKeyColumn())) {
                    // name attribute
                    final AttributeInfoBuilder attrBldName = new AttributeInfoBuilder();
                    attrBldName.setName(Name.NAME);
                    //The generate UID make the Name attribute is nor required
                    attrBldName.setRequired(true);
                    attrInfo.add(attrBldName.build());
                    LOG.ok("key column in name attribute in the schema");
                }
            }
        }

        LOG.ok("the Attribute InfoSet is done");
        return attrInfo;
    }

    /**
     * Decide if should be returned by default Generally all byte arrays are not returned by default.
     *
     * @param dataType the type of the attribute type
     * @return
     */
    private boolean isReturnedByDefault(final Class<?> dataType) {
        return !byte[].class.equals(dataType);
    }

    /**
     * Construct a connector object
     * <p>
     * Taking care about special attributes</p>
     *
     * @param columnValues from the result set
     * @return ConnectorObjectBuilder object
     */
    private ConnectorObjectBuilder buildConnectorObject(final Map<String, SQLParam> columnValues) {
        LOG.info("build ConnectorObject");
        String uidValue = null;
        ConnectorObjectBuilder bld = new ConnectorObjectBuilder();

        for (Map.Entry<String, SQLParam> colValue : columnValues.entrySet()) {
            final String columnName = colValue.getKey();
            final SQLParam param = colValue.getValue();

            // Map the special
            if (columnName.equalsIgnoreCase(config.getKeyColumn())) {
                if (param == null || param.getValue() == null) {
                    String msg = "Name cannot be null.";
                    LOG.error(msg);
                    throw new IllegalArgumentException(msg);
                }
                uidValue = param.getValue().toString();
                bld.setName(uidValue);
                bld.addAttribute(AttributeBuilder.build(columnName, param.getValue()));
            } else if (columnName.equalsIgnoreCase(config.getPasswordColumn())) {
                if (config.isRetrievePassword()) {
                    final String pwd = (String) param.getValue();
                    try {
                        if (param.getValue() != null) {
                            bld.addAttribute(AttributeBuilder.buildPassword(decodePassword(pwd).toCharArray()));
                        }
                    } catch (Exception e) {
                        LOG.error(e, "Error decoding password");
                        bld.addAttribute(AttributeBuilder.buildPassword(pwd.toCharArray()));
                    }
                } else {
                    // No Password in the result object
                    LOG.ok("No Password in the result object");
                }
            } else if (columnName.equalsIgnoreCase(config.getStatusColumn())) {
                LOG.ok("statusColumn attribute in the result");
                if (param != null && param.getValue() != null) {
                    bld.addAttribute(AttributeBuilder.buildEnabled(isEnabled(param.getValue().toString())));
                }
            } else if (param != null && param.getValue() != null) {
                bld.addAttribute(AttributeBuilder.build(columnName, param.getValue()));
            } else {
                bld.addAttribute(AttributeBuilder.build(columnName));
            }
        }

        // To be sure that uid and name are present for mysql
        if (uidValue == null) {
            final String msg = "The uid value is missing in query.";
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        // Add Uid attribute to object
        bld.setUid(new Uid(uidValue));

        // only deals w/ accounts..
        bld.setObjectClass(ObjectClass.ACCOUNT);

        LOG.ok("ConnectorObject is built");

        return bld;

    }

    /**
     * Construct a SyncDeltaBuilder the sync builder taking care about special attributes.
     *
     * @param columnValues from the resultSet
     * @return SyncDeltaBuilder the sync builder
     */
    private SyncDeltaBuilder buildSyncDelta(final Map<String, SQLParam> columnValues) {
        LOG.info("buildSyncDelta");

        SyncDeltaBuilder bld = new SyncDeltaBuilder();
        // Find a token
        SQLParam tokenParam = columnValues.get(config.getChangeLogColumn());

        if (tokenParam == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_INVALID_SYNC_TOKEN_VALUE));
        }

        Object token = tokenParam.getValue();

        // Null token, set some acceptable value
        if (token == null) {
            LOG.ok("token value is null, replacing to 0L");
            token = 0L;
        }

        // To be sure that sync token is present
        bld.setToken(new SyncToken(token));

        bld.setObject(buildConnectorObject(columnValues).build());

        // only deals w/ updates
        bld.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);

        LOG.ok("SyncDeltaBuilder is ok");

        return bld;
    }

    private Set<String> resolveColumnNamesToGet(final OperationOptions options) {
        Set<String> attributesToGet = getDefaultAttributesToGet();

        if (options != null && options.getAttributesToGet() != null) {
            attributesToGet = CollectionUtil.newSet(options.getAttributesToGet());
            attributesToGet.add(Uid.NAME); // Ensure the Uid colum is there
        }

        // Replace attributes to quoted columnNames
        Set<String> columnNamesToGet = new HashSet<String>();

        for (String attributeName : attributesToGet) {
            final String columnName = getColumnName(attributeName);
            if (StringUtil.isNotBlank(columnName)) {
                columnNamesToGet.add(quoteName(columnName));
            }
        }

        if (StringUtil.isNotBlank(config.getStatusColumn())
                && !columnNamesToGet.contains(config.getStatusColumn())) {
            columnNamesToGet.add(config.getStatusColumn());
        }

        return columnNamesToGet;
    }

    /**
     * Get the default Attributes to get.
     *
     * @return the Set of default attribute names
     */
    private Set<String> getDefaultAttributesToGet() {
        if (defaultAttributesToGet == null) {
            cacheSchema();
        }
        assert defaultAttributesToGet != null;
        return defaultAttributesToGet;
    }

    /**
     * Get the default Attributes to get.
     *
     * @return the Set of default attribute names
     */
    private Set<String> getStringColumnReguired() {
        if (stringColumnRequired == null) {
            cacheSchema();
        }
        assert stringColumnRequired != null;
        return stringColumnRequired;
    }

    /**
     * Return true if and only if status is not equals to 'disabledStatusValued'.
     *
     * @param status.
     * @return 'true' if enabled; 'false' otherwise.
     */
    private boolean isEnabled(final String status) {
        return StringUtil.isBlank(status)
                ? StringUtil.isNotBlank(config.getDisabledStatusValue())
                : !status.equalsIgnoreCase(config.getDisabledStatusValue());
    }

    /**
     * Return entry status based on given parameter.
     *
     * @param enabled should be the value of __ENABLED__
     * @return status column value.
     */
    private String getStatusColumnValue(final String enabled) {
        return "TRUE".equalsIgnoreCase(enabled) ? config.getEnabledStatusValue() : config.getDisabledStatusValue();
    }

    private <T extends OperationBuilder> void handleAttribute(
            final T builder,
            final Attribute attribute,
            boolean hashedPassword,
            final String cname) {

        Object value = AttributeUtil.getSingleValue(attribute);

        //Empty String
        if (isToBeEmpty(cname, value)) {
            LOG.info("Attribute {0} should be empty", cname);
            value = DatabaseTableConstants.EMPTY_STR;
        }

        final int sqlType = getColumnType(cname);

        LOG.info("attribute {0} fit column {1} and sql type {2}", attribute.getName(), cname, sqlType);

        try {
            if (sqlType == 91 || sqlType == 93 || sqlType == 92) {
                Object tokenVal;
                try {
                    tokenVal = SQLUtil.attribute2jdbcValue(value.toString(), sqlType);
                } catch (Exception e) {
                    tokenVal = new Timestamp(DatabaseTableSQLUtil.tsAsLong(value.toString()));
                }
                builder.addBind(new SQLParam(quoteName(cname), tokenVal, sqlType));
            } else if (cname.equalsIgnoreCase(config.getStatusColumn())) {
                builder.addBind(new SQLParam(
                        quoteName(cname),
                        getStatusColumnValue(value.toString()),
                        sqlType));
            } else if (cname.equalsIgnoreCase(config.getPasswordColumn())) {

                if (hashedPassword) {
                    final String[] password = { null };
                    ((GuardedString) value).access(new GuardedString.Accessor() {

                        @Override
                        public void access(char[] clearChars) {
                            password[0] = new String(clearChars);
                        }
                    });
                    String encodedPassword = changeCaseOfEncodedPassword(password[0]);
                    // password encryption                  
                    builder.addBind(new SQLParam(
                            quoteName(cname),
                            new GuardedString(encodedPassword.toCharArray()),
                            sqlType));
                } else {
                    // password encryption                  
                    builder.addBind(new SQLParam(
                            quoteName(cname),
                            encodePassword((GuardedString) value),
                            sqlType));
                }
            } else {
                builder.addBind(new SQLParam(quoteName(cname), value, sqlType));
            }
        } catch (Throwable t) {
            LOG.error(t, "Error parsing value '{0}' of attribute {1}:{2}", value, cname, sqlType);
        }
    }

    private GuardedString encodePassword(final GuardedString guarded) throws PasswordEncodingException {
        String cipherAlgorithm;
        try {
            cipherAlgorithm = SupportedAlgorithm.valueOf(config.getCipherAlgorithm()).getAlgorithm();
        } catch (Exception e) {
            cipherAlgorithm = config.getCipherAlgorithm();
        }

        final String cipherKey = config.getCipherKey();
        final EncodeAlgorithm algorithm;

        try {
            algorithm = (EncodeAlgorithm) Class.forName(cipherAlgorithm).newInstance();
            if (StringUtil.isNotBlank(cipherKey)) {
                algorithm.setKey(cipherKey);
            }

        } catch (Exception e) {
            LOG.error(e, "Error retrieving algorithm {0}", cipherAlgorithm);
            throw new PasswordEncodingException(e.getMessage());
        }

        final String[] password = { null };
        guarded.access(new GuardedString.Accessor() {

            @Override
            public void access(char[] clearChars) {
                password[0] = new String(clearChars);
            }
        });

        String encodedPwd;
        try {
            encodedPwd = algorithm.encode(password[0], config.getPasswordCharset());
        } catch (UnsupportedPasswordCharsetException e) {
            LOG.error(e, "Error encoding password charset not supported");
            throw new PasswordEncodingException(e.getMessage());
        }

        GuardedString encoded;
        if (StringUtil.isNotBlank(encodedPwd)) {
            encoded = new GuardedString(changeCaseOfEncodedPassword(encodedPwd).toCharArray());
        } else {
            encoded = guarded;
        }
        return encoded;
    }

    private String changeCaseOfEncodedPassword(String encodedPwd) {
        if (StringUtil.isNotBlank(encodedPwd)) {
            return config.isPwdEncodeToUpperCase() ? encodedPwd.toUpperCase()
                    : config.isPwdEncodeToLowerCase() ? encodedPwd.toLowerCase() : encodedPwd;
        }
        return null;
    }

    private String decodePassword(final String password) throws PasswordDecodingException {
        final String decoded;
        String cipherAlgorithm;

        try {
            cipherAlgorithm = SupportedAlgorithm.valueOf(config.getCipherAlgorithm()).getAlgorithm();
        } catch (Exception e) {
            cipherAlgorithm = config.getCipherAlgorithm();
        }

        final String cipherKey = config.getCipherKey();
        final EncodeAlgorithm algorithm;
        try {
            algorithm = (EncodeAlgorithm) Class.forName(cipherAlgorithm).newInstance();
            if (StringUtil.isNotBlank(cipherKey)) {
                algorithm.setKey(cipherKey);
            }
        } catch (Exception e) {
            LOG.error(e, "Error retrieving algorithm {0}", cipherAlgorithm);
            throw new PasswordDecodingException(e.getMessage());
        }

        decoded = algorithm.decode(password, config.getPasswordCharset());
        return decoded;
    }
}
