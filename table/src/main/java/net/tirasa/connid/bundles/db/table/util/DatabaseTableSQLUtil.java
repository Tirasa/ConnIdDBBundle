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
package net.tirasa.connid.bundles.db.table.util;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import net.tirasa.connid.bundles.db.table.mapping.MappingStrategy;
import net.tirasa.connid.commons.db.SQLParam;
import org.identityconnectors.common.Assertions;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;

/**
 * The SQL helper/util class
 *
 * @version $Revision 1.0$
 * @since 1.0
 */
public final class DatabaseTableSQLUtil {

    /**
     * Never allow this to be instantiated.
     */
    private DatabaseTableSQLUtil() {
        throw new AssertionError();
    }

    /**
     * <p>
     * This method binds the "?" markers in SQL statement with the parameters given as <i>values</i>. It
     * concentrates the replacement of all params. <code>GuardedString</code> are handled so the password is never
     * visible.
     * </p>
     *
     * @param sms mapping strategy
     * @param statement statement
     * @param params a <CODE>List</CODE> of the object arguments
     * @throws SQLException an exception in statement
     */
    public static void setParams(final MappingStrategy sms, final PreparedStatement statement,
            final List<SQLParam> params)
            throws SQLException {
        if (statement == null || params == null) {
            return;
        }
        for (int i = 0; i < params.size(); i++) {
            final int idx = i + 1;
            final SQLParam parm = params.get(i);
            setParam(sms, statement, idx, parm);
        }
    }

    /**
     * <p>
     * This method binds the "?" markers in SQL statement with the parameters given as <i>values</i>. It
     * concentrates the replacement of all params. <code>GuardedString</code> are handled so the password is never
     * visible.
     * </p>
     *
     * @param sms mapping strategy
     * @param statement statement
     * @param params a <CODE>List</CODE> of the object arguments
     * @throws SQLException an exception in statement
     */
    public static void setParams(final MappingStrategy sms, final CallableStatement statement,
            final List<SQLParam> params) throws SQLException {

        //The same as for prepared statements
        setParams(sms, (PreparedStatement) statement, params);
    }

    /**
     * Set the statement parameter
     * <p>
     * It is ready for overloading if necessary</p>
     *
     * @param sms a mapping strategy
     * @param stmt a <CODE>PreparedStatement</CODE> to set the params
     * @param idx an index of the parameter
     * @param parm a parameter Value
     * @throws SQLException a SQL exception
     */
    static void setParam(final MappingStrategy sms, final PreparedStatement stmt, final int idx, final SQLParam parm)
            throws SQLException {

        // Guarded string conversion
        if (parm.getValue() instanceof GuardedString) {
            setGuardedStringParam(sms, stmt, idx, parm);
        } else {
            sms.setSQLParam(stmt, idx, parm);
        }
    }

    /**
     * Read one row from database result set and convert it to columnValues map.
     *
     * @param sms a mapping strategy
     * @param resultSet database data
     * @return The transformed column values map
     * @throws SQLException a SQL exception
     */
    public static Map<String, SQLParam> getColumnValues(final MappingStrategy sms, final ResultSet resultSet)
            throws SQLException {

        Assertions.nullCheck(resultSet, "resultSet");
        final Map<String, SQLParam> ret = CollectionUtil.<SQLParam>newCaseInsensitiveMap();
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int count = meta.getColumnCount();
        for (int i = 1; i <= count; i++) {
            final String name = meta.getColumnName(i);
            final int sqlType = meta.getColumnType(i);
            final SQLParam param = sms.getSQLParam(resultSet, i, name, sqlType);
            ret.put(name, param);
        }
        return ret;
    }

    /**
     * The helper guardedString bind method
     *
     * @param sms a mapping strategy
     * @param stmt to bind to
     * @param idx index of the object
     * @param param a <CODE>GuardedString</CODE> parameter
     * @throws SQLException a SQL exception
     */
    static void setGuardedStringParam(final MappingStrategy sms, final PreparedStatement stmt, final int idx,
            SQLParam param) throws SQLException {

        final GuardedString guard = (GuardedString) param.getValue();
        final String name = param.getName();
        try {
            guard.access(new GuardedString.Accessor() {

                @Override
                public void access(char[] clearChars) {
                    try {
                        //Never use setString, the DB2 database will fail for secured columns
                        sms.setSQLParam(stmt, idx, new SQLParam(name,
                                new String(clearChars), Types.VARCHAR));
                    } catch (SQLException e) {
                        // checked exception are not allowed in the access method 
                        // Lets use the exception softening pattern
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (RuntimeException e) {
            // determine if there's a SQLException and re-throw that..
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw e;
        }
    }

    /**
     * Used to escape the table or column name.
     *
     * @param quoting the string double, single, back, brackets
     * @param value Value to be quoted
     * @return the quoted column name
     */
    public static String quoteName(String quoting, String value) {
        StringBuilder bld = new StringBuilder();
        if (StringUtil.isBlank(quoting) || "none".equalsIgnoreCase(quoting)) {
            bld.append(value);
        } else if ("double".equalsIgnoreCase(quoting)) {
            // for SQL Server, MySQL, NOT DB2, NOT Oracle, Postgresql
            bld.append('"').append(value).append('"');
        } else if ("single".equalsIgnoreCase(quoting)) {
            // for DB2, NOT Oracle, NOT SQL Server, NOT MySQL, ...
            bld.append('\'').append(value).append('\'');
        } else if ("back".equalsIgnoreCase(quoting)) {
            // for MySQL, NOT Oracle, NOT DB2, NOT SQL Server, ...
            bld.append('`').append(value).append('`');
        } else if ("brackets".equalsIgnoreCase(quoting)) {
            // MS SQL Server..
            bld.append('[').append(value).append(']');
        } else {
            throw new IllegalArgumentException();
        }
        return bld.toString();
    }

    /**
     * Timestamp string as long.
     *
     * @param ts timestamp as string.
     * @return timestamp as long.
     */
    public static long tsAsLong(final String ts) {
        final long time;

        if (ts.matches("\\d\\d:\\d\\d:\\d\\d")) {
            time = Time.valueOf(ts).getTime();
        } else if (ts.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d")) {
            time = Date.valueOf(ts).getTime();
        } else {
            time = Timestamp.valueOf(ts).getTime();
        }

        return time;
    }
}
