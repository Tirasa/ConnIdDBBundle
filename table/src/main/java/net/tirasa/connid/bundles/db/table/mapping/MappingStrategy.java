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
package net.tirasa.connid.bundles.db.table.mapping;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import net.tirasa.connid.commons.db.SQLParam;

/**
 * The SQL mapping interface.
 *
 * The delegate pattern chain applied using this interface will take care about SQL mapping.
 */
public interface MappingStrategy {

    /**
     *
     * Retrieve the SQL value from result set
     *
     * @param resultSet the result set
     *
     * @param i index
     *
     * @param name of the param
     *
     * @param sqlType expected SQL type or Types.NULL for generic
     *
     * @return the object return the retrieved object
     *
     * @throws SQLException any SQL error
     *
     */
    SQLParam getSQLParam(ResultSet resultSet, int i, String name, int sqlType) throws SQLException;

    /**
     *
     * Convert database type to connector supported set of attribute types
     *
     * @param stmt
     *
     * @param idx
     *
     * @param parm
     *
     * @throws SQLException *
     */
    void setSQLParam(final PreparedStatement stmt, final int idx, SQLParam parm) throws SQLException;

    /**
     *
     * Convert database type to connector supported set of attribute types
     *
     * @param sqlType #{@link Types}
     *
     * @return a connector supported class
     *
     */
    Class<?> getSQLAttributeType(int sqlType);
}
