/**
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.
 * Copyright 2011-2013 Tirasa. All rights reserved.
 *
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License"). You may not use this file
 * except in compliance with the License.
 *
 * You can obtain a copy of the License at https://oss.oracle.com/licenses/CDDL
 * See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at https://oss.oracle.com/licenses/CDDL.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.bundles.db.table.mapping;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.connid.bundles.db.common.SQLParam;

/**
 * The SQL mapping interface.
 * The delegate pattern chain applied using this interface will take care about SQL mapping   
 * @author Petr Jung
 * @version $Revision 1.0$
 * @since 1.0
 */
public interface MappingStrategy {

    /**
     * Retrieve the SQL value from result set
     * @param resultSet the result set
     * @param i index
     * @param name of the param
     * @param sqlType expected SQL type or  Types.NULL for generic
     * @return the object return the retrieved object
     * @throws SQLException any SQL error
     */
    public SQLParam getSQLParam(ResultSet resultSet, int i, String name, final int sqlType) throws SQLException;

    /**
     * Convert database type to connector supported set of attribute types
     * @param stmt 
     * @param idx 
     * @param parm 
     * @throws SQLException 
     */
    public void setSQLParam(final PreparedStatement stmt, final int idx, SQLParam parm) throws SQLException;
    
    /**
     * Convert database type to connector supported set of attribute types
     * @param sqlType #{@link Types}
     * @return a connector supported class
     */
    public Class<?> getSQLAttributeType(int sqlType);    

}