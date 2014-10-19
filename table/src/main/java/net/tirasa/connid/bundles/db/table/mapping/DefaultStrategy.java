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

import net.tirasa.connid.bundles.db.common.SQLParam;
import net.tirasa.connid.bundles.db.common.SQLUtil;

/**
 * The SQL get/set strategy class implementation delegate all activity to dbcommon {@link SQLUtil} functions.
 */
public class DefaultStrategy implements MappingStrategy {

    /**
     * Final sql mapping
     */
    public DefaultStrategy() {
        //
    }

    @Override
    public SQLParam getSQLParam(final ResultSet resultSet, final int i, final String name, final int sqlType)
            throws SQLException {

        return SQLUtil.getSQLParam(resultSet, i, name, sqlType);
    }

    @Override
    public Class<?> getSQLAttributeType(final int sqlType) {
        return SQLUtil.getSQLAttributeType(sqlType);
    }

    @Override
    public void setSQLParam(final PreparedStatement stmt, final int idx, SQLParam parm) throws SQLException {
        SQLUtil.setSQLParam(stmt, idx, parm);
    }
}
