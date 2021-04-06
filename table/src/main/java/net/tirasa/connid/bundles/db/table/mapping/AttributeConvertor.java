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

import org.identityconnectors.common.Assertions;
import net.tirasa.connid.commons.db.SQLParam;
import net.tirasa.connid.commons.db.SQLUtil;

/**
 * The SQL get/set strategy class implementation convert all jdbc types to attribute supported types.
 *
 * @version $Revision 1.0$
 * @since 1.0
 */
public class AttributeConvertor implements MappingStrategy {

    private final MappingStrategy delegate;

    /**
     * Final sql mapping
     *
     * @param delegate delegate
     */
    public AttributeConvertor(MappingStrategy delegate) {
        Assertions.nullCheck(delegate, "MappingStrategy delegate");
        this.delegate = delegate;
    }

    /* (non-Javadoc)
     * @see org.identityconnectors.databasetable.MappingStrategy#getSQLParam(java.sql.ResultSet, int, int)
     */
    @Override
    public SQLParam getSQLParam(ResultSet resultSet, int i, String name, final int sqlType) throws SQLException {
        // Convert all types to attribute supported types
        final SQLParam param = delegate.getSQLParam(resultSet, i, name, sqlType);
        return new SQLParam(name, SQLUtil.jdbc2AttributeValue(param.getValue()), sqlType);
    }

    /* (non-Javadoc)
     * @see org.identityconnectors.databasetable.MappingStrategy#getSQLAttributeType(int)
     */
    @Override
    public Class<?> getSQLAttributeType(int sqlType) {
        return delegate.getSQLAttributeType(sqlType);
    }

    /* (non-Javadoc)
     * @see org.identityconnectors.databasetable.MappingStrategy#setSQLParam(java.sql.PreparedStatement, int,
     * org.identityconnectors.dbcommon.SQLParam)
     */
    @Override
    public void setSQLParam(final PreparedStatement stmt, final int idx, SQLParam parm) throws SQLException {
        delegate.setSQLParam(stmt, idx, parm);
    }
}
