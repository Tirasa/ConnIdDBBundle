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
import net.tirasa.connid.bundles.db.commons.SQLParam;
import net.tirasa.connid.bundles.db.commons.SQLUtil;
import org.identityconnectors.common.Assertions;

/**
 * The SQL get/set strategy class implementation convert attributes back to database types
 *
 * @version $Revision 1.0$
 * @since 1.0
 */
public class JdbcConvertor implements MappingStrategy {

    private final MappingStrategy delegate;

    /**
     * Final sql mapping.
     *
     * @param delegate delegate
     */
    public JdbcConvertor(final MappingStrategy delegate) {
        Assertions.nullCheck(delegate, "MappingStrategy delegate");
        this.delegate = delegate;
    }

    @Override
    public SQLParam getSQLParam(final ResultSet resultSet, final int i, final String name, final int sqlType)
            throws SQLException {

        //Default processing otherwise
        return delegate.getSQLParam(resultSet, i, name, sqlType);
    }

    @Override
    public Class<?> getSQLAttributeType(final int sqlType) {
        return delegate.getSQLAttributeType(sqlType);
    }

    @Override
    public void setSQLParam(final PreparedStatement stmt, final int idx, final SQLParam parm) throws SQLException {
        final Object val = SQLUtil.attribute2jdbcValue(parm.getValue(), parm.getSqlType());
        delegate.setSQLParam(stmt, idx, new SQLParam(parm.getName(), val, parm.getSqlType()));
    }
}
