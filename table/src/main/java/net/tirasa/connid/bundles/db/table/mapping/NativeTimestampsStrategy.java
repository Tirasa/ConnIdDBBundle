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

import org.identityconnectors.common.Assertions;
import net.tirasa.connid.bundles.db.common.SQLParam;

/**
 * The SQL get/set strategy class implementation read native timestamps, and convert the value to the string
 *
 * @version $Revision 1.0$
 * @since 1.0
 */
public class NativeTimestampsStrategy implements MappingStrategy {

    private MappingStrategy delegate;

    /**
     * Final sql mapping
     *
     * @param delegate
     */
    public NativeTimestampsStrategy(final MappingStrategy delegate) {
        Assertions.nullCheck(delegate, "MappingStrategy delegate");
        this.delegate = delegate;
    }

    @Override
    public SQLParam getSQLParam(final ResultSet resultSet, final int i, final String name, final int sqlType)
            throws SQLException {

        return sqlType == Types.TIMESTAMP
                ? new SQLParam(name, resultSet.getTimestamp(i), Types.TIMESTAMP)
                : delegate.getSQLParam(resultSet, i, name, sqlType);
    }

    @Override
    public Class<?> getSQLAttributeType(final int sqlType) {
        return delegate.getSQLAttributeType(sqlType);
    }

    @Override
    public void setSQLParam(final PreparedStatement stmt, final int idx, final SQLParam parm) throws SQLException {
        delegate.setSQLParam(stmt, idx, parm);
    }
}
