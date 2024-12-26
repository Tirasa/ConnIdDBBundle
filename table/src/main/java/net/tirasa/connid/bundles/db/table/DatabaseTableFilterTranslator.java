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

import org.identityconnectors.common.StringUtil;
import net.tirasa.connid.bundles.db.table.util.DatabaseTableSQLUtil;
import net.tirasa.connid.commons.db.DatabaseFilterTranslator;
import net.tirasa.connid.commons.db.SQLParam;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;

/**
 * Database table filter translator.
 */
public class DatabaseTableFilterTranslator extends DatabaseFilterTranslator {

    private final DatabaseTableConnector connector;

    public DatabaseTableFilterTranslator(
            final DatabaseTableConnector connector, final ObjectClass oclass, final OperationOptions options) {

        super(oclass, options);
        this.connector = connector;
    }

    @Override
    protected SQLParam getSQLParam(
            final Attribute attribute, final ObjectClass oclass, final OperationOptions options) {

        final Object value = AttributeUtil.getSingleValue(attribute);
        final String columnName = connector.getColumnName(attribute.getName());

        if (StringUtil.isNotBlank(columnName)) {
            final Integer columnType = connector.getColumnType(columnName);
            final String quoting = ((DatabaseTableConfiguration) this.connector.getConfiguration()).getQuoting();
            final String quotedName = DatabaseTableSQLUtil.quoteName(quoting, columnName);
            return new SQLParam(columnName, value, columnType, quotedName);
        } else {
            return null;
        }
    }
}
