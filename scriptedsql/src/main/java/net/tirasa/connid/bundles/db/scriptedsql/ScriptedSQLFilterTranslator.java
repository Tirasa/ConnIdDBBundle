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
package net.tirasa.connid.bundles.db.scriptedsql;

import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.filter.*;
import org.identityconnectors.common.StringUtil;

import java.util.Map;
import java.util.HashMap;

/**
 * This is an implementation of AbstractFilterTranslator that gives a concrete representation
 * of which filters can be applied at the connector level (natively). If the
 * ScriptedJDBC doesn't support a certain expression type, that factory
 * method should return null. This level of filtering is present only to allow any
 * native constructs that may be available to help reduce the result set for the framework,
 * which will (strictly) reapply all filters specified after the connector does the initial
 * filtering.<p>
 * <p>
 * Note: The generic query type is most commonly a String, but does not have to be.
 */
public class ScriptedSQLFilterTranslator extends AbstractFilterTranslator<Map<String, Object>> {

    /**
     * {@inheritDoc}
     */
    private Map<String, Object> createMap(String operation, AttributeFilter filter, boolean not) {
        Map<String, Object> map = new HashMap<String, Object>();
        String name = filter.getAttribute().getName();
        String value = AttributeUtil.getAsStringValue(filter.getAttribute());
        if (StringUtil.isBlank(value)) {
            return null;
        } else {
            map.put("not", not);
            map.put("operation", operation);
            map.put("left", name);
            map.put("right", value);
            return map;
        }
    }

    @Override
    protected Map<String, Object> createContainsExpression(ContainsFilter filter, boolean not) {
        return createMap("CONTAINS", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createEndsWithExpression(EndsWithFilter filter, boolean not) {
        return createMap("ENDSWITH", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createStartsWithExpression(StartsWithFilter filter, boolean not) {
        return createMap("STARTSWITH", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createEqualsExpression(EqualsFilter filter, boolean not) {
        return createMap("EQUALS", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createAndExpression(
            Map<String, Object> leftExpression, Map<String, Object> rightExpression) {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("operation", "AND");
        map.put("left", leftExpression);
        map.put("right", rightExpression);
        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createOrExpression(
            Map<String, Object> leftExpression, Map<String, Object> rightExpression) {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("operation", "OR");
        map.put("left", leftExpression);
        map.put("right", rightExpression);
        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createGreaterThanExpression(GreaterThanFilter filter, boolean not) {
        return createMap("GREATERTHAN", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createGreaterThanOrEqualExpression(GreaterThanOrEqualFilter filter, boolean not) {
        return createMap("GREATERTHANOREQUAL", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createLessThanExpression(LessThanFilter filter, boolean not) {
        return createMap("LESSTHAN", filter, not);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String, Object> createLessThanOrEqualExpression(LessThanOrEqualFilter filter, boolean not) {
        return createMap("LESSTHANOREQUAL", filter, not);
    }
}
