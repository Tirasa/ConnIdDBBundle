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

import static net.tirasa.connid.commons.scripted.Constants.MSG_OBJECT_CLASS_REQUIRED;

import java.util.HashMap;
import java.util.Map;
import net.tirasa.connid.commons.scripted.AbstractScriptedConnector;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;

/**
 * Main implementation of the ScriptedJDBC Connector.
 */
@ConnectorClass(displayNameKey = "ScriptedSQL", configurationClass = ScriptedSQLConfiguration.class)
public class ScriptedSQLConnector extends AbstractScriptedConnector<ScriptedSQLConfiguration>
        implements PoolableConnector {

    /**
     * Place holder for the Connection created in the init method
     */
    private ScriptedSQLConnection connection;

    /**
     * Callback method to receive the {@link Configuration}.
     *
     * @param cfg configuration
     * @see Connector#init
     */
    @Override
    public void init(final Configuration cfg) {
        this.config = (ScriptedSQLConfiguration) cfg;

        this.connection = new ScriptedSQLConnection(this.config);

        super.init(cfg);
    }

    /**
     * Disposes of the {@link ScriptedSQLConnector}'s resources.
     *
     * @see Connector#dispose()
     */
    @Override
    public void dispose() {
        config = null;
        if (connection != null) {
            connection.dispose();
            connection = null;
        }
    }

    @Override
    public void checkAlive() {
        connection.test();
    }

    @Override
    protected Map<String, Object> buildArguments() {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("configuration", config);
        arguments.put("connection", connection.getSqlConnection());
        return arguments;
    }

    @Override
    public FilterTranslator<Map<String, Object>> createFilterTranslator(
            final ObjectClass objClass, final OperationOptions options) {

        if (objClass == null) {
            throw new IllegalArgumentException(config.getMessage(MSG_OBJECT_CLASS_REQUIRED));
        }
        LOG.ok("ObjectClass: {0}", objClass.getObjectClassValue());
        return new ScriptedSQLFilterTranslator();
    }
}
