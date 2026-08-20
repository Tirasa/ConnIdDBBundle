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
 * Portions Copyrighted 2026 ConnId.
 */
package net.tirasa.connid.bundles.db.scriptedsql;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import net.tirasa.connid.commons.db.SQLUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.Test;

class ScriptedSQLConnectorTests {

    private static final String TEST_SCRIPT =
            """
            import groovy.sql.Sql;
        
            log.info("Entering {0} Script", action);
            def sql = new Sql(connection);
                                              
            sql.eachRow("select * from Users", { println it.uid} );
            """;

    private static final String SEARCH_SCRIPT =
            """
            import groovy.sql.Sql;

            log.info("Entering {0} Script", action);
            def sql = new Sql(connection);
            def result = []

            sql.eachRow("select * from Users", { row ->
                result.add([__UID__:row["uid"], __NAME__:row["uid"]])
            })

            return result
            """;

    private static String JDBC_URL;

    private static final String DB = "testdb";

    static {
        try {
            EmbeddedPostgres pg = EmbeddedPostgres.builder().start();
            try (Connection conn = pg.getPostgresDatabase().getConnection()) {
                conn.setAutoCommit(true);

                SQLUtil.executeUpdateStatement(conn, "CREATE DATABASE " + DB);
                SQLUtil.executeUpdateStatement(conn, "CREATE USER " + DB + " WITH PASSWORD '" + DB + "'");
                SQLUtil.executeUpdateStatement(conn, "ALTER DATABASE " + DB + " OWNER TO " + DB);
            }

            JDBC_URL = pg.getJdbcUrl(DB, DB);

            try (Connection conn = SQLUtil.getDriverMangerConnection(
                    "org.postgresql.Driver", JDBC_URL, DB, new GuardedString(DB.toCharArray()))) {

                conn.setAutoCommit(true);

                SQLUtil.executeUpdateStatement(conn, "CREATE TABLE Users (uid VARCHAR(255) PRIMARY KEY)");
            }
        } catch (Exception e) {
            fail("Could not setup PostgreSQL database", e);
        }
    }

    private static ScriptedSQLConfiguration conf() {
        ScriptedSQLConfiguration conf = new ScriptedSQLConfiguration();
        conf.setJdbcDriver("org.postgresql.Driver");
        conf.setJdbcUrlTemplate(JDBC_URL);
        conf.setUser(DB);
        conf.setPassword(new GuardedString(DB.toCharArray()));
        conf.setTestScript(TEST_SCRIPT);
        conf.setSearchScript(SEARCH_SCRIPT);
        return conf;
    }

    private static ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(ScriptedSQLConnector.class, conf());
        impl.getResultsHandlerConfiguration().setFilteredResultsHandlerInValidationMode(true);
        return factory.newInstance(impl);
    }

    @Test
    void test() {
        newFacade().test();
    }

    @Test
    void search() {
        List<ConnectorObject> result = new ArrayList<>();
        newFacade().search(ObjectClass.ACCOUNT,
                null,
                new ResultsHandler() {

            @Override
            public boolean handle(final ConnectorObject connectorObject) {
                result.add(connectorObject);
                return true;
            }
        }, new OperationOptionsBuilder().build());

        assertTrue(result.isEmpty());
    }
}
