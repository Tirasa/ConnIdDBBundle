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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.tirasa.connid.bundles.db.table.mapping.DefaultStrategy;
import net.tirasa.connid.bundles.db.table.mapping.MappingStrategy;
import net.tirasa.connid.bundles.db.table.util.DatabaseTableSQLUtil;
import net.tirasa.connid.commons.db.ExpectProxy;
import net.tirasa.connid.commons.db.SQLParam;
import org.identityconnectors.common.Pair;
import org.identityconnectors.common.security.GuardedString;
import org.junit.jupiter.api.Test;

/**
 * The SQL util tests
 *
 * @version $Revision 1.0$
 * @since 1.0
 */
public class DatabaseTableSQLUtilTests {

    /**
     * GetAttributeSet test method
     *
     * @throws SQLException
     */
    @Test
    public void testGetColumnValues() throws SQLException {
        final String TEST1 = "test1";

        final String TEST_VAL1 = "testValue1";

        final String TEST2 = "test2";

        final String TEST_VAL2 = "testValue2";

        //Resultset
        final ExpectProxy<ResultSet> trs = new ExpectProxy<>();

        ResultSet resultSetProxy = trs.getProxy(ResultSet.class);

        //Metadata
        final ExpectProxy<ResultSetMetaData> trsmd = new ExpectProxy<>();

        ResultSetMetaData metaDataProxy = trsmd.getProxy(ResultSetMetaData.class);

        trs.expectAndReturn("getMetaData", metaDataProxy);

        trsmd.expectAndReturn("getColumnCount", 2);

        trsmd.expectAndReturn("getColumnName", TEST1);

        trsmd.expectAndReturn("getColumnType", Types.VARCHAR);

        trs.expectAndReturn("getString", TEST_VAL1);

        trsmd.expectAndReturn("getColumnName", TEST2);

        trsmd.expectAndReturn("getColumnType", Types.VARCHAR);

        trs.expectAndReturn("getString", TEST_VAL2);

        final DefaultStrategy derbyDbStrategy = new DefaultStrategy();

        final Map<String, SQLParam> actual = DatabaseTableSQLUtil.getColumnValues(derbyDbStrategy, resultSetProxy);
        assertTrue(trs.isDone());
        assertTrue(trsmd.isDone());
        assertEquals(2, actual.size());

        final SQLParam tv1 = actual.get(TEST1);
        assertNotNull(tv1);
        assertEquals(TEST_VAL1, tv1.getValue());

        final SQLParam tv2 = actual.get(TEST2);
        assertNotNull(tv2);
        assertEquals(TEST_VAL2, tv2.getValue());
    }

    /**
     * Test quoting method
     *
     * @throws Exception
     */
    @Test
    public void testQuoting() throws Exception {
        final Map<String, Pair<String, String>> data = new HashMap<>();
        data.put("none", new Pair<>("afklj", "afklj"));
        data.put("double", new Pair<>("123jd", "\"123jd\""));
        data.put("single", new Pair<>("di3nfd", "'di3nfd'"));
        data.put("back", new Pair<>("fadfk3", "`fadfk3`"));
        data.put("brackets", new Pair<>("fadlkfj", "[fadlkfj]"));

        for (Map.Entry<String, Pair<String, String>> entry : data.entrySet()) {
            final String actual = DatabaseTableSQLUtil.quoteName(entry.getKey(), entry.getValue().first);
            assertEquals(entry.getValue().second, actual);
        }
    }

    /**
     * Test quoting method
     *
     * @throws Exception
     */
    @Test
    public void testSetParams() throws Exception {
        final ExpectProxy<MappingStrategy> mse = new ExpectProxy<>();
        MappingStrategy ms = mse.getProxy(MappingStrategy.class);

        final ExpectProxy<PreparedStatement> pse = new ExpectProxy<>();
        PreparedStatement ps = pse.getProxy(PreparedStatement.class);

        final ExpectProxy<CallableStatement> cse = new ExpectProxy<>();
        CallableStatement cs = cse.getProxy(CallableStatement.class);

        List<SQLParam> params = new ArrayList<>();
        params.add(new SQLParam("test", "test", Types.VARCHAR));
        params.add(new SQLParam("password", new GuardedString("tst".toCharArray()), Types.VARCHAR));

        mse.expect("setSQLParam");
        mse.expect("setSQLParam");

        DatabaseTableSQLUtil.setParams(ms, ps, params);

        assertTrue(mse.isDone());
        assertTrue(pse.isDone());

        mse.expect("setSQLParam");
        mse.expect("setSQLParam");

        DatabaseTableSQLUtil.setParams(ms, cs, params);

        assertTrue(mse.isDone());
        assertTrue(cse.isDone());
    }
}
