/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2012 Tirasa. All rights reserved.
 * 
 * The contents of this file are subject to the terms of the Common Development 
 * and Distribution License("CDDL") (the "License").  You may not use this file 
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at 
 * https://connid.googlecode.com/svn/base/trunk/legal/license.txt
 * See the License for the specific language governing permissions and limitations 
 * under the License. 
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields 
 * enclosed by brackets [] replaced by your own identifying information: 
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.bundles.db.table;

import static org.junit.Assert.*;

import org.connid.bundles.db.table.security.AES;
import org.connid.bundles.db.table.security.EncodeAlgorithm;
import org.connid.bundles.db.table.security.MD5;
import org.connid.bundles.db.table.security.SHA_1;
import org.connid.bundles.db.table.security.SHA_256;
import org.junit.Test;

public class DatabaseTableSecurityTests {

    @Test
    public void aes()
            throws Exception {

        final String clazz = AES.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("AES", algorithm.getName());

        algorithm.setKey("1abcdefghilmnopqrstuvz2!");

        String encoded = algorithm.encode("password");
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));

        String decoded = algorithm.decode(encoded);
        assertNotNull(decoded);
        assertEquals("password", decoded);
    }

    @Test
    public void md5()
            throws Exception {

        final String clazz = MD5.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("MD5", algorithm.getName());

        String encoded = algorithm.encode("password");
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
    }

    @Test
    public void sha_1()
            throws Exception {

        final String clazz = SHA_1.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("SHA-1", algorithm.getName());

        String encoded = algorithm.encode("password");
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
    }

    @Test
    public void sha_256()
            throws Exception {

        final String clazz = SHA_256.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("SHA-256", algorithm.getName());

        String encoded = algorithm.encode("password");
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
    }
}
