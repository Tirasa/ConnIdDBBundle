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

    final static String UTF8_CHARSETNAME = "UTF-8";

    final static String LATIN1_CHARSETNAME = "ISO-8859-1";

    @Test
    public void aes()
            throws Exception {

        final String clazz = AES.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("AES", algorithm.getName());

        algorithm.setKey("1abcdefghilmnopqrstuvz2!");

        String encoded = algorithm.encode("password", UTF8_CHARSETNAME);
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));

        String decoded = algorithm.decode(encoded, UTF8_CHARSETNAME);
        assertNotNull(decoded);
        assertEquals("password", decoded);
    }

    @Test
    public void md5()
            throws Exception {

        final String clazz = MD5.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("MD5", algorithm.getName());

        String encoded = algorithm.encode("password", UTF8_CHARSETNAME);
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
        assertEquals("MD5", algorithm.getName());
        assertEquals(encoded, "5f4dcc3b5aa765d61d8327deb882cf99");

        encoded = algorithm.encode("password", LATIN1_CHARSETNAME);
        assertEquals(encoded, "5f4dcc3b5aa765d61d8327deb882cf99");

        encoded = algorithm.encode("passwordè", UTF8_CHARSETNAME);
        assertEquals(encoded, "1ea7e38c6126a0765dc619a1e2aa99fc");

        encoded = algorithm.encode("passwordè", LATIN1_CHARSETNAME);
        assertEquals(encoded, "079fea8ebf50752a85c2cd90a32270db");
    }

    @Test
    public void sha_1()
            throws Exception {

        final String clazz = SHA_1.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("SHA-1", algorithm.getName());

        String encoded = algorithm.encode("password", UTF8_CHARSETNAME);
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
        assertEquals(encoded, "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8");

        encoded = algorithm.encode("password", LATIN1_CHARSETNAME);
        assertEquals(encoded, "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8");

        encoded = algorithm.encode("passwordè", UTF8_CHARSETNAME);
        assertEquals(encoded, "a5c28b16a7d49e6aadd711b7b61c0568f1e2b6fe");

        encoded = algorithm.encode("passwordè", LATIN1_CHARSETNAME);
        assertEquals(encoded, "d2231ee6665bed00c1677fdcd0605d12ea48f1a2");
    }

    @Test
    public void sha_256()
            throws Exception {

        final String clazz = SHA_256.class.getName();

        final EncodeAlgorithm algorithm = (EncodeAlgorithm) Class.forName(clazz).newInstance();

        assertEquals("SHA-256", algorithm.getName());

        String encoded = algorithm.encode("password", UTF8_CHARSETNAME);
        assertNotNull(encoded);
        assertFalse("password".equalsIgnoreCase(encoded));
    }
}
