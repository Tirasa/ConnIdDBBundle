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
package net.tirasa.connid.bundles.db.table.security;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class ClearText extends EncodeAlgorithm {

    private final static String NAME = "CLEARTEXT";

    @Override
    public String encode(final String clearPwd, final String charsetName)
            throws PasswordEncodingException, UnsupportedPasswordCharsetException {

        try {
            Charset charset = Charset.forName(charsetName);
            return new String(clearPwd.getBytes(charset), charset);
        } catch (Exception e) {
            throw new UnsupportedPasswordCharsetException("Unsupported password charset.", e);
        }
    }

    @Override
    public String decode(final String encodedPwd, final String charsetName) throws PasswordDecodingException {
        try {
            Charset charset = Charset.forName(charsetName);
            return new String(encodedPwd.getBytes(charset), charset);
        } catch (Exception e) {
            throw new PasswordDecodingException("Unsupported password charset.");
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void setKey(final String key) throws UnsupportedEncodingException {
    }
}
