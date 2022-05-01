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
import java.security.MessageDigest;

public abstract class MessageDigestAlgorithm extends EncodeAlgorithm {

    @Override
    public String encode(final String clearPwd, final String charsetName)
            throws PasswordEncodingException, UnsupportedPasswordCharsetException {

        if (charsetName == null) {
            throw new UnsupportedPasswordCharsetException("Invalid password charset.");
        }

        try {
            final MessageDigest msgd = MessageDigest.getInstance(getName());

            msgd.reset();
            msgd.update(clearPwd.getBytes(Charset.forName(charsetName)));

            final byte[] message = msgd.digest();

            final StringBuilder hexString = new StringBuilder();

            for (int i = 0; i < message.length; i++) {
                String hex = Integer.toHexString(0xff & message[i]);

                if (hex.length() == 1) {
                    hexString.append('0');
                }

                hexString.append(hex);
            }

            return hexString.toString();
        } catch (Exception e) {
            LOG.error(e, "Error encoding password");
            throw new PasswordEncodingException(e.getMessage());
        }
    }

    @Override
    public String decode(final String encodedPwd, final String charsetName) throws PasswordDecodingException {
        if (charsetName == null) {
            throw new PasswordDecodingException("Invalid password charset.");
        }

        return encodedPwd;
    }

    @Override
    public final void setKey(final String key) throws UnsupportedEncodingException {
    }
}
