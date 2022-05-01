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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AES extends EncodeAlgorithm {

    private final static String NAME = "AES";

    private SecretKeySpec keySpec = null;

    @Override
    public String encode(final String clearPwd, final String charsetName) throws PasswordEncodingException {
        if (keySpec == null) {
            throw new PasswordEncodingException("Invalid secret key.");
        }

        if (charsetName == null) {
            throw new PasswordEncodingException("Invalid password charset.");
        }

        try {
            final byte[] cleartext = clearPwd.getBytes(Charset.forName(charsetName));

            final Cipher cipher = Cipher.getInstance(getName());
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);

            return Base64.getEncoder().encodeToString(cipher.doFinal(cleartext));
        } catch (Exception e) {
            LOG.error(e, "Error encoding password");
            throw new PasswordEncodingException(e.getMessage());

        }
    }

    @Override
    public String decode(final String encodedPwd, final String charsetName) throws PasswordDecodingException {
        if (keySpec == null) {
            throw new PasswordDecodingException("Invalid secret key");
        }

        if (charsetName == null) {
            throw new PasswordDecodingException("Invalid password charset.");
        }

        try {
            byte[] encoded = Base64.getDecoder().decode(encodedPwd);

            final Cipher cipher = Cipher.getInstance(getName());
            cipher.init(Cipher.DECRYPT_MODE, keySpec);

            return new String(cipher.doFinal(encoded), Charset.forName(charsetName));
        } catch (Exception e) {
            LOG.error(e, "Error decoding password");
            throw new PasswordDecodingException(e.getMessage());
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void setKey(final String key) throws UnsupportedEncodingException {
        keySpec = new SecretKeySpec(Arrays.copyOfRange(key.getBytes(StandardCharsets.UTF_8), 0, 16), getName());
    }
}
