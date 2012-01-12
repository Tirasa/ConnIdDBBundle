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
 * http://IdentityConnectors.dev.java.net/legal/license.txt
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
package org.identityconnectors.databasetable.security;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.identityconnectors.common.Base64;

public class AES extends EncodeAlgorithm {

    private final static String NAME = "AES";

    private SecretKeySpec keySpec = null;

    @Override
    public String encode(String clearPwd)
            throws PasswordEncodingException {

        if (keySpec == null) {
            throw new PasswordEncodingException("Invalid secret key");
        }

        try {

            final byte[] cleartext = clearPwd.getBytes("UTF8");

            final Cipher cipher = Cipher.getInstance(getName());

            cipher.init(Cipher.ENCRYPT_MODE, keySpec);

            return Base64.encode(cipher.doFinal(cleartext));

        } catch (Exception e) {
            LOG.error(e, "Error encoding password");
            throw new PasswordEncodingException(e.getMessage());

        }
    }

    @Override
    public String decode(String ecodedPwd)
            throws PasswordDecodingException {

        if (keySpec == null) {
            throw new PasswordDecodingException("Invalid secret key");
        }

        try {

            byte[] encoded = Base64.decode(ecodedPwd);

            final Cipher cipher = Cipher.getInstance(getName());

            cipher.init(Cipher.DECRYPT_MODE, keySpec);

            return new String(cipher.doFinal(encoded), "UTF-8");

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
    public void setKey(final String key)
            throws UnsupportedEncodingException {

        keySpec = new SecretKeySpec(
                Arrays.copyOfRange(key.getBytes("UTF8"), 0, 16), getName());

    }
}
