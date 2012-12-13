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
package org.connid.bundles.db.table.security;

import java.io.UnsupportedEncodingException;

public class ClearText extends EncodeAlgorithm {

    private final static String NAME = "CLEARTEXT";

    @Override
    public String encode(String clearPwd, String charsetName)
            throws PasswordEncodingException, UnsupportedPasswordCharsetException {

        try {
            return new String(clearPwd.getBytes(charsetName), charsetName);
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedPasswordCharsetException("Unsupported password charset.", e);
        }
    }

    @Override
    public String decode(String encodedPwd, String charsetName)
            throws PasswordDecodingException {

        try {
            return new String(encodedPwd.getBytes(charsetName), charsetName);
        } catch (UnsupportedEncodingException e) {
            throw new PasswordDecodingException("Unsupported password charset.");
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void setKey(String key) throws UnsupportedEncodingException {
    }
}
