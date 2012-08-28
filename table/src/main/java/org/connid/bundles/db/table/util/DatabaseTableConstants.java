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
package org.connid.bundles.db.table.util;

/**
 * The database table constants 
 * @author Petr Jung
 * @since 1.0
 */
public class DatabaseTableConstants {
    /**  The default value for the RA_DRIVER resource attribute. */
    public static final String DEFAULT_DRIVER = "oracle.jdbc.driver.OracleDriver";
    /**  The default connect URL template.  */
    public static final String DEFAULT_TEMPLATE = "jdbc:oracle:thin:@%h:%p:%d";    
    /** The null column default value */
    public static final String EMPTY_STR = "";    

    public static final String MSG_ACCOUNT_OBJECT_CLASS_REQUIRED = "acount.object.class.required";    
    public static final String MSG_AUTH_FAILED="auth.op.failed";
    public static final String MSG_AUTHENTICATE_OP_NOT_SUPPORTED = "auth.op.not.supported";
    public static final String MSG_CAN_NOT_CREATE="can.not.create";    
    public static final String MSG_CAN_NOT_DELETE="can.not.delete";
    public static final String MSG_CAN_NOT_READ="can.not.read";
    public static final String MSG_CAN_NOT_UPDATE="can.not.update";
    public static final String MSG_DATABASE_BLANK = "database.blank";
    public static final String MSG_HOST_BLANK = "host.blank";
    public static final String MSG_CHANGELOG_COLUMN_BLANK="changelog.column.blank";
    public static final String MSG_INVALID_ATTRIBUTE_SET="invalid.attribute.set";
    public static final String MSG_INVALID_QUOTING = "invalid.quoting";
    public static final String MSG_INVALID_SYNC_TOKEN_VALUE="invalid.sync.token.value";
    public static final String MSG_JDBC_DRIVER_BLANK = "jdbc.driver.blank";
    public static final String MSG_JDBC_DRIVER_NOT_FOUND  = "jdbc.driver.not.found";
    public static final String MSG_JDBC_TEMPLATE_BLANK = "jdbc.template.blank";
    public static final String MSG_KEY_COLUMN_BLANK="key.column.blank";
    public static final String MSG_KEY_COLUMN_EQ_CHANGE_LOG_COLUMN="key.column.eq.change.log.column";
    public static final String MSG_MORE_USERS_DELETED = "more.users.deleted";
    public static final String MSG_NAME_BLANK = "name.blank";
    public static final String MSG_PASSWD_COLUMN_EQ_CHANGE_LOG_COLUMN="passwd.column.eq.change.log.column";
    public static final String MSG_PASSWD_COLUMN_EQ_KEY_COLUMN="passwd.column.eq.key.column";
    public static final String MSG_PASSWORD_BLANK = "admin.password.blank";
    public static final String MSG_PORT_BLANK = "port.blank";
    public static final String MSG_PWD_BLANK = "pwd.blank";
    public static final String MSG_PWD_COLUMN_BLANK="pwd.column.blank";   
    public static final String MSG_QUERY_INVALID = "query.invalid"; 
    public static final String MSG_RESULT_HANDLER_NULL="result.handler.null";
    public static final String MSG_TABLE_BLANK = "table.blank";
    public static final String MSG_UID_BLANK="uid.blank";
    public static final String MSG_USER_BLANK = "admin.user.blank";
    
    private DatabaseTableConstants() {
        throw new AssertionError();
    }
    
}
