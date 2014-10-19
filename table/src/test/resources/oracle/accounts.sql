--
-- ====================
-- DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
-- 
-- Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.
-- 
-- The contents of this file are subject to the terms of the Common Development
-- and Distribution License("CDDL") (the "License").  You may not use this file
-- except in compliance with the License.
-- 
-- You can obtain a copy of the License at
-- http://opensource.org/licenses/cddl1.php
-- See the License for the specific language governing permissions and limitations
-- under the License.
-- 
-- When distributing the Covered Code, include this CDDL Header Notice in each file
-- and include the License file at http://opensource.org/licenses/cddl1.php.
-- If applicable, add the following below this CDDL Header, with the fields
-- enclosed by brackets [] replaced by your own identifying information:
-- "Portions Copyrighted [year] [name of copyright owner]"
-- ====================
-- Portions Copyrighted 2011 ConnId.
--

create table Accounts (
  accountId   VARCHAR2(50) NOT NULL,
  password    VARCHAR2(100),
  manager     VARCHAR2(50),
  middlename  VARCHAR2(50),
  firstname   VARCHAR2(50) NOT NULL,
  lastname    VARCHAR2(50) NOT NULL,
  email       VARCHAR2(250),
  department  VARCHAR2(250),
  title       VARCHAR2(250),
  age         INTEGER,
  accessed    NUMBER,
  salary      DECIMAL(9,2),
  jpegphoto   BLOB,
  activate    DATE,
  opentime    DATE,      
  changed     TIMESTAMP NOT NULL,
  changelog   NUMBER,
  status      VARCHAR(10)
)
