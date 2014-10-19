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
  accountId   VARCHAR(50) NOT NULL,
  password    VARCHAR(100),
  manager     VARCHAR(50),
  middlename  VARCHAR(50),
  firstname   VARCHAR(50) NOT NULL,
  lastname    VARCHAR(50) NOT NULL,
  email       VARCHAR(250),
  department  VARCHAR(250),
  title       VARCHAR(250),
  age         INTEGER,
  accessed    BIGINT,
  salary      DECIMAL(9,2),
  jpegphoto   BLOB,
  activate    DATE,  
  opentime    TIME,      
  changed     TIMESTAMP NOT NULL,
  changelog   BIGINT,
  status      VARCHAR(10)
)
