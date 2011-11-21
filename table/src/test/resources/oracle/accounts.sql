create table Accounts (
  accountId   VARCHAR2(50) NOT NULL,
  password    VARCHAR2(50),
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
  changelog   NUMBER
)