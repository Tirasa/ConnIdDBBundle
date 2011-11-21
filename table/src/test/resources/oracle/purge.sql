BEGIN EXECUTE immediate 'drop table Accounts'; EXCEPTION WHEN others THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
