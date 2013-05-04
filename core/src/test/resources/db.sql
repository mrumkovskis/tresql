      -- HSQL DB CREATE SCRIPT --

CREATE TABLE WORK
 (WDATE DATE NOT NULL,
  EMPNO INTEGER NOT NULL,
  HOURS INTEGER NOT NULL,
  EMPNO_MGR INTEGER)
//
COMMENT ON TABLE work IS 'work'
COMMENT ON COLUMN work.wdate IS 'work date'
      
CREATE TABLE EMP
 (EMPNO INTEGER NOT NULL,
  ENAME VARCHAR(50),
  JOB VARCHAR(9),
  MGR INTEGER,
  HIREDATE DATE,
  SAL DECIMAL(7, 2),
  COMM DECIMAL(7, 2),
  DEPTNO INTEGER)
//
CREATE TABLE DEPT
 (DEPTNO INTEGER NOT NULL,
  DNAME VARCHAR(14),
  LOC VARCHAR(13))
//
CREATE TABLE CAR
 (NR VARCHAR(10) NOT NULL,
  NAME VARCHAR(20) NOT NULL,
  IS_ACTIVE BOOLEAN,
  DEPTNR INTEGER)
//
CREATE TABLE CAR_IMAGE
 (CARNR INTEGER NOT NULL,
  IMAGE BLOB(1K))
//
CREATE TABLE SALGRADE
 (GRADE INTEGER,
  LOSAL INTEGER,
  HISAL INTEGER)
//
CREATE TABLE DUMMY (DUMMY INTEGER)
//
alter table dept add primary key (deptno)
//
alter table emp add primary key (empno)
//
alter table emp add foreign key (deptno) references dept(deptno)
//
alter table emp add foreign key (mgr) references emp(empno)
//
alter table salgrade add primary key (grade)
//
alter table work add foreign key (empno) references emp(empno)
//
alter table work add foreign key (empno_mgr) references emp(empno)
//
alter table car add primary key (nr)
//
alter table car add foreign key (deptnr) references dept(deptno)
//
CREATE FUNCTION inc_val_5 (x INTEGER)
  RETURNS INTEGER
  RETURN x + 5
//  
CREATE PROCEDURE in_out(INOUT newid INTEGER, OUT outstring VARCHAR(100), IN instring VARCHAR(100))
BEGIN ATOMIC
  SET newid = newid + 5;
  SET outstring = instring;
END

CREATE SEQUENCE seq START WITH 10000
