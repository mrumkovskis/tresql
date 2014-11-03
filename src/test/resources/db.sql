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
  DEPTNO INTEGER NOT NULL)
//
CREATE TABLE DEPT
 (DEPTNO INTEGER NOT NULL,
  DNAME VARCHAR(14),
  LOC VARCHAR(13))
//
CREATE TABLE DEPT_ADDR
 (DEPTNR INTEGER NOT NULL,
  ADDR VARCHAR(50) NOT NULL,
  ZIP_CODE VARCHAR(10))
//
CREATE TABLE CAR
 (NR VARCHAR(10) NOT NULL,
  NAME VARCHAR(20) NOT NULL,
  IS_ACTIVE BOOLEAN,
  DEPTNR INTEGER,
  TYRES_NR INTEGER)
//
CREATE TABLE CAR_USAGE
 (CAR_NR VARCHAR(10) NOT NULL,
  EMPNO INTEGER NOT NULL,
  DATE_FROM DATE)
//
CREATE TABLE CAR_IMAGE
 (CARNR INTEGER NOT NULL,
  IMAGE BLOB(1K))
//
CREATE TABLE TYRES
  (NR INTEGER NOT NULL,
   CARNR VARCHAR(10) NOT NULL,
   BRAND VARCHAR(20) NOT NULL,
   SEASON VARCHAR(1) NOT NULL)
//
CREATE TABLE SALGRADE
 (GRADE INTEGER,
  LOSAL INTEGER,
  HISAL INTEGER)
//
CREATE TABLE TYRES_USAGE
  (NR INTEGER NOT NULL,
   CARNR VARCHAR(10) NOT NULL,
   DATE_FROM DATE)
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
alter table work add foreign key (empno) references emp(empno) on delete cascade
//
alter table work add foreign key (empno_mgr) references emp(empno)
//
alter table car add primary key (nr)
//
alter table car add foreign key (deptnr) references dept(deptno)
//
alter table dept_addr add primary key (deptnr)
//
alter table dept_addr add foreign key (deptnr) references dept(deptno)
//
alter table car_usage add primary key (car_nr, empno)
//
alter table car_usage add foreign key (car_nr) references car(nr)
//
alter table car_usage add foreign key (empno) references emp(empno)
//
alter table tyres add primary key (nr)
//
alter table tyres add foreign key (carnr) references car(nr)
//
alter table car add foreign key (tyres_nr) references tyres(nr)
//
alter table tyres add unique (nr, carnr)
//
alter table tyres_usage add foreign key (nr) references tyres(nr)
//
alter table tyres_usage add foreign key (nr, carnr) references tyres(nr, carnr)
//
alter table tyres_usage add foreign key (carnr) references car(nr)
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
