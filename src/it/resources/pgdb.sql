      -- POSTGRESQL DB CREATE SCRIPT --
      -- PART1 BASED ON POSTGRESQL 10 REGRESSION TEST SUITE --
// 
DROP TABLE IF EXISTS TIMESTAMP_TBL;
//
CREATE TABLE TIMESTAMP_TBL (f1 timestamp);
//
SET TIMEZONE TO 'Europe/Riga';
//
DROP TABLE IF EXISTS HOBBIES_R, EQUIPMENT_R, ONEK, BMSCANTEST;
//
CREATE TABLE BMSCANTEST 
 (a int, 
  b int, 
  t text);
--- TODO fixme move to pgtest.txt
//
INSERT INTO bmscantest
  SELECT (r%53), (r%59), 'foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo'
  FROM generate_series(1,70000) r;
//
CREATE INDEX i_bmtest_a ON BMSCANTEST(a);
//
CREATE INDEX i_bmtest_b ON BMSCANTEST(b);
//
set work_mem = 64;
//
CREATE TABLE HOBBIES_R
 (name text,
  person text);
//
CREATE TABLE EQUIPMENT_R
 (name text,
  hobby text);
//
CREATE TABLE ONEK
 (unique1     int4,
  unique2     int4,
  two         int4,
  four        int4,
  ten         int4,
  twenty      int4,
  hundred     int4,
  thousand    int4,
  twothousand int4,
  fivethous   int4,
  tenthous    int4,
  odd         int4,
  even        int4,
  stringu1    name, 
  stringu2    name,
  string4     name
);
     -- PART2 BASED ON HSQL TEST SUITE --
DROP TABLE IF EXISTS DUMMY, SALGRADE, TYRES_USAGE, TYRES, CAR_IMAGE, CAR_USAGE, CAR, DEPT_EQUIPMENT, ADDRESS, DEPT_SUB_ADDR, DEPT_ADDR, EMP, DEPT, WORK, RESULTS;
//
CREATE TABLE WORK
 (WDATE DATE NOT NULL,
  EMPNO INTEGER NOT NULL,
  HOURS INTEGER NOT NULL,
  EMPNO_MGR INTEGER);
//
COMMENT ON TABLE work IS 'work';
//
COMMENT ON COLUMN work.wdate IS 'work date';
//
COMMENT ON DATABASE postgres IS 'Development Test Database';
//
CREATE TABLE EMP
 (EMPNO INTEGER NOT NULL,
  ENAME VARCHAR(50),
  JOB VARCHAR(9),
  MGR INTEGER,
  HIREDATE DATE,
  SAL DECIMAL(7, 2),
  COMM DECIMAL(7, 2),
  DEPTNO INTEGER NOT NULL);
//
CREATE TABLE DEPT
 (DEPTNO INTEGER NOT NULL,
  DNAME VARCHAR(14),
  LOC VARCHAR(13));
//
CREATE TABLE IF NOT EXISTS DEPT_ADDR
 (DEPTNR INTEGER NOT NULL,
  ADDR VARCHAR(50) NOT NULL,
  ZIP_CODE VARCHAR(10),
 ADDR_NR INTEGER);
//
CREATE TABLE IF NOT EXISTS DEPT_SUB_ADDR
  (DEPTNO INTEGER NOT NULL,
   ADDR VARCHAR(50) NOT NULL);
//
CREATE TABLE IF NOT EXISTS ADDRESS
 (NR INTEGER NOT NULL,
  ADDR VARCHAR(50) NOT NULL);
//
CREATE TABLE IF NOT EXISTS DEPT_EQUIPMENT
  (DEPT_NAME VARCHAR(14) NOT NULL,
   EQUIPMENT VARCHAR(50) NOT NULL);
//
CREATE TABLE IF NOT EXISTS CAR
 (NR VARCHAR(20) NOT NULL,
 NAME VARCHAR(20) NOT NULL,
 IS_ACTIVE BOOLEAN,
 DEPTNR INTEGER,
 TYRES_NR INTEGER);
//
CREATE TABLE IF NOT EXISTS CAR_USAGE
 (CAR_NR VARCHAR(10) NOT NULL,
  EMPNO INTEGER NOT NULL,
  DATE_FROM DATE);
//
CREATE TABLE IF NOT EXISTS CAR_IMAGE
 (CARNR INTEGER NOT NULL,
  IMAGE bytea);
//
CREATE TABLE IF NOT EXISTS TYRES
  (NR INTEGER NOT NULL,
   CARNR VARCHAR(10) NOT NULL,
   BRAND VARCHAR(20) NOT NULL,
   SEASON VARCHAR(1) NOT NULL);
//
CREATE TABLE IF NOT EXISTS TYRES_USAGE
  (TUNR INTEGER NOT NULL,
   CARNR VARCHAR(10) NOT NULL,
   DATE_FROM DATE);
//
CREATE TABLE IF NOT EXISTS SALGRADE
 (GRADE INTEGER,
  LOSAL INTEGER,
  HISAL INTEGER);
//
CREATE TABLE IF NOT EXISTS DUMMY (DUMMY INTEGER);
//
CREATE TABLE IF NOT EXISTS RESULTS (ID INTEGER PRIMARY KEY, SCORES DECIMAL[], NAMES VARCHAR[])
//
alter table dept add primary key (deptno);
//
alter table dept add unique (dname);
//
alter table emp add primary key (empno);
//
alter table emp add foreign key (deptno) references dept(deptno);
//
alter table emp add foreign key (mgr) references emp(empno);
//
alter table salgrade add primary key (grade);
//
alter table work add foreign key (empno) references emp(empno) on delete cascade;
//
alter table work add foreign key (empno_mgr) references emp(empno);
//
alter table car add primary key (nr);
//
alter table car add foreign key (deptnr) references dept(deptno);
//
alter table dept_addr add primary key (deptnr);
//
alter table dept_addr add foreign key (deptnr) references dept(deptno);
//
alter table dept_sub_addr add foreign key (deptno) references dept_addr(deptnr)
//
alter table address add primary key (nr);
//
alter table dept_addr add foreign key (addr_nr) references address(nr)
//
alter table dept_equipment add foreign key (dept_name) references dept(dname)
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
alter table tyres add unique (nr, carnr);
//
alter table tyres_usage add foreign key (tunr) references tyres(nr) on delete cascade
//
alter table tyres_usage add foreign key (tunr, carnr) references tyres(nr, carnr)
//
alter table tyres_usage add foreign key (carnr) references car(nr)
//
CREATE OR REPLACE FUNCTION inc_val_5 (x INTEGER) RETURNS INTEGER AS ' 
BEGIN
    RETURN x + 5;
END;
' LANGUAGE plpgsql;
//
CREATE OR REPLACE FUNCTION in_out(INOUT newid INTEGER, OUT outstring VARCHAR(100), IN instring VARCHAR(100)) AS '
BEGIN
    newid := newid + 5;
    outstring := instring;
END;
' LANGUAGE plpgsql;
//
DROP SEQUENCE IF EXISTS seq;
//
CREATE SEQUENCE seq START WITH 10000;
