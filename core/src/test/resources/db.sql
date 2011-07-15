      -- HSQL DB CREATE SCRIPT --
      
CREATE TABLE EMP
 (EMPNO INTEGER NOT NULL,
  ENAME VARCHAR(10),
  JOB VARCHAR(9),
  MGR INTEGER,
  HIREDATE DATE,
  SAL DECIMAL(7, 2),
  COMM DECIMAL(7, 2),
  DEPTNO INTEGER);
 
CREATE TABLE DEPT
 (DEPTNO INTEGER NOT NULL,
  DNAME VARCHAR(14),
  LOC VARCHAR(13));

CREATE TABLE SALGRADE
 (GRADE INTEGER,
  LOSAL INTEGER,
  HISAL INTEGER);

CREATE TABLE DUMMY (DUMMY INTEGER);

alter table dept add primary key (deptno);
alter table emp add primary key (empno);
alter table emp add foreign key (deptno) references dept(deptno);
alter table emp add foreign key (mgr) references emp(empno);
alter table salgrade add primary key (grade)