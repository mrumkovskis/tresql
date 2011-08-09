ONQL Language Guide
===================

Contents
--------
* [Syntax quickchart](#syntax-quickchart)  
* [Data selection](#data-selection)  
* [Data manipulation](#data-manipulation)  
* [Appendix Data](#appendix-data)  

ONQL provides syntax for both data manipulation (INSERT, UPDATE) and querying (SELECT) as well as more rarely used SQL clauses (GROUP BY, INTERSECT etc.). Once the database structure is defined, it can fulfill all the SQL needs for your application.  

The sample data used by the following examples is described in [Appendix Data](#appendix-data).  
 
<a name="wiki-syntax-quickchart"/> Syntax quickchart 
------------------
```
{}      Selection set (columns or sub-selects). Similar to JSON notation for objects properties.
[]      WHERE conditions
/       Follow the foreign key to parent table: similar to Xpath
:foo    Named binding variable foo.
:m(N)   Special binding variable, binds to Nth column of m-th parent select. 
?       Unnamed binding variable
+       INSERT
=       UPDATE 
-       DELETE
&       AND
|       OR, also nested query
++      UNION
&&      INTERSECT
,       Delimiter for columns and statements
#       ORDER BY (ascending), DISTINCT
~#      ORDER BY DESCENDING
~       LIKE
~~      LIKE (case-insensitive)     
```

<a name="wiki-data-selection"/>Data selection 
--------------

[Simple SELECTs ](#simple-selects)  
[Joins (inner and outer) ](#joins-inner-and-outer)  
[Nesting objects](#nesting-objects)  
[Shortcut for joins](#shortcut-for-joins)  
[ORDER, DISTINCT, GROUP BY, HAVING](#order)  
[Nesting selects with IN and EXISTS](#nesting-selects-with-in)  
[Combining boolean conditions (AND, OR, NOT) in WHERE clauses](#boolean)  
[UNION, INTERSECT, product](#union)  
[Binding variables](#binding-variables)  
[Combining several queries in a batch](#combining-queries)  
[LIKE comparison](#like-comparison)  
[Array binding](#array-binding)
[Check your knowledge - examples](#check-your-knowledge)  

### <a name="wiki-simple-selects"/> Simple SELECTs 

Examples:

Select all records from table EMP with all columns:  
`EMP`

Select by primary key:  
`EMP [ EMPNO = 7369]`

This is the same as simply:  
`EMP [7369]`

Select some columns with WHERE condition:  
`EMP [ EMPNO = 7369] { ENAME, JOB }`

### <a name="wiki-joins-inner-and-outer"/>Joins (inner and outer) 

Join EMP and DEPT tables. This is an inner join:
```
EMP                                  
  [EMP.DEPTNO = DEPT.DEPTNO] DEPT    
  [EMP.EMPNO = 7369]                 
  { ENAME, JOB, DNAME }
```

You can just as well start join with the other table, to produce the same results:
```
DEPT
  [EMP.DEPTNO = DEPT.DEPTNO] EMP    
  [EMP.EMPNO = 7369]                 
  { ENAME, JOB, DNAME }
```


Join and provide table aliases (EMP E, DEPT D). Here we are selecting employees from "RESEARCH" department:
```
EMP E                                 
  [E.DEPTNO = D.DEPTNO] DEPT ? D
  [D.DNAME = "RESEARCH"]                 
  { E.ENAME, E.JOB, D.DNAME }
```

Add outer join with EMP table to select manager. Outer join is denoted by question mark "?" after table name being joined:
```
EMP E                                 
  [E.DEPTNO = D.DEPTNO] DEPT D
  [E.MGR = E2.EMPNO] EMP ? E2
  { E.ENAME, E.JOB, D.DNAME, E2.ENAME MANAGER }
```

### <a name="wiki-nesting-objects"/> Nesting objects
Let us proceed at once to the more complex topic of including nested objects in your results, since this was one of the main reasons to use ONQL (for the more basic ONQL usage, proceed to next section).

We have already seen this query in [Quickstart](#quickstart) section:  
```
dept {
  deptno, 
  dname, 
  |emp [deptno = :1(1)] {empno, ename} emp
}
```
The pipe "|" syntax specifies a query for nested object. This enclosed query follows the same ONQL syntax as it's parent query. It has to somehow reference parent query, and this is specified by special binding variables like :1(1).   
Syntax :m(N) specifies to use as a binding value column number N (starting from 1) of parent (enclosing) select number m. Think of select numbering as this: parent (enclosing) select query is number 1, it's parent is number 2, etc.  
The following 3-level query returns departments, their employees (1st level nesting) and their managers, if the manager belongs to the same department. So for managers subselect, departments query is a 2nd level parent select:   
```
dept {
  deptno, 
  dname, 
  |emp [deptno = :1(1)] {
     empno, ename, mgr,
     |emp mgr [mgr.empno = :1(1) & mgr.deptno = :2(1)] { ename manager } mgr_object
  } emp_object
}
```

Here is another example to select employees in a specific salary grade:
```
salgrade
  {grade, hisal, losal, 
     |emp [sal >= :1(2) & sal <= :1(3)] {ename, sal} emp_object
  }
```

### <a name="wiki-shortcut-for-joins"/> Shortcut for joins

ONQL provides slash "/" as a shortcut for joins. This is an Xpath-like syntax. It infers the relationship between the two tables if there is just 1 foreign key joining them. The following query joins EMP and DEPT tables:  
`emp e/dept d {e.ename, d.dname}`

It is same as the following (table order does not matter):   
`dept d/emp e {e.ename, d.dname}`

### <a name="wiki-order"/>ORDER, DISTINCT, GROUP BY, HAVING
#### ORDER BY
Order employees by ename:  
`emp { ename, sal } #(ename)`

 `#(ename)` sorts employee records in ASCENDING order by ename.   
` ~#(ename)` sorts in DESCENDING order.

ORDER BY descending by department name and employee name:  
`emp e/dept d { d.dname, e.ename, e.sal } ~#(d.dname)~#(e.ename)`

#### DISTINCT 
DISTINCT is denoted by "#" sign before column list. The following query selects distinct departments from EMP table:   
`emp #{ deptno }`

You can combine DISTINCT with ORDER BY:  
`emp e/dept d #{ d.deptno, d.dname } #(d.dname)`

#### GROUP BY:
```
emp e 
  [e.sal >= s.hisal & e.sal <= s.losal] salgrade s 
  {grade, hisal, losal, count(empno)} 
  (grade, hisal, losal) 
  #(grade)
```

This query groups employees by salary grades. The GROUP BY columns are specified in round brackets: (grade, hisal, losal). This produces following SELECT statement:
```sql
select grade,hisal,losal,count(empno) from emp e join salgrade s 
on e.sal >= s.hisal and e.sal <= s.losal group by grade,hisal,losal order by grade asc
```

#### GROUP BY with HAVING
Select employees only in those salary grades where employee count >1. HAVING is specified by ^ operator:
```
emp 
  [sal >= hisal & sal <= losal] salgrade 
  {grade, hisal, losal, count(*)} 
  (grade, hisal, losal)^(count(*) > 1)#(grade)
```
Here the clause 
    (grade, hisal, losal)^(count(*) > 1) 
translates into SQL: 
```sql
group by grade, hisal, losal having count(*) > 1
```

### <a name="wiki-nesting-selects-with-in"/> Nesting selects with IN and EXISTS
You can specify a nested IN query in ONQL, just as you would specify a nested IN query in SQL WHERE clause:
```
dept [
      deptno in (
        emp [sal >= 5000]  {deptno}
      )
     ] {dname}
```

You can specify a nested EXISTS query using parenthesis () in WHERE condition. The following query selects departments where employees exist:
```
dept d [
  (emp e[e.deptno = d.deptno])
  ] 
```

### <a name="wiki-boolean"/>Combining boolean conditions (AND, OR, NOT) in WHERE clauses
ONQL uses the syntax:
<pre>
& for AND
| for OR
! for NOT
</pre>
You can also use parentheses, it's rather straightforward. Try the following examples (to remember nested object notation :1(1), jump back to [Nesting objects](#Nesting_objects) section):

Select employees in salary grades:
```
salgrade
  {grade, hisal, losal, 
     |emp [sal >= :1(2) & sal <= :1(3) ] {ename, sal}
  }
```

Select employees outside of salary grades:
```
salgrade
  {grade, hisal, losal, 
     |emp [sal <= :1(2) | sal >= :1(3)] {ename, sal}
  }
```

Another turn-around way to select employees in salary grades:
```
salgrade
  {grade, hisal, losal, 
     |emp [!(sal <= :1(2) | sal >= :1(3))] {ename, sal}
  }
```

### <a name="wiki-union"/>UNION, INTERSECT, product
UNION is specified by "++" :
```
   emp [deptno = 10] { deptno, ename} 
++ emp [deptno = 20] { deptno, ename} 
++ emp [deptno = 30] { deptno, ename} 
```

INTERSECT is specified by "&&" :  
`emp[sal >= 1000] {empno, ename} && emp[sal <= 3000] {empno, ename}`

There is special notation for product of two tables (join  without condition). You have to explicitly specify empty join condition as []. The reason for this is to produce an error if join condition was omitted by mistake.
```
dept [] salgrade
{dname, grade}
```

### <a name="wiki-binding-variables"/>Binding variables

There are named variables, prefixed by colon ":" :<variable_name> and unnamed variable placeholders, specified by question mark "?". The following are two variations of the same query:
```
emp [empno = :empno] {ename}
emp [empno = ?] {ename}
```

### <a name="wiki-combining-queries"/>Combining several queries in a batch
Like data manipulation statements (see [Data manipulation](#data-manipulation) chapter), you can combine several queries in a batch if you separate them with commas ",". Query results are combined together into one JSON array.
```
emp [ job = "ANALYST"] {ename},
dept { dname }
```

### <a name="wiki-like-comparison"/>LIKE comparison
In WHERE conditions, tilde "~" stands for LIKE comparison, and "~~" for case-insensitive LIKE comparison:
```
emp [ job ~ "%LYST"] {ename, job}
emp [ job ~~ "%lyST"] {ename, job}
```

### <a name="wiki-array-binding"/>Array binding
Array binding is a special shortcut of ONQL that allows to bind several binding variables as one array parameter. What distinguishes it is that you specify only one placeholder, and supply only one binding value, which is an array:
```
emp [empno in (?)] 
  {empno, ename, job}
```
For this query, an array of employee ids should be supplied, such as `[7839, 7782]`.
This feature is currently available from API only (not from web service).[TODO]

### <a name="wiki-check-your-knowledge"/>Check your knowledge - examples

The following 3-level query selects departments, salary grades encountered in a given department with employee count, and employees falling into each salary grade:
```
dept {
  deptno, dname, 
  |emp 
    [sal >= hisal & sal <= losal] salgrade
    [deptno = :1(1)] {
      grade, hisal, losal, count(empno), 
      |emp/dept [sal >= :1(2) & sal <= :1(3) & dept.deptno = :2(1)] {
         ename, dept.deptno, dname, sal
      } #(empno)
    } 
  (grade, hisal, losal) #(1)
} #(dept.deptno)
```

<a name="wiki-data-manipulation"/>Data manipulation 
-----------------

[INSERT statement](#insert-statement)  
[UPDATE statement](#update-statement)  
[DELETE statement](#delete-statement)  
[Combining several statements in a batch](#combining-dml)  

### <a name="wiki-insert-statement"/>INSERT statement

Example:  
`DEPT {DEPTNO, DNAME, LOC} + [10, "ACCOUNTING", "NEW YORK"]`  
(Note that columns are specified in figure brackets and values in square brackets).

Or using unnamed binding variables (for binding variables, values should be provided, either by API or when calling from web service):  
`DEPT {DEPTNO, DNAME, LOC} + [?, ?, ?]`

Or named variables:  
`DEPT {DEPTNO, DNAME, LOC} + [:DEPTNO, :DNAME, :LOC]`

### <a name="wiki-update-statement"/>UPDATE statement
Example (named binding variables):  
`DEPT [:DEPTNO] { DNAME } = [ :DNAME ]`

Increment salary of employees hired between given hiredates (for :
```
emp [hiredate >= ? & hiredate <= ?] 
  {sal} = [sal + 100]
```

### <a name="wiki-delete-statement"/>DELETE statement
Use minus sign "-" for DELETE.
DELETE FROM SALGRADE WHERE GRADE = 5:  
`salgrade - [grade = 5]`

or shorthand:  
`salgrade - [5]`

### <a name="wiki-combining-dml"/>Combining several statements in a batch
You can execute several statements in a batch simply by separating them with comma ",". See data creation script in [Appendix Data](#appendix-data) for example of this.

<a name="wiki-appendix-data"/>Appendix Data 
-------------

### Data structure

Sample data tables contain contain information about employees, company departments and salary grades. Use the following SQL script to create data structure.

```sql
CREATE TABLE EMP               -- employees
 (EMPNO INTEGER NOT NULL,      -- employee primary key
  ENAME VARCHAR(10),           -- employee name
  JOB VARCHAR(9),              -- job name   
  MGR INTEGER,                 -- manager, foreign key to the same emp table
  HIREDATE DATE,               -- date of hiring
  SAL DECIMAL(7, 2),           -- salary
  COMM DECIMAL(7, 2),          -- commission
  DEPTNO INTEGER);             -- foreign key to DEPT table
 
CREATE TABLE DEPT              -- departments
 (DEPTNO INTEGER NOT NULL,     -- primary key
  DNAME VARCHAR(14),           -- department name
  LOC VARCHAR(13));            -- location

CREATE TABLE SALGRADE          -- salary grades
 (GRADE INTEGER,               -- grade number - primary key
  LOSAL INTEGER,               -- low salary value
  HISAL INTEGER);              -- high salary value

CREATE TABLE DUMMY (DUMMY INTEGER);

alter table dept add primary key (deptno);
alter table emp add primary key (empno);
alter table emp add foreign key (deptno) references dept(deptno);
alter table emp add foreign key (mgr) references emp(empno);
alter table salgrade add primary key (grade);
```

### Data records

You can use the following ONQL script to load the data.

```
DEPT {DEPTNO, DNAME, LOC} + [10, "ACCOUNTING", "NEW YORK"], 
DEPT {DEPTNO, DNAME, LOC} + [20, "RESEARCH",   "DALLAS"],
DEPT {DEPTNO, DNAME, LOC} + [30, "SALES",      "CHICAGO"], 
DEPT {DEPTNO, DNAME, LOC} + [40, "OPERATIONS", "BOSTON"] 

,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7839, "KING",   "PRESIDENT", null, "1981-11-17", 5000, null, 10] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7698, "BLAKE",  "MANAGER",   7839, "1981-05-01",  2850, null, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7782, "CLARK",  "MANAGER",   7839, "1981-06-9", 2450, null, 10] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7566, "JONES",  "MANAGER",   7839, "1981-04-02",  2975, null, 20] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7499, "ALLEN",  "SALESMAN",  7698, "1981-02-20", 1600,  300, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7521, "WARD",   "SALESMAN",  7698, "1981-02-22", 1250,  500, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7654, "MARTIN", "SALESMAN",  7698, "1981-09-28", 1250, 1400, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7844, "TURNER", "SALESMAN",  7698, "1981-09-8", 1500,    0, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7900, "JAMES",  "CLERK",     7698, "1981-12-3", 950, null, 30] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7934, "MILLER", "CLERK",     7782, "1982-01-23", 1300, null, 10] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7788, "SCOTT",  "ANALYST",   7566, "1982-12-09", 3000, null, 20] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7902, "FORD",   "ANALYST",   7566, "1981-12-3", 3000, null, 20] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7876, "ADAMS",  "CLERK",     7788, "1983-01-12", 1100, null, 20] 
,EMP {EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO} + [7369, "SMITH",  "CLERK",     7902, "1980-12-17", 800, null, 20]

,SALGRADE {GRADE, HISAL, LOSAL} + [1,  700, 1200] 
,SALGRADE {GRADE, HISAL, LOSAL} + [2, 1201, 1400] 
,SALGRADE {GRADE, HISAL, LOSAL} + [3, 1401, 2000] 
,SALGRADE {GRADE, HISAL, LOSAL} + [4, 2001, 3000] 
,SALGRADE {GRADE, HISAL, LOSAL} + [5, 3001, 9999] 
```

Dependencies
------------

## 3rd Party libs

I've tried to keep the list of dependencies as short as possible but currently the following
libraries are being used.

* TODO
...

## Testing

TODO
For testing I use [scala-test](http://www.scalatest.org) for unit-tests and [hsqldb](http://hsqldb.org) for in process db interaction during tests.

Feedback
--------

TODO
If you have any questions or feedback just send me a message here or on [twitter](http://twitter.com/) and if you want to contribute just send a pull request.
