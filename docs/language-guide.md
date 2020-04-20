tresql language guide
===================

Contents
--------
* [Data selection](#data-selection)  
* [Data manipulation](#data-manipulation)
* [Expression list](#expression-list)  
* [Sample database structure](#sample-database-structure)  
* [Syntax quickchart](#syntax-quickchart)  

tresql provides syntax for both querying (SELECT) and data manipulation (INSERT, UPDATE)
as well as more complicated SQL constructs like Common Table Expressions (CTE aka WITH queries).

Data selection 
--------------

[Simple SELECTs](#simple-selects)  
[Binding variables](#binding-variables)  
[Table joins](#table-joins)  
  * [Inner join](#inner-join)
  * [Outer join](#outer-join)
  * [Shortcuts](#join-shortcuts)
  * [Implicit outer, explicit inner joins](#implicit-left-join-explicit-inner-join-on-shortcut-syntax)
  * [Product join](#product)
  * [Table as function WITH ORDINALITY](#table-as-function-with-ordinality)
  
[Subqueries IN and EXISTS](#subqueries-in-and-exists)  
[ORDER, DISTINCT, GROUP BY, HAVING](#order-distinct-group-by-having)  
[LIMIT, OFFSET](#limit-offset)  
[UNION, INTERSECT, EXCEPT](#union-intersect-except)  
[Aggregate expressions](#aggregate-expressions)  
[Common Table Expressions - CTE (WITH queries)](#common-table-expressions---cte-with-queries)  
[Hierarchical queries](#hierarchical-queries)  

### Simple SELECTs 

Select all records from table `emp` with all columns:
`emp` or `emp {*}`
```sql
select * from emp
```

Select some columns with WHERE condition:
`emp [ empno = 7369] { ename, job }`
```sql
select ename, job from emp where empno = 7369
```

`emp [ sal > 1000 & sal < 2000 ] {ename, sal}`
```sql
select ename, sal from emp where sal > 1000 and sal < 2000
```

`emp [ 1000 < sal < 2000 ] {ename, sal}`
```sql
select ename, sal from emp where 1000 < sal and sal < 2000
```

`dept [deptno in (10, 20)]`

```sql
select * from dept where deptno in(10, 20)
```

#### LIKE comparison
In WHERE conditions, tilde "~" stands for LIKE comparison, and "~~" for case-insensitive LIKE comparison:

`emp [ job ~ '%LYST'] {ename, job}`

```sql
select ename, job from emp where job like '%LYST'
```

ILIKE operator depends on database dialect, since it is not SQL standard. For hsqldb sql is like this:

`emp [ job ~~ '%LYST'] {ename, job}`

```sql
select ename, job from emp where lcase(job) like lcase('%LYST')
```

#### CAST operator

This is postgresql style "::" operator that can be added after expression.

`dept{deptno::varchar}#(1)`

```sql
select deptno::varchar from dept order by 1 asc
```

> NOTE: CAST operator will not work on hsqldb

#### Table less query

You can omit table clause or put "null" in table clause to produce table less query:

`{1, 'a'}` or `null {1, 'a'}`

```sql
select 1, 'a'
```

### Binding variables

There are named variables, prefixed by colon ":" :<variable_name> and unnamed variable placeholders, specified by question mark "?". The following are two variations of the same query:
```
emp [empno = :empno] {ename}
emp [empno = ?] {ename}
```

```sql
select ename from emp where empno = ?/*empno*/
```

tresql supports array binding that allows to bind several binding variables in sql as one array parameter in tresql.
What distinguishes it is that you specify only one placeholder, and supply only one binding value, which is an array:

`emp [empno in (:ids)] {empno, ename, job}`

For this query, an array of employee ids should be supplied, such as `[7839, 7782]`:

```sql
select empno, ename, job from emp where empno in(/*ids[*/?,?/*]ids*/)
/*Bind vars: ids = List(7839, 7782)*/
```

tresql supports optional variable binding i.e. it removes or reduces clause where optional variable is referenced,
but not present in binding list. Optional variable must have `?` at the end of name:

`dept[deptno = :id?] {dname}`

If variable is present:

```sql
select dname from dept where deptno = ?/*id*/
/*Bind vars: id = 10*/
```

If variable is not present:

```sql
select dname from dept
```

Optional binding works also in LIMIT OFFSET clauses

`dept {dname}@(:limit?)`

```sql
select dname from dept limit ?/*limit*/
/*Bind vars: limit = 1*/

select dname from dept
/* No bind variable */
```

### Table joins 

#### Inner join

`emp [emp.deptno = dept.deptno] dept { ename, job, dname }`

```sql
select ename, job, dname from emp join dept on emp.deptno = dept.deptno
```

#### Outer join.

Left outer join is denoted by question mark "?" after table name being joined:

`emp e [e.deptno = d.deptno] dept d[e.mgr = e2.mgr] emp e2? { e.ename, e.job, d.dname, e2.ename manager }`

```sql
select e.ename, e.job, d.dname, e2.ename manager
  from emp e join dept d on e.deptno = d.deptno left join emp e2 on e.mgr = e2.mgr
```

Right outer join is denoted by question mark "?" before table name being joined:

`emp e[e.deptno = d.deptno]?dept d`

```sql
select * from emp e right join dept d on e.deptno = d.deptno
```

#### Join shortcuts

tresql provides slash "/" as a shortcut syntax for joins. It infers the relationship between the two tables if
there is just 1 foreign key joining them. The following query joins EMP and DEPT tables:  

`emp e/dept d {e.ename, d.dname}`

```sql
select e.ename, d.dname from emp e join dept d on e.deptno = d.deptno
```

You can also add additional condition to join shortcut syntax:

`dept d/[job = 'PRESIDENT']emp? e {dname, ename}`

```sql
select dname, ename from dept d left join emp e on d.deptno = e.deptno and job = 'PRESIDENT'
```

Multiple joins for the same table. If table is to be joined multiple times use semicolon ; to the left of the table.

`emp e[e.empno = w.empno]work w; e/dept [ename = 'SCOTT']{hours, ename, dname}`

```sql
select hours, ename, dname from emp e
  join work w on e.empno = w.empno
  join dept on e.deptno = dept.deptno where ename = 'SCOTT'
```

Foreign key join shortcut syntax. If table has more then one foreign key reference to other table one can specify only foreign key column name,
or list of foreign column names if table is to be joined more than one time.

`emp e[e.deptno d]dept {ename, dname}`

```sql
select ename, dname from emp e join dept d on e.deptno = d.deptno
```

`work w[w.empno e, w.empno_mgr m]emp[m.ename != null] {e.ename, m.ename, hours}`

```sql
select e.ename, m.ename, hours
 from work w join emp e on w.empno = e.empno left join emp m on w.empno_mgr = m.empno
where m.ename is not null
```

#### Implicit left join, explicit inner join on shortcut syntax

If join column for the left table is primary key or nullable foreign key
shortcut syntax joins are translated to sql as left joins. If inner join is required put ! mark after table name or alias. 

Implicit outer join:

Primary key table is on the left side.

`dept/emp{dname, ename}`

```sql
select dname, ename from dept left join emp on dept.deptno = emp.deptno
```

Nullable foreign key column.

`emp m[m.mgr]emp e {m.ename, e.ename}`

```sql
select m.ename, e.ename from emp m left join emp e on m.mgr = e.empno
```

Explicit inner join:

`dept/emp!`

```sql
select * from dept join emp on dept.deptno = emp.deptno
```

`work[empno e!, empno_mgr! m]emp`

```sql
select * from work join emp e on empno = e.empno join emp m on empno_mgr = m.empno
```

#### Product

To create product i.e. no join use empty square brackets []:

`dept[]salgrade`

```sql
select * from dept join salgrade on true
```

#### Table as function WITH ORDINALITY

You can use data generation functions in table clause with optional WITH ORDINALITY clause
denoted by '#' symbol before column declarations (see database documentation for details).

```dept
   [deptno = x] unnest(sequence_array(10, 20, 10)) a(# x::int, i::int)
   [a.i = b.i]  unnest(sql('sequence_array(current_date, current_date + 1 day, 1 day)'))
     b(# date::date, i::int)
   {a.i, dname, date}
```

```sql
select a.i, dname, date from dept
  join unnest(sequence_array(10,20,10)) with ordinality a(x, i) on deptno = x
  join unnest(sequence_array(current_date, current_date + 1 day, 1 day))
    with ordinality b(date, i) on a.i = b.i
```

### Subqueries IN and EXISTS
`dept [ deptno in (emp [sal >= 5000]  {deptno}) ] {dname}`
```sql
select dname from dept where deptno in(select deptno from emp where sal >= 5000)
```

`dept d [exists(emp e[e.deptno = d.deptno])]`

or exists can be omitted

`dept d [(emp e[e.deptno = d.deptno])]`

```sql
select * from dept d where exists(select * from emp e where e.deptno = d.deptno)
```

### ORDER, DISTINCT, GROUP BY, HAVING
#### ORDER BY
Sort employees by ename in ascending order:  

`emp { ename, sal } #(ename)`

```sql
select ename, sal from emp order by ename asc
```

Sort employees by ename in descending order:

`emp { ename, sal } #(~ename)`

```sql
select ename, sal from emp order by ename desc
```
 
Sort descending by department name and ascending by employee name:  
`emp e/dept d { d.dname, e.ename, e.sal } #(~d.dname, e.ename)`

```sql
select d.dname, e.ename, e.sal from emp e join dept d on e.deptno = d.deptno
  order by d.dname desc, e.ename asc
```

NULL FIRST, NULLS LAST clause

`dept/emp{dname, ename, mgr}#(dname, mgr null, null ~ename)`

```sql
select dname, ename, mgr from dept left join emp on dept.deptno = emp.deptno
  order by dname asc, mgr asc nulls last, ename desc nulls first
```

#### DISTINCT 
DISTINCT is denoted by "#" sign before column list. The following query selects distinct departments from EMP table:   

`emp #{ deptno }`

```sql
select distinct deptno from emp
```

DSTINCT in aggregate function

`emp{count(# deptno)}`

```sql
select count(distinct deptno) from emp
```

#### GROUP BY:
```
emp e [s.losal <= e.sal < s.hisal] salgrade s 
  {grade, hisal, losal, count(empno)}
  (grade, hisal, losal) #(grade)
```

This query groups employees by salary grades. The GROUP BY columns are specified in round brackets: (grade, hisal, losal). This produces following SELECT statement:
```sql
select grade, hisal, losal, count(empno) from emp e join salgrade s on s.losal <= e.sal and e.sal < s.hisal
  group by grade,hisal,losal order by grade asc
```

#### GROUP BY with HAVING
Select employees only in those salary grades where employee count > 1. HAVING is specified by ^ operator:

```
emp e [s.losal <= e.sal < s.hisal] salgrade s  {grade, hisal, losal, count(empno)} (grade, hisal, losal)^(count(*) > 1) #(grade)
```

```sql
select grade, hisal, losal, count(empno)
  from emp e join salgrade s on s.losal <= e.sal and e.sal < s.hisal
  group by grade,hisal,losal having count(*) > 1 order by grade asc
```

### LIMIT, OFFSET

Only LIMIT clause

`dept#(deptno)@(1)`

```sql
select * from dept order by deptno asc limit 1
```

Only OFFSET clause

`dept#(deptno)@(3, )`

```sql
select * from dept order by deptno asc offset 3
```

Both clauses

`dept#(deptno)@(1 2)`

```sql
select * from dept order by deptno asc offset 1 limit 2
```

### UNION, INTERSECT, EXCEPT
UNION is specified by "+" operator between queries:

`emp [deptno = 10] { deptno, ename} + emp [deptno = 20] { deptno, ename} + emp [deptno = 30] { deptno, ename}`

```sql
select deptno, ename from emp where deptno = 10 union
select deptno, ename from emp where deptno = 20 union
select deptno, ename from emp where deptno = 30
```

UNION ALL is specified by "++" operator between queries:

`emp [deptno = 10] { deptno, ename} ++ emp [deptno = 10] { deptno, ename}`

```sql
select deptno, ename from emp where deptno = 10 union all select deptno, ename from emp where deptno = 10
```

INTERSECT is specified by "&&" :  

`emp[sal >= 1000] {empno, ename} && emp[sal <= 3000] {empno, ename}`

```sql
select empno, ename from emp where sal >= 1000 intersect select empno, ename from emp where sal <= 3000
```

EXCEPT is specified by "-" operator between queries:

Select departments which do not have any employees

`dept{deptno} - dept/emp?[ename != null]{dept.deptno}`

```sql
select deptno from dept except
select dept.deptno from dept left join emp on dept.deptno = emp.deptno where ename is not null
```

If you would like "+" or "-" operation between queries to be treated as plus or minus you should cast query expression like in
example below queries in the column clause are cast to "decimal" type:

`dept d { (emp e[d.deptno = e.deptno]{sum(sal)})::decimal - (emp e[d.deptno = e.deptno]{sum(comm)})::decimal diff }`

```sql
select
  (select sum(sal) from emp e where d.deptno = e.deptno)::decimal -
  (select sum(comm) from emp e where d.deptno = e.deptno)::decimal diff
from dept d
```

### Aggregate expressions

Aggregate expressions depend on the database. Syntax is like function call optionally followed by filter and/or order
expressions.

`dept{group_concat(dname)#(dname)}`

```sql
select group_concat(dname order by dname asc) from dept
```

`dept{group_concat(dname)#(dname)[deptno < 30]}`

```sql
select group_concat(dname order by dname asc) filter (where deptno < 30) from dept
```

### Common Table Expressions - CTE (WITH queries)

### Hierarchical queries
This topic explains how single tresql query can generate multiple parent-child connected sql queries returning non tabular (nested) result structure. 

```
dept {
  deptno, 
  dname, 
  |[deptno = dept.deptno] emp {empno, ename} emp
}
```

or shorter since emp table has one reference to dept table:

```
dept {
  deptno, 
  dname, 
  |emp {empno, ename} emp
}
```

For each department row employees query is performed with current department number as a filtering parameter:

```sql
select deptno, dname, dept.deptno dept_deptno_ from dept
  select empno, ename from emp where (deptno = ?/*1(dept_deptno_)*/)
  /*Bind vars: 1(dept_deptno_) = 10*/
  select empno, ename from emp where (deptno = ?/*1(dept_deptno_)*/)
  /*Bind vars: 1(dept_deptno_) = 20*/
  /* ... */
```

Result
```yaml
- deptno: 10
  dname: ACCOUNTING
  emp:
  - empno: 10055
    ename: Nicky
  - empno: 10052
    ename: Lara
  - empno: 7782
    ename: CLARK
  - empno: 7839
    ename: KING
- deptno: 20
  dname: RESEARCH
  emp:
  - empno: 7566
    ename: JONES
  - empno: 7902
    ename: FORD
  - empno: 7876
    ename: ADAMS
  - empno: 7788
    ename: SCOTT
- deptno: 30
  dname: SALES
  emp:
  - empno: 7499
    ename: ALLEN SMITH
  - empno: 7654
    ename: MARTIN BLAKE
  - empno: 10023
    ename: DEISE ROSE
  - empno: 7698
    ename: BLAKE
  - empno: 7369
    ename: SMITH
- deptno: 40
  dname: DEVELOPMENT
  emp:
  - empno: 10024
    ename: AMY
  - empno: 10025
    ename: LENE
```

The pipe "|" syntax specifies a query for nested object. This enclosed query follows the same tresql syntax as it's parent query.
It has to reference parent query, and this is specified by filtering condition in square brackets "[]" immediately after pipe "|"
Filtering condition has to reference some `table.column` from parent query.

Three level example:

```
dept [deptno in (10, 20)]
  { deptno,  dname,
    |emp
      { empno, ename, mgr,
        |[mgr.empno = emp.empno & mgr.deptno = dept.deptno]emp mgr
          { ename manager } mgr_object
      } emp_object
  }
```

```sql
select deptno, dname from dept where deptno in(10, 20)
  select empno, ename, mgr from emp where (deptno = ?/*1(1)*/)
  /*Bind vars: 1(1) = 10*/
    select ename manager from emp mgr where (mgr.empno = ?/*1(1)*/ and mgr.deptno = ?/*2(1)*/)
    /*Bind vars: 1(1) = 10055, 2(1) = 10*/
  /* ... */
```

Result
```yaml
- deptno: 10
  dname: ACCOUNTING
  emp_object:
  - empno: 10055
    ename: Nicky
    mgr: 
    mgr_object:
    - manager: Nicky
  - empno: 10052
    ename: Lara
    mgr: 
    mgr_object:
    - manager: Lara
  - empno: 7782
    ename: CLARK
    mgr: 7839
    mgr_object:
    - manager: CLARK
  - empno: 7839
    ename: KING
    mgr: 
    mgr_object:
    - manager: KING
- deptno: 20
  dname: RESEARCH
  emp_object:
  - empno: 7566
    ename: JONES
    mgr: 7839
    mgr_object:
    - manager: JONES
  - empno: 7902
    ename: FORD
    mgr: 7566
    mgr_object:
    - manager: FORD
  - empno: 7876
    ename: ADAMS
    mgr: 7788
    mgr_object:
    - manager: ADAMS
  - empno: 7788
    ename: SCOTT
    mgr: 7839
    mgr_object:
    - manager: SCOTT
```

The example above can be written using technical parent query referencing syntax:

```
dept [deptno in (10, 20)]
  { deptno,  dname,
    |[deptno = :1(1)] emp
      { empno, ename, mgr,
        |[mgr.empno = :1(1) & mgr.deptno = :2(1)]emp mgr
          { ename manager } mgr_object
      } emp_object
  }
```

Syntax :m(N) specifies to use as a binding value column number N (starting from 1) of parent (enclosing) select number m.
Think of select numbering as this: parent (enclosing) select query is number 1, it's parent is number 2, etc.  
The following 3-level query returns departments, their employees (1st level nesting) and their managers,
if the manager belongs to the same department. So for managers subselect, departments query is a 2nd level parent select.
However this syntax is discouraged since `parent_query_table.column` syntax is more understandable.   


Select employees in salary grades:
```
salgrade sg [grade in(3,4)]
  { grade, losal, hisal, |[sal >= sg.losal & sal <= sg.hisal ]emp {ename, sal} }
```
```sql
select grade, losal, hisal, sg.losal sg_losal_, sg.hisal sg_hisal_ from salgrade sg where grade in(3, 4)
  select ename, sal from emp where (sal >= ?/*1(sg_losal_)*/ and sal <= ?/*1(sg_hisal_)*/)
  /*Bind vars: 1(sg_losal_) = 1401, 1(sg_hisal_) = 2000*/
  select ename, sal from emp where (sal >= ?/*1(sg_losal_)*/ and sal <= ?/*1(sg_hisal_)*/)
  /*Bind vars: 1(sg_losal_) = 2001, 1(sg_hisal_) = 3000*/
```
Result
grade | losal | hisal | ename | sal
:----:|------:|------:|:------|---:
3     |1401   | 2000  | ALLEN SMITH | 1600.00
4     |2001   | 3000  | JONES       | 2975.00
|||| BLAKE       | 2850.00
|||| CLARK       | 2450.00
|||| FORD        | 3000.00

The following 3-level query selects departments, salary grades encountered in a given department with employee count, and employees falling into each salary grade:
```
dept d1
  { deptno, dname,
    |emp [sal >= losal & sal <= hisal] salgrade
      { grade, hisal, losal, count(empno) emp_count,
        |[sal >= salgrade.losal & sal <= salgrade.hisal & dept.deptno = d1.deptno] emp/dept
          { ename, dept.deptno, dname, sal } #(empno) emp
      }(grade, hisal, losal) #(1) emps
  } #(deptno)
```

```sql
select deptno, dname, d1.deptno d1_deptno_ from dept d1 order by deptno asc
  select grade, hisal, losal, count(empno) emp_count, salgrade.losal salgrade_losal_, salgrade.hisal salgrade_hisal_
    from emp join salgrade on sal >= losal and sal <= hisal
      where emp.deptno = ?/*1(d1_deptno_)*/ group by grade,hisal,losal order by 1 asc
  /*Bind vars: 1(d1_deptno_) = 10*/
  select grade, hisal, losal, count(empno) emp_count, salgrade.losal salgrade_losal_, salgrade.hisal salgrade_hisal_
    from emp join salgrade on sal >= losal and sal <= hisal
      where emp.deptno = ?/*1(d1_deptno_)*/ group by grade,hisal,losal order by 1 asc
  /*Bind vars: 1(d1_deptno_) = 20*/
    select ename, dept.deptno, dname, sal from emp join dept on emp.deptno = dept.deptno
      where (sal >= ?/*1(salgrade_losal_)*/ and sal <= ?/*1(salgrade_hisal_)*/ and dept.deptno = ?/*2(d1_deptno_)*/)
        order by empno asc
    /*Bind vars: 1(salgrade_losal_) = 700, 1(salgrade_hisal_) = 1200, 2(d1_deptno_) = 20*/
/* ... */
```

Result
```yaml
- deptno: 10
    dname: ACCOUNTING
    emps:
    - grade: 4
      hisal: 3000
      losal: 2001
      emp_count: 1
      emp:
      - ename: CLARK
        deptno: 10
        dname: ACCOUNTING
        sal: 2450
- deptno: 20
    dname: RESEARCH
    emps:
    - grade: 1
      hisal: 1200
      losal: 700
      emp_count: 2
      emp:
      - ename: ADAMS
        deptno: 20
        dname: RESEARCH
        sal: 1100
      - ename: MĀRTIŅŠ ŽVŪKŠĶIS
        deptno: 20
        dname: RESEARCH
        sal: 1100
    - grade: 4
      hisal: 3000
      losal: 2001
      emp_count: 2
      emp:
      - ename: JONES
        deptno: 20
        dname: RESEARCH
        sal: 2975
      - ename: FORD
        deptno: 20
        dname: RESEARCH
        sal: 3000
  - deptno: 30
    dname: SALES
    emps:
    - grade: 1
      hisal: 1200
      losal: 700
      emp_count: 1
      emp:
      - ename: SMITH
        deptno: 30
        dname: SALES
        sal: 850
    - grade: 2
      hisal: 1400
      losal: 1201
      emp_count: 1
      emp:
      - ename: MARTIN BLAKE
        deptno: 30
        dname: SALES
        sal: 1250
    - grade: 3
      hisal: 2000
      losal: 1401
      emp_count: 1
      emp:
      - ename: ALLEN SMITH
        deptno: 30
        dname: SALES
        sal: 1600
    - grade: 4
      hisal: 3000
      losal: 2001
      emp_count: 1
      emp:
      - ename: BLAKE
        deptno: 30
        dname: SALES
        sal: 2850
```

Data manipulation 
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


Expression list
---------------
You can combine several expressions in comma separated list. For every element sql statement will be issued.

```
emp [ job = "ANALYST"] {ename},
dept { dname }
```

`round(1.55, 1), 1 + 9`

Sample database structure
-------------------------

Database structure see [db.sql](/src/test/resources/db.sql)

Syntax quickchart 
------------------
```
{}      Selection set (columns clause).
[]      WHERE or join conditions
/       Shortcut join between tables (referential constraint defined), division operator
:foo    Named binding variable foo.
?       Unnamed binding variable, outer join
+       INSERT, plus operator
=       UPDATE, equals operator
-       DELETE, minus operator
&       AND
|       OR, also nested query
!       NOT, explicit inner join
++      UNION
&&      INTERSECT
,       Delimiter for columns and statements
()      GROUB BY
^()     HAVING
#       ORDER BY, DISTINCT, WITH ORDINALITY
~       LIKE, descending order
~~      ILIKE (case-insensitive)
@()     OFFSET, LIMIT
;       join separator
fun()   function
::      cast operator
```

### SELECT statement structure

`<from>[<where>][<columns>][<group by> [<having>]][<order>][<offset> [<limit>]`

FROM

`<table expr>[<join><table expr> ...]`

Example:

`table1[join cond]table2[where]{columns} (group cols)^(having expr) #(order) (offset limit)`
