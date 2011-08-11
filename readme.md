TreSQL - object-notated SQL for web clients
=========================================

TreSQL (Tree SQL) is a query language built on top of SQL that can select data into hierarchical [JSON](http://en.wikipedia.org/wiki/JSON) objects. This allows to select data for complex input form using one simple query, without any additional server-side programming. TreSQL has very compact notation and provides many powerful shortcuts. It aims to provide complete functionality of SQL for query and data manipulation purposes. In a web application or a client-server application, it can fullfill all the database querying needs for the client. It's Scala API is much more concise than JDBC and can be used for all database requests.

<b>With TreSQL, we provide: </b>  
1. Query and data manipulation language with SQL functionality  
2. Database API from Scala or Java  
3. [Web service and simple web form](http://uniso.lv/querytest/get) to run queries and receive data in JSON format.

<b>
Reasons to use TreSQL:  
</b>
1. Get hierarchical instead of flat results.  
2. API that is much shorter than JDBC.  
3. Syntax shortcuts. TreSQL infers information about primary keys, foreign keys, column types [TODO], array binding etc.  
4. No superfluous programming or object declaration needed.  



## Background

TreSQL is written in [Scala](http://www.scala-lang.org/) on top of JDBC. It provides a query language that can be used as a web service, i.e. from Javascript. It also provides an API that can be used from Scala or Java. Since it uses JDBC, it should be compatible with most JDBC supported databases.  

One shortcoming of SQL is that it can select data only in terms of tables and records. In a client/web application, you often have a GUI screen where data is based on more than one table, a simple example if which is a parent/child table relationship (master-detail input form). A common solution here requires a server-side layer that executes several SQL queries to extract parent table record and related child records, converts them to corresponding objects and passes to client. 
One example of such solution is Hibernate to convert table records to their corresponding objects. However, Hibernate requires to redefine data structure in terms of objects that is already nicely defined in terms of SQL tables. It also lacks flexibility  filtering result data, requires extra coding, etc.

TreSQL allows client to gather data from database without any server-side programming (or any client-side programming) except a single query to gather data. It does not require declaration of any other custom data objects because all the results are passed in type-unspecific JSON format. It does not sacrifice performance trying to combine a lot of data into one huge SQL query, because one TreSQL query can get split into several SQL statements on the server.  

See documentation below how TreSQL can make your life easier (some familiarity with SQL is desirable).  

Try it online
-------------
You can test TreSQL queries, including all examples from this guide, directly using our web service:  
http://uniso.lv/querytest/get

This web service is set up with test data described in [Appendix Data](wiki/language-guide#wiki-appendix-data). (We do no allow data manipulation at this URL).  
Note: With this web service you can not only receive result data, but also see SQL statement(s) that are generated from your TreSQL query. This way you can understand the meaning of any TreSQL query even without explanations below.  

<a name="wiki-quickstart"/>Quickstart
----------
Consider the following simplified tables for employees and departments with parent-child relationship:  

    dept(deptno (primary key), dname)
    emp(empno (primary key), ename, deptno (references dept))

Consider the following data in those tables: 

    dept[10, "ACCOUNTING"]
    emp[7839, "BLAKE", 10]
    emp[7839, "SCOTT", 10]

Example 1. To select employees with their department name, use the following TreSQL query:  
`emp/dept {empno, ename, dname}`

The query returns a JSON object:  

    [
      {"empno": 7698, "ename": "BLAKE", "dname": "ACCOUNTING"}, 
      {"empno": 7788, "ename": "SCOTT", "dname": "ACCOUNTING"}
    ]

Example 2. Now let's select parent record and the child records:

    dept {
      deptno, 
      dname, 
      |emp [deptno = :1(1)] {empno, ename} emp
    }

Result:

    [
        {
    	"deptno": 10, 
    	"dname": "ACCOUNTING", 
    	"emp": [
                {"empno": 7698, "ename": "BLAKE"}, 
                {"empno": 7788, "ename": "SCOTT"}
            ]
        }
    ]


This query returns information from both tables in a JSON object. It is ready to use by the client application.   
You can also choose to receive data more concisely in an array format:

    [
      [
        10, "ACCOUNTING", [
          [7698, "BLAKE"], 
          [7788, "SCOTT"]
        ]
      ]
    ]

### Syntax explanation

In example 1, the following TreSQL syntax has been used:  
`emp/dept {empno, ename, dname}`

Figure brackets {} denote columns to be selected.  
Slash "/" is an Xpath-like notation to follow a foreign key to parent table.  
In this example, TreSQL has inferred the foreign key relationship between the two tables. It was not necessary to specify a join condition.  

In the example 2:

    dept {
      deptno, 
      dname, 
      |emp [deptno = :1(1)] {empno, ename} emp
    }

Figure brackets {} again denote objects to be selected. The 3d object is a subselect from emp table to return employee records.  
Square brackets [] denote a WHERE condition.   
Notation :1(1) is a little tricky. Colon ":" denotes a binding variable. :1(1) instructs to bind value to parent SELECT statement (1 level up) , 1st value. The TreSQL query in this example produces 2 SELECT statements:   

    1. select deptno, dname from dept  
    2. select empno, ename from emp where deptno = :1(1)  

Therefore, :1(1) substitutes deptno value from first select as deptno value in second select statement.  

Further reading
---------------

[Compiling and installing](Query/wiki/Installation)  
[TreSQL language guide](Query/wiki/Language-documentation)  