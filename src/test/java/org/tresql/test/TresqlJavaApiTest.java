package org.tresql.test;

import java.sql.Connection;
import java.util.*;

import org.tresql.LogTopic;
import org.tresql.java_api.*;

public class TresqlJavaApiTest {

    public void run(ThreadLocalResources resources) {
        println("");
        println("---- Testing Java API ----");
        resources.setLogger(new Logger() {
            // TODO test msg laziness
            @Override
            public void log(LogMessage msg, LogParams params, LogTopic topic) {
                println("Java API logger [" + topic + "]: " + msg.get() + "; params: " + params.get());
            }
        });
        Connection c = resources.getConnection();
        resources.setConnection(c);
        resources.setDialect(Dialects.HSQL());
        println("id expr: " + resources.getIdExprFunc().getIdExpr("my_table"));
        resources.setIdExprFunc(new IdExprFunc() {
            @Override
            public String getIdExpr(String table) {
                return "nextval(" + table + "_seq)";
            }
        });
        println("id expr: " + resources.getIdExprFunc().getIdExpr("my_table[2]"));
        resources.setMetadata(Metadata.JDBC(resources.getConnection(), null));

        println("");
        for (Row r : Query.select("dept[deptno < 100]{deptno, dname}", resources)) {
            println("" + r.i(0) + ": " + r.s(1));
        }

        println("");
        for (Row r : Query.select("dept[deptno < ?]{deptno, dname}", resources, 40)) {
            println("" + r.int_(0) + ": " + r.string(1));
        }

        println("");
        java.util.Map<String, Object> pars = new java.util.HashMap<String, Object>();
        pars.put("id", 10);
        for (Row r : Query.select(
                "dept[deptno = :id]{deptno, dname, |emp {ename} emps}", resources, pars)) {
            println("" + r.int_(0) + ": " + r.string(1) + ": " + "emps:");
            for (Row er : r.result("emps")) {
                println("  " + er.s("ename"));
            }
        }

        println("");
        for (Map<String, Object> r : Query.select(
                "dept[deptno < 30]{deptno, dname, |emp {ename} emps}", resources)
                .toListOfMaps()) {
            println("toListOfMaps() - " + r.get("deptno") + ": "
                    + r.get("dname") + ", " + "emps:");
            for (Map<String, Object> er : (List<Map<String, Object>>) r
                    .get("emps")) {
                println("toListOfMaps() -   " + er.get("ename"));
            }
        }

        println("");
        for (Row r : Query.select("dept[60]{deptno, dname}", resources)) {
            println("" + r.i(0) + ": " + r.s(1));
        }
        Query.execute("dept[60]{dname} = ['POLAR FOX']", resources);
        for (Row r : Query.select("dept[60]{deptno, dname}", resources)) {
            println("" + r.i("deptno") + ": " + r.s("dname"));
        }

        println("");
        for (Row r : Query.select("dept[60]{deptno, dname}", resources)) {
            java.util.Map<String, Object> map = r.toMap();
            println("toMap() - " + map.get("deptno") + ": "
                    + map.get("dname"));
        }

        println("");
        Result res = Query.select("dept[60]{deptno, dname}", resources);
        println("columns(0).name, index: " + res.column(0).name + ", "
                + res.column(0).index);
        println("columns(1).name, index: " + res.columns().get(1).name + ", "
                + res.columns().get(1).index);
        println("column count: " + res.columnCount() + " ("
                + res.columns().size() + ", actually)");

        println("--------------------------");
        println("");

        //set back previous env values
    }

    private void println(String s) {
        System.out.println(s);
    }
}
