package org.tresql.test;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import org.tresql.JavaQuery;
import org.tresql.SimpleCache;
import org.tresql.java_api.*;

public class TresqlJavaApiTest implements Runnable {
	
	public static class TresqlJavaApiTestFunctions {
	    public String echo(String s) {
	        return s;
	    }

	    public Long plus(Long a, Long b) {
	        return a + b;
	    }
	}
	
    public void run() {
        println("");
        println("---- Testing Java API ----");
        Env.setLogger(new Logger() {
            // TODO test msg laziness
            @Override
            public void log(LogMessage msg, int level) {
                println("Java API logger [" + level + "]: " + msg.get());
            }
        });
        Env.getCache();
        Env.setCache(new SimpleCache());
        Connection c = Env.getConnection();
        Env.setConnection(c);
        Env.getDialect();
        Env.setDialect(Dialects.InsensitiveCmp("ĀŠāš", "ASas").orElse(
                Dialects.HSQL()));
        Env.getFunctions();
        // TODO TEST function execution, check function results
        Env.setFunctions(new TresqlJavaApiTestFunctions());
        println("id expr: " + Env.getIdExprFunc().getIdExpr("my_table"));
        Env.setIdExprFunc(new IdExprFunc() {
            @Override
            public String getIdExpr(String table) {
                return "nextval(" + table + "_seq)";
            }
        });
        println("id expr: " + Env.getIdExprFunc().getIdExpr("my_table[2]"));
        List<Long> ids = JavaQuery.ids("dept{deptno}", Collections.emptyList());
        Env.getMetadata();
        Env.setMetadata(Metadata.JDBC(null));

        String deptnos = "deptnos:";
        for (Long id : ids) {
            deptnos = deptnos + " " + id;
        }
        println(deptnos);
        if (!ids.contains(10L) || !ids.contains(20L))
            throw new RuntimeException("Failed to select ids");

        println("");
        for (Row r : Query.select("dept[deptno < 100]{deptno, dname}")) {
            println("" + r.i(0) + ": " + r.s(1));
        }

        println("");
        for (Row r : Query.select("dept[deptno < ?]{deptno, dname}", 40)) {
            println("" + r.int_(0) + ": " + r.string(1));
        }
        
        println("");
        for (Row r : Query.select("dummy {plus(1, 2)}")) {
            println("" + r.long_(0));
        }        
        println("--------------------------");
        println("");
    }

    private void println(String s) {
        System.out.println(s);
    }
}
