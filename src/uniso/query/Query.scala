package uniso.query

import java.sql.{Array => JArray}
import java.sql.{Date, Timestamp, PreparedStatement}
import uniso.query.metadata._

object Query {
    //TODO parsed expression or even built expression also should be available as a parameter for 
    //performance reasons

    //TODO connection passing design should be improved
    private implicit val conn:java.sql.Connection = null

    def apply(expr:String, params:Any*)(implicit conn:java.sql.Connection):Any = {
        apply(expr, params.toList)(conn)
    }

    def apply(expr: String, params: List[Any])(implicit conn:java.sql.Connection):Any = {
        var i = 0
        apply(expr, params.map{e => i += 1; (i.toString, e)}.toMap)(conn)
    }
    
    def apply(expr: String, params: Map[String, Any])(implicit conn:java.sql.Connection):Any = {
        apply(expr, false, params)
    }

    def apply(expr:String, parseParams:Boolean, params:Any*)(implicit conn:java.sql.Connection):Any = {
        apply(expr, parseParams, params.toList)(conn)
    }

    def apply(expr: String, parseParams:Boolean, params: List[Any])(implicit conn:java.sql.Connection):Any = {
        var i = 0
        apply(expr, parseParams, params.map{e => i += 1; (i.toString, e)}.toMap)(conn)
    }

    def apply(expr: String, parseParams:Boolean, params: Map[String, Any])(implicit conn:java.sql.Connection):Any = {
        new QueryBuilder(Env(params, parseParams)(conn)).build(expr)()
    }

    def apply(expr:Any)(implicit conn:java.sql.Connection) = {
        new QueryBuilder(Env(Map())(conn)).build(expr)()
    }

    def select(expr:String, params:String*)(implicit conn:java.sql.Connection) = {
        apply(expr, params.toList)(conn).asInstanceOf[Result]
    }

    def select(expr: String, params: List[String])(implicit conn:java.sql.Connection) = {
        apply(expr, params)(conn).asInstanceOf[Result]
    }
    
    def select(expr: String, params: Map[String, String])(implicit conn:java.sql.Connection) = {
        apply(expr, params)(conn).asInstanceOf[Result]
    }
    
    def select(expr:String)(implicit conn:java.sql.Connection) = {
        apply(expr)(conn).asInstanceOf[Result]
    }
    
    //private[query] modifier results in runtime error :(
    def select(sql:String, cols:List[QueryBuilder#ColExpr],
               bindVariables:List[Expr], env:Env):Result = {
        println(sql)
        val conn = env.conn
        val st = conn.prepareStatement(sql)
        bindVars(st, bindVariables)
        var i = 0
        val r = new Result(st.executeQuery, Vector(cols.map{
          c => if (c.separateQuery) Column(-1, c.aliasOrName, c.col) else {
            i += 1; Column(i, c.aliasOrName, null)}}.asInstanceOf[List[Column]]:_*))
        env result r
        r
    }
    
    def update(sql:String, bindVariables:List[Expr], env:Env) = {
        println(sql)
        val conn = env.conn
        val st = conn.prepareStatement(sql)
        bindVars(st, bindVariables)
        val r = st.executeUpdate
        st.close
        r
    }
    
    private def bindVars(st:PreparedStatement, bindVariables:List[Expr]) {
        bindVariables.foldLeft(1){(idx, expr) =>
            expr() match {
                case s:String => st.setString(idx, s)
                case n:BigDecimal => st.setBigDecimal(idx, n.bigDecimal)
                case d:Date => st.setDate(idx, d)
                case dt:Timestamp => st.setTimestamp(idx, dt)
                case b:Boolean => st.setBoolean(idx, b)
                case o => st.setObject(idx, o)
            }
            idx + 1
        }        
    }
    
    /*
     * scala command line sample:
     * set JAVA_OPTS=-Djdbc.drivers=com.sap.dbtech.jdbc.DriverSapDB 
     * -Duniso.query.db=jdbc:sapdb://frida/donna -Duniso.query.user=xx -Duniso.query.password=xx
     * scala -classpath C:\dev\scala-test\bin\;C:\java\jdbc\sapdbc.jar 
     * 
     * script:
     * Class.forName("com.sap.dbtech.jdbc.DriverSapDB")
     * import uniso.query._
     * val md = metadata.JDBCMetaData("burvis", "burfull2")
     * val conn = Conn()()
     * val env = new Env(Map(), md, conn)
     */
    def main(args: Array[String]) {
        args.length < 2 match {
            case true => println("usage: <db schema> <sql> [<variable name> <variable value> ...]")
            case _ => {
                val c = Conn()()
                try {
                    Env.metaData(new JDBCMetaData("db", args(0)))
                    Query(args(1), args.drop(2).grouped(2).map(a => (a(0), a(1))).toMap)
                } finally {
                    c.close
                }
            }
        }
    }
    
}
