package uniso.query

import java.sql.{ Array => JArray }
import java.sql.{ Date, Timestamp, PreparedStatement }
import uniso.query.metadata._

object Query {
  //TODO parsed expression or even built expression also should be available as a parameter for 
  //performance reasons

  def apply(expr: String, params: Any*): Any = {
    apply(expr, params.toList)
  }

  def apply(expr: String, params: List[Any]): Any = {
    var i = 0
    apply(expr, params.map { e => i += 1; (i.toString, e) }.toMap)
  }

  def apply(expr: String, params: Map[String, Any]): Any = {
    apply(expr, false, params)
  }

  def apply(expr: String, parseParams: Boolean, params: Any*): Any = {
    apply(expr, parseParams, params.toList)
  }

  def apply(expr: String, parseParams: Boolean, params: List[Any]): Any = {
    var i = 0
    apply(expr, parseParams, params.map { e => i += 1; (i.toString, e) }.toMap)
  }

  def apply(expr: String, parseParams: Boolean, params: Map[String, Any]): Any = {
    val exp = QueryBuilder(expr, Env(params, parseParams))
    exp()
  }

  def apply(expr: Any) = {
    val exp = QueryBuilder(expr, Env(Map()))
    exp()
  }

  def select(expr: String, params: String*) = {
    apply(expr, params.toList).asInstanceOf[Result]
  }

  def select(expr: String, params: List[String]) = {
    apply(expr, params).asInstanceOf[Result]
  }

  def select(expr: String, params: Map[String, String]) = {
    apply(expr, params).asInstanceOf[Result]
  }

  def select(expr: String) = {
    apply(expr).asInstanceOf[Result]
  }

  //private[query] modifier results in runtime error :(
  def select(sql: String, cols: List[QueryBuilder#ColExpr],
    bindVariables: List[Expr], env: Env): Result = {
    println(sql)
    val conn = env.conn
    val st = conn.prepareStatement(sql)
    bindVars(st, bindVariables)
    var i = 0
    val r = new Result(st.executeQuery, Vector(cols.map { c =>
      if (c.separateQuery) Column(-1, c.aliasOrName, c.col) else {
        i += 1; Column(i, c.aliasOrName, null)
      }
    }.asInstanceOf[List[Column]]: _*))
    env update r
    r
  }

  def update(sql: String, bindVariables: List[Expr], env: Env) = {
    println(sql)
    val conn = env.conn
    val st = conn.prepareStatement(sql)
    bindVars(st, bindVariables)
    val r = st.executeUpdate
    st.close
    r
  }

  private def bindVars(st: PreparedStatement, bindVariables: List[Expr]) {
    bindVariables.foldLeft(1) { (idx, expr) =>
      expr() match {
        case s: String => st.setString(idx, s)
        case n: BigDecimal => st.setBigDecimal(idx, n.bigDecimal)
        case d: Date => st.setDate(idx, d)
        case dt: Timestamp => st.setTimestamp(idx, dt)
        case b: Boolean => st.setBoolean(idx, b)
        case o => st.setObject(idx, o)
      }
      idx + 1
    }
  }

}
