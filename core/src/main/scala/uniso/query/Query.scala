package uniso.query

import java.sql.{ Array => JArray }
import java.sql.{ Date, Timestamp, PreparedStatement, ResultSet }
import uniso.query.metadata._

object Query {

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
    val exp = QueryBuilder(expr, Env(params, false, parseParams))
    exp()
  }

  def apply(expr: Any) = {
    val exp = QueryBuilder(expr, Env(Map(), false))
    exp()
  }

  def parse(expr: String) = QueryParser.parseAll(expr)
  def build(expr: String): Expr = QueryBuilder(expr, Env(Map(), true, false))
  def build(expr: Any): Expr = QueryBuilder(expr, Env(Map(), true, false))

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

  private[query] def select(sql: String, cols: List[QueryBuilder#ColExpr],
    bindVariables: List[Expr], env: Env): Result = {
    Env log sql
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    var i = 0
    val r = new Result(st.executeQuery, Vector(cols.map { c =>
      if (c.separateQuery) Column(-1, c.aliasOrName, c.col) else {
        i += 1; Column(i, c.aliasOrName, null)
      }
    }: _*), env.reusableExpr)
    env update r
    r
  }

  private[query] def update(sql: String, bindVariables: List[Expr], env: Env) = {
    Env log sql
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    val r = st.executeUpdate
    if (!env.reusableExpr) st.close
    r
  }

  private def statement(sql: String, env: Env) = {
    if (env.reusableExpr)
      if (env.statement == null) {
        val conn = env.conn; val s = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY); env.update(s); s
      } else env.statement
    else { val conn = env.conn; conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY) }
  }

  private def bindVars(st: PreparedStatement, bindVariables: List[Expr]) {
    bindVariables.foldLeft(1) { (idx, expr) =>
      expr() match {
        case s: String => st.setString(idx, s)
        case n: BigDecimal => st.setBigDecimal(idx, n.bigDecimal)
        case d: Date => st.setDate(idx, d)
        case dt: Timestamp => st.setTimestamp(idx, dt)
        case d: java.util.Date => st.setDate(idx, new Date(d.getTime))
        case b: Boolean => st.setBoolean(idx, b)
        case o => st.setObject(idx, o)
      }
      idx + 1
    }
  }

}
