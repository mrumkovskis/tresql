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
    bindVariables: List[Expr], env: Env, allCols: Boolean): Result = {
    Env log sql
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    var i = 0
    val rs = st.executeQuery
    def rcol(c: QueryBuilder#ColExpr) = if (c.separateQuery) Column(-1, c.aliasOrName, c.col) else {
      i += 1; Column(i, c.aliasOrName, null)
    }
    val r = new Result(rs, Vector((if (!allCols) cols.map { rcol(_) }
    else cols.flatMap { c =>
      (if (c.col.isInstanceOf[QueryBuilder#AllExpr]) {
        var (j, md, l) = (1, rs.getMetaData, List[Column]()); val cnt = md.getColumnCount
        while (j <= cnt) { i += 1; l = Column(i, md.getColumnLabel(j), null) :: l; j += 1 }
        l.reverse
      } else List(rcol(c)))
    }): _*), env.reusableExpr)
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
    else {
      val conn = env.conn; conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
    }
  }

  private def bindVars(st: PreparedStatement, bindVariables: List[Expr]) {
    Env.log(bindVariables.map(_.toString).mkString("Bind vars: ", ", ", "\n"), 1)
    bindVariables.map(_()).zipWithIndex.foreach {
      case (null, idx) => st.setNull(idx + 1, java.sql.Types.VARCHAR)
      case (i: Int, idx) => st.setInt(idx + 1, i)
      case (l: Long, idx) => st.setLong(idx + 1, l)
      case (d: Double, idx) => st.setDouble(idx + 1, d)
      case (f: Float, idx) => st.setFloat(idx + 1, f)
      // Allow the user to specify how they want the Date handled based on the input type
      case (t: java.sql.Timestamp, idx) => st.setTimestamp(idx + 1, t)
      case (d: java.sql.Date, idx) => st.setDate(idx + 1, d)
      case (t: java.sql.Time, idx) => st.setTime(idx + 1, t)
      /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
* assume a Timestamp value */
      case (d: java.util.Date, idx) => st.setTimestamp(idx + 1, new java.sql.Timestamp(d.getTime))
      case (b: Boolean, idx) => st.setBoolean(idx + 1, b)
      case (s: String, idx) => st.setString(idx + 1, s)
      case (bn: java.math.BigDecimal, idx) => st.setBigDecimal(idx + 1, bn)
      case (bd: BigDecimal, idx) => st.setBigDecimal(idx + 1, bd.bigDecimal)
      case (obj, idx) => st.setObject(idx + 1, obj)
    }
  }
}
