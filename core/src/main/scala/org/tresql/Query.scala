package org.tresql

import java.sql.{ Array => JArray }
import java.sql.{ Date, Timestamp, PreparedStatement, ResultSet }
import org.tresql.metadata._
import sys._

object Query {

  def apply(expr: String, params: Any*): Any = {
    apply(expr, params.toList)
  }

  def apply(expr: String, params: List[Any]): Any = {
    apply(expr, params.zipWithIndex.map(t => (t._2 + 1).toString -> t._1).toMap)
  }

  def apply(expr: String, params: Map[String, Any]): Any = {
    val exp = QueryBuilder(expr, Env(params, false))
    exp()
  }

  def apply(expr: Any) = {
    val exp = QueryBuilder(expr, Env(Map(), false))
    exp()
  }

  def parse(expr: String) = QueryParser.parseAll(expr)
  def build(expr: String): Expr = QueryBuilder(expr, Env(Map(), true))
  def build(expr: Any): Expr = QueryBuilder(expr, Env(Map(), true))

  def select(expr: String, params: Any*) = {
    apply(expr, params.toList).asInstanceOf[Result]
  }

  def select(expr: String, params: List[Any]) = {
    apply(expr, params).asInstanceOf[Result]
  }

  def select(expr: String, params: Map[String, Any]) = {
    apply(expr, params).asInstanceOf[Result]
  }

  def select(expr: String) = {
    apply(expr).asInstanceOf[Result]
  }

  def foreach(expr: String, params: Any*)(f: (RowLike) => Unit = (row) => ()) {
    (if (params.size == 1) params(0) match {
      case l: List[_] => select(expr, l)
      case m: Map[String, _] => select(expr, m)
      case x => select(expr, x)
    }
    else select(expr, params)) foreach f
  }

  def head[T](expr: String, params: Any*)(conv: Any => T = (x: Any) => x.asInstanceOf[T]): T = {
    if (params.size == 1) params(0) match {
      case l: List[_] => conv(typedRow[T](select(expr, l), HEAD))
      case m: Map[String, _] => conv(typedRow[T](select(expr, m), HEAD))
      case x => conv(typedRow[T](select(expr, x), HEAD))
    }
    else conv(typedRow[T](select(expr, params), HEAD))
  }
  def headOption[T](expr: String, params: Any*)(conv: Any => T = (x: Any) => x.asInstanceOf[T]): Option[T] = {
    if (params.size == 1) params(0) match {
      case l: List[_] => Some(conv(typedRow(select(expr, l), HEAD_OPTION)))
      case m: Map[String, _] => Some(conv(typedRow(select(expr, m), HEAD_OPTION)))
      case x => Some(conv(typedRow(select(expr, x), HEAD_OPTION)))
    }
    else Some(conv(typedRow[T](select(expr, params), HEAD_OPTION)))
  }
  def unique[T](expr: String, params: Any*)(conv: Any => T = (x: Any) => x.asInstanceOf[T]): T = {
    if (params.size == 1) params(0) match {
      case l: List[_] => conv(typedRow[T](select(expr, l), UNIQUE))
      case m: Map[String, _] => conv(typedRow[T](select(expr, m), UNIQUE))
      case x => conv(typedRow[T](select(expr, x), UNIQUE))
    }
    else conv(typedRow[T](select(expr, params), UNIQUE))
  }

  private val HEAD = 1
  private val HEAD_OPTION = 2
  private val UNIQUE = 3
  private def typedRow[T](r: Result, mode: Int) = {
    mode match {
      case HEAD | HEAD_OPTION => {
        r hasNext match {
          case true => r next; val v = r(0); r close; v
          case false => if (mode == HEAD) error("No rows in result") else None
        }
      }
      case UNIQUE => r hasNext match {
        case true =>
          r next; val v = r(0); if (r hasNext) {
            r.close; error("More than one row for unique result")
          } else v
        case false => error("No rows in result")
      }
      case x => error("Knipis: " + x)
    }
  }

  private[tresql] def select(sql: String, cols: List[QueryBuilder#ColExpr],
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
        var (md, l) = (rs.getMetaData, List[Column]())
        1 to md.getColumnCount foreach { j => i += 1; l = Column(i, md.getColumnLabel(j), null) :: l }
        l.reverse
      } else List(rcol(c)))
    }): _*), env)
    env update r
    r
  }

  private[tresql] def update(sql: String, bindVariables: List[Expr], env: Env) = {
    Env log sql
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    val r = st.executeUpdate
    if (!env.reusableExpr) {
      st.close
      env update (null: PreparedStatement)
    }
    r
  }

  private def statement(sql: String, env: Env) = {
    val conn = env.conn
    if (conn == null) throw new NullPointerException(
      """Connection not found in environment. Check if "Env update conn" (in this case statement execution must be done in the same thread) or "Env.sharedConn = conn" is called.""")
    if (env.reusableExpr)
      if (env.statement == null) {
        val s = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY); env.update(s); s
      } else env.statement
    else {
      val conn = env.conn; conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
    }
  }

  private def bindVars(st: PreparedStatement, bindVariables: List[Expr]) {
    Env.log(bindVariables.map(_.toString).mkString("Bind vars: ", ", ", ""), 1)
    var idx = 1
    def bindVar(p: Any) {
      p match {
        case null => st.setNull(idx, java.sql.Types.NULL)
        case i: Int => st.setInt(idx, i)
        case l: Long => st.setLong(idx, l)
        case d: Double => st.setDouble(idx, d)
        case f: Float => st.setFloat(idx, f)
        // Allow the user to specify how they want the Date handled based on the input type
        case t: java.sql.Timestamp => st.setTimestamp(idx, t)
        case d: java.sql.Date => st.setDate(idx, d)
        case t: java.sql.Time => st.setTime(idx, t)
        /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
* assume a Timestamp value */
        case d: java.util.Date => st.setTimestamp(idx, new java.sql.Timestamp(d.getTime))
        case b: Boolean => st.setBoolean(idx, b)
        case s: String => st.setString(idx, s)
        case bn: java.math.BigDecimal => st.setBigDecimal(idx, bn)
        case bd: BigDecimal => st.setBigDecimal(idx, bd.bigDecimal)
        //array binding
        case i: scala.collection.Traversable[_] => i foreach (bindVar(_)); idx -= 1
        case a: Array[_] => a foreach (bindVar(_)); idx -= 1
        //unknown object
        case obj => st.setObject(idx, obj)
      }
      idx += 1
    }
    bindVariables.map(_()).foreach { bindVar(_) }
  }
}
