package org.tresql

import java.sql.{ Array => JArray }
import java.sql.{ Date, Timestamp, PreparedStatement, CallableStatement, ResultSet }
import org.tresql.metadata._
import sys._
import java.sql.Connection

trait Query extends TypedQuery {

  def apply(expr: String, params: Any*)(implicit resources: Resources = Env): Result =
    exec(expr, normalizePars(params: _*), resources)

  private def exec(expr: String, params: Map[String, Any], resources: Resources): Result =
    build(expr, params, false)(resources)() match {
      case r: Result => r
      case x => SingleValueResult(x)
    }
    
  def build(expr: String, params: Map[String, Any] = null, reusableExpr: Boolean = true)(
    implicit resources: Resources = Env): Expr = {
    Env log expr
    QueryBuilder(expr, new Env(params, resources, reusableExpr))
  }
  
  def parse(expr: String) = QueryParser.parseExp(expr)

  private[tresql] def normalizePars(pars: Any*): Map[String, Any] = pars match {
    case Seq(m: Map[String, Any]) => m
    case l => l.zipWithIndex.map(t => (t._2 + 1).toString -> t._1).toMap
  }

  private[tresql] def sel(sql: String, cols: List[QueryBuilder#ColExpr],
    bindVariables: List[Expr], env: Env, allCols: Boolean, identAll: Boolean,
    hasHiddenCols: Boolean): Result = {
    Env log sql
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    var i = 0
    val rs = st.executeQuery
    val md = rs.getMetaData
    def jdbcRcols = (1 to md.getColumnCount).foldLeft(List[Column]()) {
        (l, j) => i += 1; Column(i, md.getColumnLabel(j), null) :: l
      } reverse
    def rcol(c: QueryBuilder#ColExpr) = if (c.separateQuery) Column(-1, c.name, c.col) else {
      i += 1; Column(i, c.name, null)
    }
    def rcols = if (hasHiddenCols /*cols.exists(_.hidden)*/) {
      val res = cols.zipWithIndex.foldLeft((List[Column](), Map[Expr, Int](), 0)) {
        (r, c) => (rcol(c._1) :: r._1,
            if (c._1.hidden) r._2 + (c._1.col -> c._2) else r._2,
            if (!c._1.hidden) r._3 + 1 else r._3)
      }
      env.updateExprs(res._2)
      (res._1.reverse, res._3)
    } else (cols map rcol, -1)
    
    val result = if (allCols) new SelectResult(rs, Vector(cols.flatMap {c => 
      if (c.col.isInstanceOf[QueryBuilder#AllExpr]) jdbcRcols else List(rcol(c))
    }: _*), env)
    else if (identAll) new SelectResult(rs,
        Vector(jdbcRcols ++ (cols.filter(_.separateQuery) map rcol) :_*), env)
    else rcols match {
      case (c, -1) => new SelectResult(rs, Vector(c: _*), env)
      case (c, s) => new SelectResult(rs, Vector(c: _*), env, s)
    }
    env.result = result
    result
  }

  private[tresql] def update(sql: String, bindVariables: List[Expr], env: Env) = {
    Env log sql
    val st = statement(sql, env)
    try {
      bindVars(st, bindVariables)
      st.executeUpdate
    } finally if (!env.reusableExpr) {
      st.close
      env.statement = null
    }
  }

  private[tresql] def call(sql: String, bindVariables: List[Expr], env: Env) = {
    Env log sql
    val st = statement(sql, env, true).asInstanceOf[CallableStatement]
    var result: Any = null
    var outs: List[Any] = null
    try {
      bindVars(st, bindVariables)
      if (st.execute) {
        val rs = st.getResultSet
        val md = rs.getMetaData
        val res = new SelectResult(rs, Vector(1 to md.getColumnCount map
          { i => Column(i, md.getColumnLabel(i), null) }: _*), env)
        env.result = res
        result = res
      }
      outs = bindVariables map (_()) filter (_.isInstanceOf[OutPar]) map { x =>
        val p = x.asInstanceOf[OutPar]
        p.value = p.value match {
          case null => st.getObject(p.idx)
          case i: Int =>
            val x = st.getInt(p.idx); if (st.wasNull) null else x
          case l: Long =>
            val x = st.getLong(p.idx); if (st.wasNull) null else x
          case d: Double =>
            val x = st.getDouble(p.idx); if (st.wasNull) null else x
          case f: Float =>
            val x = st.getFloat(p.idx); if (st.wasNull) null else x
          // Allow the user to specify how they want the Date handled based on the input type
          case t: java.sql.Timestamp => st.getTimestamp(p.idx)
          case d: java.sql.Date => st.getDate(p.idx)
          case t: java.sql.Time => st.getTime(p.idx)
          /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
* assume a Timestamp value */
          case d: java.util.Date => st.getTimestamp(p.idx)
          case b: Boolean => st.getBoolean(p.idx)
          case s: String => st.getString(p.idx)
          case bn: java.math.BigDecimal => st.getBigDecimal(p.idx)
          case bd: BigDecimal => val x = st.getBigDecimal(p.idx); if (st.wasNull) null else BigDecimal(x)
        }
        p.value
      }
    } finally if (result == null && !env.reusableExpr) {
      st.close
      env.statement = null
    }
    result :: outs match {
      case null :: Nil => ()
      case List(r: Result) => r
      case l @ List(r: Result, x, _*) => l
      case null :: l => l
      case x => error("Knipis: " + x)
    }
  }

  private def statement(sql: String, env: Env, call: Boolean = false) = {
    val conn = env.conn
    if (conn == null) throw new NullPointerException(
      """Connection not found in environment. Check if "Env.conn = conn" (in this case statement execution must be done in the same thread) or "Env.sharedConn = conn" is called.""")
    val st = if (env.reusableExpr)
      if (env.statement == null) {
        val s = if (call) conn.prepareCall(sql) else {
          conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        }
        env.statement = s
        s
      } else env.statement
    else if (call) conn.prepareCall(sql)
    else conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if (env.queryTimeout == 0) st else {
      st.setQueryTimeout(env.queryTimeout)
      st
    }
  }

  private def bindVars(st: PreparedStatement, bindVariables: List[Expr]) {
    Env.log(bindVariables.map(_.toString).mkString("Bind vars: ", ", ", ""), 1)
    var idx = 1
    def bindVar(p: Any) {
      try p match {
        case null => st.setNull(idx, java.sql.Types.NULL)
        case i: Int => st.setInt(idx, i)
        case l: Long => st.setLong(idx, l)
        case d: Double => st.setDouble(idx, d)
        case f: Float => st.setFloat(idx, f)
        case i: java.lang.Integer => st.setInt(idx, i)
        case l: java.lang.Long => st.setLong(idx, l)
        case d: java.lang.Double => st.setDouble(idx, d)
        case f: java.lang.Float => st.setFloat(idx, f)
        // Allow the user to specify how they want the Date handled based on the input type
        case t: java.sql.Timestamp => st.setTimestamp(idx, t)
        case d: java.sql.Date => st.setDate(idx, d)
        case t: java.sql.Time => st.setTime(idx, t)
        /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
* assume a java.sql.Date value */
        case d: java.util.Date => st.setTimestamp(idx, new java.sql.Timestamp(d.getTime))
        case c: java.util.Calendar => st.setTimestamp(idx, new java.sql.Timestamp(c.getTime.getTime))
        case b: Boolean => st.setBoolean(idx, b)
        case b: java.lang.Boolean => st.setBoolean(idx, b)
        case s: String => st.setString(idx, s)
        case bn: java.math.BigDecimal => st.setBigDecimal(idx, bn)
        case bd: BigDecimal => st.setBigDecimal(idx, bd.bigDecimal)
        case in: java.io.InputStream => st.setBinaryStream(idx, in)
        case bl: java.sql.Blob => st.setBlob(idx, bl)
        case rd: java.io.Reader => st.setCharacterStream(idx, rd)
        case cl: java.sql.Clob => st.setClob(idx, cl)
        case ab: Array[Byte] => st.setBytes(idx, ab)
        //array binding
        case i: scala.collection.Traversable[_] => i foreach (bindVar(_)); idx -= 1
        case a: Array[_] => a foreach (bindVar(_)); idx -= 1
        case p@InOutPar(v) => {
          bindVar(v)
          idx -= 1
          registerOutPar(st.asInstanceOf[CallableStatement], p, idx)
        }
        //OutPar must be matched bellow InOutPar since it is superclass of InOutPar
        case p@OutPar(_) => registerOutPar(st.asInstanceOf[CallableStatement], p, idx)
        //unknown object
        case Some(o) => bindVar(o); idx -= 1
        case None => bindVar(null); idx -= 1
        case obj => st.setObject(idx, obj)
      } catch {
        case e:Exception => throw new RuntimeException("Failed to bind variable at index " +
            (idx - 1) + ". Value: " + (String.valueOf(p) match {
              case x if x.length > 100 => x.substring(0, 100) + "..."
              case x => x 
            }) + " of class " + (if (p == null) "null" else p.getClass),e)
      }
      idx += 1
    }
    bindVariables.map(_()).foreach { bindVar(_) }
  }
  private def registerOutPar(st: CallableStatement, par: OutPar, idx: Int) {
    import java.sql.Types._
    par.idx = idx
    par.value match {
      case null => st.registerOutParameter(idx, NULL)
      case i: Int => st.registerOutParameter(idx, INTEGER)
      case l: Long => st.registerOutParameter(idx, BIGINT)
      case d: Double => st.registerOutParameter(idx, DECIMAL)
      case f: Float => st.registerOutParameter(idx, DECIMAL)
      case i: java.lang.Integer => st.registerOutParameter(idx, INTEGER)
      case l: java.lang.Long => st.registerOutParameter(idx, BIGINT)
      case d: java.lang.Double => st.registerOutParameter(idx, DECIMAL)
      case f: java.lang.Float => st.registerOutParameter(idx, DECIMAL)
      // Allow the user to specify how they want the Date handled based on the input type
      case t: java.sql.Timestamp => st.registerOutParameter(idx, TIMESTAMP)
      case d: java.sql.Date => st.registerOutParameter(idx, DATE)
      case t: java.sql.Time => st.registerOutParameter(idx, TIME)
      /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
* assume a Timestamp value */
      case d: java.util.Date => st.registerOutParameter(idx, TIMESTAMP)
      case b: Boolean => st.registerOutParameter(idx, BOOLEAN)
      case b: java.lang.Boolean => st.registerOutParameter(idx, BOOLEAN)
      case s: String => st.registerOutParameter(idx, VARCHAR)
      case bn: java.math.BigDecimal => st.registerOutParameter(idx, DECIMAL, bn.scale)
      case bd: BigDecimal => st.registerOutParameter(idx, DECIMAL, bd.scale)
      //unknown object
      case obj => st.registerOutParameter(idx, OTHER)
    }
  }

}

/** Out parameter box for callable statement */
class OutPar(var value: Any) {
  private[tresql] var idx = 0
  def this() = this(null)
  override def toString = "OutPar(" + value + ")"
}
object OutPar {
  def apply() = new OutPar()
  def apply(value: Any) = new OutPar(value)
  def unapply(par: OutPar): Option[Any] = Some(par.value)
}

/** In out parameter box for callable statement */
class InOutPar(v: Any) extends OutPar(v) {
  override def toString = "InOutPar(" + value + ")"
}
object InOutPar {
  def apply(value: Any) = new InOutPar(value)
  def unapply(par: InOutPar): Option[Any] = Some(par.value)
}

object Query extends Query
