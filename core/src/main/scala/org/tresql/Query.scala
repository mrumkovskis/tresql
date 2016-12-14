package org.tresql

import java.sql.{ Array => JArray }
import java.sql.{ Date, Timestamp, PreparedStatement, CallableStatement, ResultSet }
import org.tresql.metadata._
import sys._
import java.sql.Connection
import CoreTypes.RowConverter

trait Query extends QueryBuilder with TypedQuery {

  def apply(expr: String, params: Any*)(implicit resources: Resources = Env): DynamicResult =
    exec(expr, normalizePars(params: _*), resources).asInstanceOf[DynamicResult]

  def compiledResult[T <: RowLike](expr: String, params: Any*)(
    implicit resources: Resources = Env): CompiledResult[T] = {
    exec(expr, normalizePars(params: _*), resources).asInstanceOf[CompiledResult[T]]
  }

  private[tresql] def converters: Map[(Int, Int), RowConverter[_ <: RowLike]] = null

  private def exec(
    expr: String,
    params: Map[String, Any],
    resources: Resources
  ): Result[_] = {
    val builtExpr = build(expr, params, false)(resources)
    builtExpr() match {
      case r: CompiledResult[_] => r
      case r: Result[_] =>
        val builder = builtExpr.builder
        builder.env.rowConverter(builder.queryDepth, builder.childIdx).map { conv =>
          /*Convert result if converter is found and wrap it into {{{SingleValueResult}}}.
          For example this may happen when procedure is called directly (outside of select statement)
          and returns {{{org.tresql.Result}}} where instead some single value is expected
          (judging by function's signature)*/
          SingleValueResult(conv(r))
        } getOrElse (r)
      case x => SingleValueResult(x)
    }
  }

  def build(
    expr: String,
    params: Map[String, Any] = null,
    reusableExpr: Boolean = true
  )(implicit resources: Resources = Env): Expr = {
    Env log expr
    val pars =
      if (resources.params.isEmpty) params
      else if (params != null) resources.params ++ params else resources.params
    newInstance(new Env(pars, resources, reusableExpr), 0, 0, 0).buildExpr(expr)
  }

  /** QueryBuilder methods **/
  private[tresql] def newInstance(
    e: Env,
    depth: Int,
    idx: Int,
    chIdx: Int
  ) = {
    if (converters != null) e.rowConverters = converters
    new Query {
      override def env = e
      override private[tresql] def queryDepth = depth
      override private[tresql] var bindIdx = idx
      override private[tresql] def childIdx = chIdx
    }
  }

  def parse(expr: String) = QueryParser.parseExp(expr)

  private[tresql] def normalizePars(pars: Any*): Map[String, Any] = pars match {
    case Seq(m: Map[String @unchecked, Any @unchecked]) => m
    case l => l.zipWithIndex.map(t => (t._2 + 1).toString -> t._1).toMap
  }

  private[tresql] def sel(sql: String, cols: List[QueryBuilder#ColExpr]): Result[_ <: RowLike] = {
    val (rs, columns, visibleColCount) = sel_result(sql, cols)
    val result = env.rowConverter(queryDepth, childIdx).map { conv =>
      new CompiledSelectResult(rs, columns, env, sql,bindVariables,
        env.maxResultSize, visibleColCount, conv)
    }.getOrElse {
      new DynamicSelectResult(rs, columns, env, sql, bindVariables, env.maxResultSize, visibleColCount)
    }
    env.result = result
    result
  }

  private[this] def sel_result(sql: String, cols: List[QueryBuilder#ColExpr]):
    (ResultSet, Vector[Column], Int) = { //jdbc result, columns, visible column count
    val st = statement(sql, env)
    bindVars(st, bindVariables)
    var i = 0
    val rs = st.executeQuery
    val md = rs.getMetaData
    var visibleColCount = -1
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
    val columns =
      if (allCols) Vector(cols.flatMap { c =>
        if (c.col.isInstanceOf[QueryBuilder#AllExpr]) jdbcRcols else List(rcol(c))
      }: _*)
      else if (identAll) Vector(jdbcRcols ++ (cols.filter(_.separateQuery) map rcol) :_*)
      else rcols match {
        case (c, -1) => Vector(c: _*)
        case (c, s) =>
          visibleColCount = s
          Vector(c: _*)
      }
      (rs, columns, visibleColCount)
  }

  private[tresql] def update(sql: String) = {
    val st = statement(sql, env)
    try {
      bindVars(st, bindVariables)
      st.executeUpdate
    } finally if (!env.reusableExpr) {
      st.close
      env.statement = null
    }
  }

  private[tresql] def call(sql: String) = {
    val st = statement(sql, env, true).asInstanceOf[CallableStatement]
    var result: Any = null
    var outs: List[Any] = null
    try {
      bindVars(st, bindVariables)
      if (st.execute) {
        val rs = st.getResultSet
        val md = rs.getMetaData
        val res = env.rowConverter(queryDepth, childIdx).map { conv =>
          new CompiledSelectResult(
            rs,
            Vector(1 to md.getColumnCount map { i => Column(i, md.getColumnLabel(i), null) }: _*),
            env,
            sql,
            bindVariables,
            Env.maxResultSize,
            -1,
            conv)
        }.getOrElse {
          new DynamicSelectResult(
            rs,
            Vector(1 to md.getColumnCount map { i => Column(i, md.getColumnLabel(i), null) }: _*),
            env,
            sql,
            bindVariables,
            Env.maxResultSize
          )
        }
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
      case List(r: Result[_]) => r
      case l @ List(r: Result[_], x, _*) => l
      case null :: l => l
      case x => error("Knipis: " + x)
    }
  }

  private def statement(sql: String, env: Env, call: Boolean = false) = {
    Env log sql
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
