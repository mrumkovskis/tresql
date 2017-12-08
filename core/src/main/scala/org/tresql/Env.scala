package org.tresql

import sys._
import CoreTypes.RowConverter

/* Environment for expression building and execution */
class Env(_provider: EnvProvider, resources: Resources, val reusableExpr: Boolean)
  extends Resources with Metadata {

  def this(provider: EnvProvider, reusableExpr: Boolean) = this(provider, Env, reusableExpr)
  def this(resources: Resources, reusableExpr: Boolean) = this(null: EnvProvider, resources, reusableExpr)
  def this(params: Map[String, Any], resources: Resources, reusableExpr: Boolean) = {
    this(null: EnvProvider, resources, reusableExpr)
    update(params)
  }

  //provided envs are used for statement closing. this list is filled only if provider is not set.
  //NOTE: list contains also this environment
  private var providedEnvs: List[Env] = Nil
  //is package private since is accessed from QueryBuilder
  private[tresql] val provider: Option[EnvProvider] = Option(_provider)

  private def rootEnv(e: Env): Env = e.provider.map(p=>rootEnv(p.env)).getOrElse(e)
  private val root = rootEnv(this)
  root.providedEnvs = this :: root.providedEnvs

  private var vars: Option[scala.collection.mutable.Map[String, Any]] = None
  private var _exprs: Option[Map[Expr, Int]] = None
  private val ids = scala.collection.mutable.Map[String, Any]()
  private var _result: Result[_ <: RowLike] = _
  private var _statement: java.sql.PreparedStatement = _
  //stores row count returned by SelectResult and all subresults.
  //if resources.maxResultSize is greater than zero
  //Row count is accumulated only for top level Env i.e. provider is None
  private var _rowCount = 0
  //used in macro to convert result at certain query depth and child position to macro generated object
  //converter map is set from macro and are stored in level Env object i.e. provider is None
  private var _rowConverters: Option[Map[(Int /*query depth*/,
    Int /*col idx*/), RowConverter[_ <: RowLike]]] = None

  def apply(name: String): Any = get(name).map {
    case e: Expr => e()
    case x => x
  } getOrElse (throw new MissingBindVariableException(name))

  def get(name: String): Option[Any] =
    vars.flatMap(_.get(name)) orElse provider.flatMap(_.env.get(name))

  /* if not found into this variable map look into provider's if such exists */
  def contains(name: String): Boolean =
    vars.map(_.contains(name))
     .filter(_ == true)
     .getOrElse(provider.exists(_.env.contains(name)))

  /* finds closest env with vars map set (Some(vars)) and looks there if variable exists */
  def containsNearest(name: String): Boolean =
    vars.map(_.contains(name)).getOrElse(provider.exists(_.env.containsNearest(name)))

  private[tresql] def update(name: String, value: Any) {
    vars.map(_(name) = value) orElse provider.map(_.env(name) = value)
  }

  private[tresql] def update(vars: Map[String, Any]) {
    this.vars = if (vars == null) None else Some(scala.collection.mutable.Map(vars.toList: _*))
  }

  private [tresql] def updateExprs(exprs: Map[Expr, Int]) = _exprs = Option(exprs)

  def apply(rIdx: Int): Result[_ <: RowLike] = {
    var i = 0
    var e: Env = this
    while (i < rIdx && e != null) {
      e = e.provider.map(_.env).orNull
      i += 1
    }
    if (i == rIdx && e != null) e.result else error("Result not available at index: " + rIdx)
  }

  def apply(expr: Expr): Int = _exprs.map(_.getOrElse(expr,
      error(s"Expression not found in env: $this. Expr: $expr"))).getOrElse(
          error(s"Expression '$expr' not found. Hidden column expressions not set in environment: $this"))

  private[tresql] def statement = _statement
  private[tresql] def statement_=(st: java.sql.PreparedStatement) = _statement = st

  private[tresql] def result = _result
  private[tresql] def result_=(r: Result[_ <: RowLike]) = _result = r

  private[tresql] def closeStatement {
    root.providedEnvs foreach (e=> if (e.statement != null) e.statement.close)
  }

  private[tresql] def nextId(seqName: String): Any = {
    import CoreTypes._
    //TODO perhaps built expressions can be used to improve performance?
    val id = Query.unique[Any](resources.idExpr(seqName))
    ids(seqName) = id
    id
  }
  private[tresql] def currId(seqName: String): Any = (ids.get(seqName) orElse provider.map(_.env.currId(seqName))).get
  private[tresql] def currIdOption(seqName: String): Option[Any] =
    ids.get(seqName) orElse provider.flatMap(_.env.currIdOption(seqName))
  //update current id. This is called from QueryBuilder.IdExpr
  private[tresql] def currId(seqName: String, id: Any): Unit = ids(seqName) = id

  private[tresql] def rowCount: Int = provider.map(_.env.rowCount).getOrElse(_rowCount)
  private[tresql] def rowCount_=(rc: Int) {
    provider.map(_.env.rowCount = rc).getOrElse (_rowCount = rc)
  }

  private[tresql] def rowConverter(depth: Int, child: Int): Option[RowConverter[_ <: RowLike]] =
    rowConverters.flatMap(_.get((depth, child)))
  private[tresql] def rowConverters: Option[Map[(Int /*query depth*/,
    Int /*child idx*/), RowConverter[_ <: RowLike]]] =
    provider.flatMap(_.env.rowConverters) orElse _rowConverters
  private[tresql] def rowConverters_=(rc: Map[(Int /*query depth*/,
    Int /*child idx*/), RowConverter[_ <: RowLike]]) {
    provider.map(_.env.rowConverters = rc).getOrElse (_rowConverters = Option(rc))
  }

  //resources methods
  def conn: java.sql.Connection = provider.map(_.env.conn).getOrElse(resources.conn)
  override def metadata = provider.map(_.env.metadata).getOrElse(resources.metadata)
  override def dialect: CoreTypes.Dialect = provider.map(_.env.dialect).getOrElse(resources.dialect)
  override def idExpr = provider.map(_.env.idExpr).getOrElse(resources.idExpr)
  override def queryTimeout = provider.map(_.env.queryTimeout).getOrElse(resources.queryTimeout)
  override def maxResultSize = provider.map(_.env.maxResultSize).getOrElse(resources.maxResultSize)

  //meta data methods
  override def table(name: String) = metadata.table(name)
  override def tableOption(name:String) = metadata.tableOption(name)
  override def procedure(name: String) = metadata.procedure(name)
  override def procedureOption(name:String) = metadata.procedureOption(name)

  def printVariables = "\nBind variables:" +
    vars.map(_.mkString("\n ", "\n ", "\n")).getOrElse("<none>")
  def printAllVariables = "\nBind variables:" + printVars(" ")
  private def printVars(offset: String): String =
    vars.map(_.mkString("\n" + offset, "\n" + offset, "\n")).
      getOrElse("\n" + offset + "<none>\n") +
      provider.map(_.env.printVars(offset + " ")).getOrElse("")
  override def toString: String = super.toString +
    provider.map(p=> s":$p#${p.env.toString}").getOrElse("<no provider>")

}

object Env extends Resources {
  private val threadConn = new ThreadLocal[java.sql.Connection]
  //query timeout
  private val query_timeout = new ThreadLocal[Int]
  private val threadLogLevel = new ThreadLocal[Option[Int]] {
    override def initialValue = None
  }
  //this is for single thread usage
  var sharedConn: java.sql.Connection = _
  //meta data object must be thread safe!
  private var _metadata: Option[Metadata] = Some(org.tresql.metadata.JDBCMetadata())
  private var _dialect: Option[CoreTypes.Dialect] = None
  private var _idExpr: Option[String => String] = None
  //available functions
  private var _functions: Option[Any] = None
  private var _functionMethods: Option[Map[String, java.lang.reflect.Method]] = None
  functions = new Functions //invoke setter to set function names
  //macros
  private var _macros: Option[Any] = None
  private var _macrosMethods: Option[Map[String, java.lang.reflect.Method]] = None
  //cache
  private var _cache: Option[Cache] = None
  //logger
  private var _logger: (=> String, Int) => Unit = _
  //loggable bind variable filter
  private var _bindVarLogFilter: Option[PartialFunction[Expr, String]] = Some({
    case v: QueryBuilder#VarExpr if v.name == "password" => v.fullName + " = [FILTERED]"
    case x => x.toString
  })
  //recursive execution depth
  private var _recursive_stack_depth = 50
  private var _maxResultSize = 0

  def apply(params: Map[String, Any], reusableExpr: Boolean) = new Env(params, this, reusableExpr)
  def conn = { val c = threadConn.get; if (c == null) sharedConn else c }
  override def metadata = _metadata.get
  override def dialect = _dialect.getOrElse(super.dialect)
  override def idExpr = _idExpr.getOrElse(super.idExpr)
  def functions = _functions
  def isDefined(functionName: String) = _functionMethods.exists(_.contains(functionName))
  def function(name: String) = _functionMethods.map(_(name)).get
  def macros = _macros
  def isMacroDefined(macroName: String) = _macrosMethods.exists(_.contains(macroName))
  def macroMethod(name: String) = _macrosMethods.map(_(name)).get
  def cache = _cache
  override def queryTimeout = query_timeout.get
  override def maxResultSize = _maxResultSize

  def conn_=(conn: java.sql.Connection) = this.threadConn set conn
  def metadata_=(metadata: Metadata) = this._metadata = Option(metadata)
  def dialect_=(dialect: CoreTypes.Dialect) = this._dialect =
    Option(dialect).map(_.orElse {case e=> e.defaultSQL})
  def idExpr_=(idExpr: String => String) = this._idExpr = Option(idExpr)
  def functions_=(funcs: Any) = {
    this._functions = Option(funcs)
    this._functionMethods = functions.flatMap(f => Option(f.getClass.getMethods.map(m => m.getName -> m).toMap))
  }
  def macros_=(macr: Any) = {
    this._macros = Option(macr)
    this._macrosMethods = macros.flatMap(f => Option(f.getClass.getMethods.map(m => m.getName -> m).toMap))
  }

  def recursive_stack_dept = _recursive_stack_depth
  def recursive_stack_dept_=(depth: Int) = _recursive_stack_depth = depth

  def cache_=(cache: Cache) = this._cache = Option(cache)
  def queryTimeout_=(timeout: Int) = this.query_timeout set timeout

  def maxResultSize_=(size: Int) = this._maxResultSize = size

  def log(msg: => String, level: Int = 0): Unit = if (_logger != null) _logger(msg, level)
  def logger = _logger
  def logger_=(logger: (=> String, Int) => Unit) = this._logger = logger
  def bindVarLogFilter = _bindVarLogFilter
  def bindVarLogFilter_=(filter: PartialFunction[Expr, String]) = Option(filter)
}

trait Resources { self =>
  private case class Resources_(
    _conn: Option[java.sql.Connection] = None,
    _metadata: Option[Metadata] = None,
    _dialect: Option[CoreTypes.Dialect] = None,
    _idExpr: Option[String => String] = None,
    _queryTimeout: Option[Int] = None,
    _maxResultSize: Option[Int] = None,
    _params: Option[Map[String, Any]] = None) extends Resources {
    override def conn: java.sql.Connection = _conn getOrElse self.conn
    override def metadata: Metadata = _metadata getOrElse self.metadata
    override def dialect: CoreTypes.Dialect = _dialect getOrElse self.dialect
    override def idExpr: String => String = _idExpr getOrElse self.idExpr
    override def queryTimeout: Int = _queryTimeout getOrElse self.queryTimeout
    override def maxResultSize: Int = _maxResultSize getOrElse self.maxResultSize
    override def params: Map[String, Any] = _params getOrElse self.params
    override def toString = s"Resources_(conn = $conn, " +
      s"metadata = $metadata, dialect = $dialect, idExpr = $idExpr, " +
      s"queryTimeout = $queryTimeout, maxResultSize = $maxResultSize, params = $params)"
  }

  private var _valueExprMap: Option[Map[(String, String), String]] = None

  def conn: java.sql.Connection
  def metadata: Metadata
  def dialect: CoreTypes.Dialect = null
  def idExpr: String => String = s => "nextval('" + s + "')"
  def queryTimeout = 0
  def maxResultSize = 0
  def params: Map[String, Any] = Map()

  /** Column value expression in tresql statement value clause.
   *  Default is named bind variable - {{{:columnName}}} */
  def valueExpr(tableName: String, columnName: String) =
    _valueExprMap.flatMap(_.get((tableName, columnName)))
      .getOrElse(":" + columnName)

  /** Set value expr map
   * key: table name -> column name, value: expr passed to tresql
   */
  def updateValueExprs(map: Map[(String, String), String]) = _valueExprMap = Option(map)

  //resource construction convenience methods
  def withConn(conn: java.sql.Connection): Resources = Resources_(_conn = Option(conn))
  def withMetadata(metadata: Metadata): Resources = Resources_(_metadata = Option(metadata))
  def withDialect(dialect: CoreTypes.Dialect): Resources = Resources_(_dialect = Option(dialect))
  def withIdExpr(idExpr: String => String): Resources = Resources_(_idExpr = Option(idExpr))
  def withQueryTimeout(queryTimeout: Int): Resources = Resources_(_queryTimeout = Option(queryTimeout))
  def withMaxResultSize(maxResultSize: Int): Resources = Resources_(_maxResultSize = Option(maxResultSize))
  def withParams(params: Map[String, Any]): Resources = Resources_(_params = Option(params))
}

trait EnvProvider {
  private[tresql] def env: Env
}

class MissingBindVariableException(val name: String)
  extends RuntimeException(s"Missing bind variable: $name")
