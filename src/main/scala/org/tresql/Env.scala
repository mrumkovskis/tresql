package org.tresql

import sys._

/* Environment for expression building and execution */
class Env(_provider: EnvProvider, resources: Resources, val reusableExpr: Boolean)
  extends Resources with MetaData {

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
  private var _result: Result = null
  private var _statement: java.sql.PreparedStatement = null

  def apply(name: String): Any = get(name).map {
    case e: Expr => e()
    case x => x    
  } getOrElse (throw new MissingBindVariableException(name))
  
  def get(name: String) = vars.flatMap(_.get(name)) orElse provider.map(_.env(name))

  /* if not found into this variable map look into provider's if such exists */
  def contains(name: String): Boolean =
    vars.map(_.contains(name))
     .filter(_ == true)
     .getOrElse(provider.map(_.env.contains(name)).getOrElse(false))
  
  /* finds closest env with vars map set (Some(vars)) and looks there if variable exists */
  def containsNearest(name: String): Boolean =
    vars.map(_.contains(name)).getOrElse(provider.map(_.env.containsNearest(name)).getOrElse(false))

  private[tresql] def update(name: String, value: Any) {
    vars.get(name) = value
  }

  private[tresql] def update(vars: Map[String, Any]) {
    this.vars = if (vars == null) None else Some(scala.collection.mutable.Map(vars.toList: _*))
  }
  
  private [tresql] def updateExprs(exprs: Map[Expr, Int]) = _exprs = Option(exprs)
  
  def apply(rIdx: Int): Result = {
    var i = 0
    var e: Env = this
    while (i < rIdx && e != null) {
      e = e.provider.map(_.env).orNull
      i += 1
    }
    if (i == rIdx && e != null) e.result else error("Result not available at index: " + rIdx)
  }

  def apply(expr: Expr): Int = _exprs.map(_.getOrElse(expr,
      error("Expression not found in env: " + this + ". Expr: " + expr))).getOrElse(
          error("Hidden column expressions not set in evnvironment: " + this))

  private[tresql] def statement = _statement
  private[tresql] def statement_=(st: java.sql.PreparedStatement) = _statement = st

  private[tresql] def result = _result
  private[tresql] def result_=(r: Result) = _result = r 
    
  private[tresql] def closeStatement {
    root.providedEnvs foreach (e=> if (e.statement != null) e.statement.close)
  }
  
  private[tresql] def nextId(seqName: String): Any = provider.map(_.env.nextId(seqName)).getOrElse {
    //TODO perhaps built expressions can be used to improve performance?
    val id = Query.unique[Any](resources.idExpr(seqName))
    ids(seqName) = id
    id
  }
  private[tresql] def currId(seqName: String): Any = provider.map(_.env.currId(seqName)).getOrElse(ids(seqName))
  private[tresql] def currIdOption(seqName: String): Option[Any] =
    provider.map(_.env.currIdOption(seqName)).getOrElse(ids.get(seqName))
  //update current id. This is called from QueryBuilder.IdExpr
  private[tresql] def currId(seqName: String, id: Any): Unit =
    provider.map(_.env.currId(seqName, id)).getOrElse(ids(seqName) = id)
  
  //resources methods
  def conn: java.sql.Connection = provider.map(_.env.conn).getOrElse(resources.conn)
  override def metaData = provider.map(_.env.metaData).getOrElse(resources.metaData)
  override def dialect: PartialFunction[Expr, String] = provider.map(_.env.dialect).getOrElse(resources.dialect)
  override def idExpr = provider.map(_.env.idExpr).getOrElse(resources.idExpr)
  override def queryTimeout = provider.map(_.env.queryTimeout).getOrElse(resources.queryTimeout)
  
  //meta data methods
  def table(name: String) = metaData.table(name)
  def tableOption(name:String) = metaData.tableOption(name)
  def procedure(name: String) = metaData.procedure(name)
  def procedureOption(name:String) = metaData.procedureOption(name)
}

object Env extends Resources {
  private val threadConn = new ThreadLocal[java.sql.Connection]
  private val threadLogLevel = new ThreadLocal[Option[Int]] {
    override def initialValue = None
  }
  //this is for single thread usage
  var sharedConn: java.sql.Connection = null
  //meta data object must be thread safe!
  private var _metaData: Option[MetaData] = Some(metadata.JDBCMetaData())
  private var _dialect: Option[PartialFunction[Expr, String]] = None
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
  private var logger: (=> String, Int) => Unit = null
  //query timeout
  private var query_timeout = 0
  //recursive execution depth
  private var _recursive_stack_depth = 50
  
  def apply(params: Map[String, Any], reusableExpr: Boolean) = new Env(params, this, reusableExpr)
  def conn = { val c = threadConn.get; if (c == null) sharedConn else c }
  override def metaData = _metaData.get
  override def dialect = _dialect.getOrElse(super.dialect)
  override def idExpr = _idExpr.getOrElse(super.idExpr)
  def functions = _functions
  def isDefined(functionName: String) = _functionMethods.map(_.contains(functionName)).getOrElse(false)
  def function(name: String) = _functionMethods.map(_(name)).get
  def macros = _macros
  def isMacroDefined(macroName: String) = _macrosMethods.map(_.contains(macroName)).getOrElse(false)
  def macroMethod(name: String) = _macrosMethods.map(_(name)).get
  def cache = _cache
  override def queryTimeout = query_timeout
  
  def conn_=(conn: java.sql.Connection) = this.threadConn set conn
  def metaData_=(metaData: MetaData) = this._metaData = Option(metaData)
  def dialect_=(dialect: PartialFunction[Expr, String]) = this._dialect =
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
  def queryTimeout_=(timeout: Int) = this.query_timeout = timeout
  
  def logLevel = threadLogLevel.get
  def logLevel_=(level: Any) = level match {
    case l: Int => threadLogLevel.set(Some(l))
    case None | null => threadLogLevel.set(None)
    case l: Option[Int] => threadLogLevel.set(l)
  }
  
  def log(msg: => String, level: Int = 0): Unit = if (logger != null) logger(msg,
      level + logLevel.getOrElse(0))
  def update(logger: (=> String, Int) => Unit) = this.logger = logger
}

trait Resources extends NameMap {
  private var _nameMap:Option[(Map[String, String], Map[String, Map[String, (String, String)]])] = None
  private var _delegateNameMap:Option[NameMap] = None

  def conn: java.sql.Connection
  def metaData: MetaData
  def dialect: PartialFunction[Expr, String] = null
  def idExpr: String => String = s => "nextval('" + s + "')"
  def queryTimeout = 0
  //name map methods
  override def tableName(objectName: String): String = _delegateNameMap.map(_.tableName(
      objectName)).getOrElse(_nameMap.flatMap(_._1.get(objectName)).getOrElse(objectName))
  override def colName(objectName: String, propertyName: String): String = _delegateNameMap.map(
      _.colName(objectName, propertyName)).getOrElse(_nameMap.flatMap(_._2.get(objectName).flatMap(
          _.get(propertyName))).map(_._1).getOrElse(propertyName))
  override def valueExpr(objectName: String, propertyName: String) = _delegateNameMap.map(
      _.valueExpr(objectName, propertyName)).getOrElse(_nameMap.flatMap(_._2.get(objectName).flatMap(
          _.get(propertyName))).map(_._2).getOrElse(super.valueExpr(objectName, propertyName)))

  def nameMap = _delegateNameMap getOrElse this
  def nameMap_=(map:NameMap) = _delegateNameMap = Option(map).filter(_ != this)
  /** Set name map for this NameMap implementation
   * 1. map: object name -> table name,
   * 2. map: object name -> map: (property name -> (column name -> column value clause)),
   */
  def update(map:(Map[String, String], Map[String, Map[String, (String, String)]])) =
    _nameMap = Option(map)
}

trait NameMap {
  def tableName(objectName:String):String = objectName
  def colName(objectName:String, propertyName:String):String = propertyName
  /** Column value expression in tresql statement value clause.
   *  Default is named bind variable - {{{:propertyName}}} */
  def valueExpr(objectName: String, propertyName: String) = ":" + propertyName  
}

trait EnvProvider {
  def env: Env
}

class MissingBindVariableException(val name: String)
  extends RuntimeException(s"Missing bind variable: $name")