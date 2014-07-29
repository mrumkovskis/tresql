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

  def apply(name: String): Any = vars.map(_(name)) getOrElse provider.get.env(name) match {
    case e: Expr => e()
    case x => x
  }
  
  def get(name: String): Option[Any] = vars.flatMap(_.get(name)) orElse provider.flatMap(
      _.env.get(name)).map {
    case e: Expr => e()
    case x => x    
  }

  def contains(name: String): Boolean = vars.map(_.contains(name)) getOrElse
    provider.map(_.env.contains(name)).getOrElse(false)

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
  //update current id. This is called from QueryBuilder.IdExpr
  private[tresql] def currId(seqName: String, id: Any): Unit =
    provider.map(_.env.currId(seqName, id)).getOrElse(ids(seqName) = id)
  
  //resources methods
  def conn: java.sql.Connection = provider.map(_.env.conn).getOrElse(resources.conn)
  override def metaData = provider.map(_.env.metaData).getOrElse(resources.metaData)
  override def dialect: PartialFunction[Expr, String] = provider.map(_.env.dialect).getOrElse(resources.dialect)
  override def idExpr = provider.map(_.env.idExpr).getOrElse(resources.idExpr)
  
  //meta data methods
  def table(name: String) = metaData.table(name)
  def tableOption(name:String) = metaData.tableOption(name)
  def procedure(name: String) = metaData.procedure(name)
  def procedureOption(name:String) = metaData.procedureOption(name)
}

object Env extends Resources {
  private val threadConn = new ThreadLocal[java.sql.Connection]
  //this is for single thread usage
  var sharedConn: java.sql.Connection = null
  //meta data object must be thread safe!
  private var _metaData: Option[MetaData] = Some(metadata.JDBCMetaData())
  private var _dialect: Option[PartialFunction[Expr, String]] = None
  private var _idExpr: Option[String => String] = None
  //available functions
  private var _functions: Option[Any] = None
  private var _functionNames: Option[Set[String]] = None
  functions = new Functions //invoke setter to set function names
  //macros
  private var _macros: Option[Any] = None
  private var _macrosNames: Option[Set[String]] = None
  //cache
  private var _cache: Option[Cache] = None
  private var logger: (=> String, Int) => Unit = null
  
  def apply(params: Map[String, Any], reusableExpr: Boolean) = new Env(params, this, reusableExpr)
  def conn = { val c = threadConn.get; if (c == null) sharedConn else c }
  override def metaData = _metaData.get
  override def dialect = _dialect.getOrElse(super.dialect)
  override def idExpr = _idExpr.getOrElse(super.idExpr)
  def functions = _functions
  def isDefined(functionName: String) = _functionNames.map(_.contains(functionName)).getOrElse(false)
  def macros = _macros
  def isMacroDefined(macroName: String) = _macrosNames.map(_.contains(macroName)).getOrElse(false)
  def cache = _cache
  
  def conn_=(conn: java.sql.Connection) = this.threadConn set conn
  def metaData_=(metaData: MetaData) = this._metaData = Option(metaData)
  def dialect_=(dialect: PartialFunction[Expr, String]) = this._dialect =
    Option(dialect).map(_.orElse {case e=> e.defaultSQL})
  def idExpr_=(idExpr: String => String) = this._idExpr = Option(idExpr)
  def functions_=(funcs: Any) = {
    this._functions = Option(funcs)
    this._functionNames = functions.flatMap(f => Option(f.getClass.getMethods.map(_.getName).toSet))
  }
  def macros_=(macr: Any) = {
    this._macros = Option(macr)
    this._macrosNames = macros.flatMap(f => Option(f.getClass.getMethods.map(_.getName).toSet))
  }
  
  def cache_=(cache: Cache) = this._cache = Option(cache)
  
  def log(msg: => String, level: Int = 0): Unit = if (logger != null) logger(msg, level)
  def update(logger: (=> String, Int) => Unit) = this.logger = logger
}

trait Resources extends NameMap {
  private var _nameMap:Option[(Map[String, String], Map[String, Map[String, (String, String)]])] = None
  private var _delegateNameMap:Option[NameMap] = None

  def conn: java.sql.Connection
  def metaData: MetaData
  def dialect: PartialFunction[Expr, String] = null
  def idExpr: String => String = s => "nextval('" + s + "')"
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