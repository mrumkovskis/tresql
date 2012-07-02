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
  
  private var providedEnvs: List[Env] = Nil
  private val provider: Option[EnvProvider] = if(_provider == null) None else Some(_provider)

  if (provider != null) {
    def rootEnv(e: Env): Env = e.provider.map(p=>rootEnv(p.env)).getOrElse(e)
    val root = rootEnv(this)
    root.providedEnvs = this :: root.providedEnvs
  }
  
  private var vars: Option[scala.collection.mutable.Map[String, Any]] = None
  private val ids = scala.collection.mutable.Map[String, Any]()
  private var _result: Result = null
  private var _statement: java.sql.PreparedStatement = null

  def apply(name: String):Any = vars.map(_(name)) getOrElse provider.get.env(name) match {
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
  
  def apply(rIdx: Int) = {
    var i = 0
    var e: Env = this
    while (i < rIdx && e != null) {
      e = e.provider.map(_.env).orNull
      i += 1
    }
    if (i == rIdx && e != null) e.result else error("Result not available at index: " + rIdx)
  }

  private[tresql] def statement = _statement
  private[tresql] def statement_=(st: java.sql.PreparedStatement) = _statement = st

  private[tresql] def result = _result
  private[tresql] def result_=(r: Result) = _result = r 
    
  private[tresql] def closeStatement {
    if (statement != null) statement.close
    this.providedEnvs foreach (_.closeStatement)
  }
  
  private[tresql] def nextId(seqName: String): Any = provider.map(_.env.nextId(seqName)).getOrElse {
    //TODO perhaps built expressions can be used?
    val id = Query.unique[Any](resources.idExpr(seqName))
    ids(seqName) = id
    id
  }
  private[tresql] def currId(seqName: String): Any = provider.map(_.env.currId(seqName)).getOrElse(ids(seqName))
  
  //resources methods
  def conn: java.sql.Connection = provider.map(_.env.conn).getOrElse(resources.conn)
  override def metaData = provider.map(_.env.metaData).getOrElse(resources.metaData)
  override def dialect: Expr => String = provider.map(_.env.dialect).getOrElse(resources.dialect)
  override def idExpr = provider.map(_.env.idExpr).getOrElse(resources.idExpr)
  
  //meta data methods
  def dbName = metaData.dbName
  def table(name: String) = metaData.table(name)
  def procedure(name: String) = metaData.procedure(name)
  
}

object Env extends Resources {
  private val threadConn = new ThreadLocal[java.sql.Connection]
  //this is for single thread usage
  var sharedConn: java.sql.Connection = null
  //meta data object must be thread safe!
  private var _metaData: Option[MetaData] = None
  private var _dialect: Option[Expr => String] = None
  private var _idExpr: Option[String => String] = None
  //available functions
  private var functions = Functions.getClass.getDeclaredMethods map (_.getName) toSet

  private var logger: (=> String, Int) => Unit = null
  
  def apply(params: Map[String, Any], reusableExpr: Boolean) = new Env(params, this, reusableExpr)
  def conn = { val c = threadConn.get; if (c == null) sharedConn else c }
  override def metaData = _metaData.getOrElse(super.metaData)
  override def dialect = _dialect.getOrElse(super.dialect)
  override def idExpr = _idExpr.getOrElse(super.idExpr)
  
  def conn_=(conn: java.sql.Connection) = this.threadConn set conn
  def metaData_=(metaData: MetaData) = this._metaData = Some(metaData)
  def dialect_=(dialect: Expr => String) = this._dialect = Some(dialect)
  def idExpr_=(idExpr: String => String) = this._idExpr = Some(idExpr)
  
  def availableFunctions(list: Traversable[String]) = functions = list.toSet
  def isDefined(functionName: String) = functions.contains(functionName) 
  
  def log(msg: => String, level: Int = 0): Unit = if (logger != null) logger(msg, level)
  def update(logger: (=> String, Int) => Unit) = this.logger = logger
}

trait Resources extends NameMap {
  private val _metaData = metadata.JDBCMetaData("")
  //1. map: object name -> table name, 2. map: object name -> (property name -> column name), 3. map: object name -> name tresql expression
  private var _nameMap:Option[(Map[String, String], Map[String, Map[String, String]], Map[String, String])] = None
  def conn: java.sql.Connection
  def metaData: MetaData = _metaData
  def dialect: Expr => String = null
  def idExpr: String => String = s => "nextval('" + s + "')"
  def nameMap:NameMap = this
  //name map methods
  override def tableName(objectName:String):String = _nameMap.flatMap(_._1.get(objectName)).getOrElse(
      super.tableName(objectName))
  override def colName(objectName: String, propertyName: String): String = 
    _nameMap.flatMap(_._2.get(objectName).flatMap(_.get(propertyName))).getOrElse(super.colName(
        objectName, propertyName))
  override def nameExpr(objectName:String):Option[String] = _nameMap.flatMap(_._3.get(objectName))
  //set name map
  def update(map:(Map[String, String], Map[String, Map[String, String]], Map[String, String])) = 
    _nameMap = Some(map)
}

trait NameMap {
  def tableName(objectName:String):String = objectName
  def colName(objectName:String, propertyName:String):String = propertyName
  /** TreSQL expression returning table record "name". Typically it is a concatenation of row
   * fields plus fields from joined (lookup) tables. Expression must contain at least one
   * column with a name "name" and one unnamed binding variable.
   * Example - emp table name:
   * emp/dept[empno = ?]{ename + ', ' + dname 'name'}*/
  def nameExpr(objectName:String):Option[String] = None  
}

trait EnvProvider {
  def env: Env
}