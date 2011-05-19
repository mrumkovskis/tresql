package uniso.query

/* Environment for expression building and execution */
class Env(private val provider: EnvProvider, private val resourceProvider: ResourceProvider,
  val reusableExpr: Boolean) extends (String => Any) with MetaData {

  private var providedEnvs: List[Env] = Nil

  if (provider != null) {
    def rootEnv(e: Env): Env = if (e.provider == null) e else rootEnv(e.provider.env)
    val root = rootEnv(this)
    root.providedEnvs = this :: root.providedEnvs
  }

  private val vars = new ThreadLocal[scala.collection.mutable.Map[String, Any]]
  private val res = new ThreadLocal[Result]
  private val st = new ThreadLocal[java.sql.PreparedStatement]

  def this(provider: EnvProvider, reusableExpr: Boolean) = this(provider, null, reusableExpr)
  def this(vars: Map[String, Any], resourceProvider: ResourceProvider, reusableExpr: Boolean) = {
    this(null.asInstanceOf[EnvProvider], resourceProvider, reusableExpr)
    update(vars)
  }

  def apply(name: String) = if (provider != null) provider.env(name) else vars.get()(name) match {
    case e: Expr => e()
    case x => x
  }

  override implicit def conn = if (provider != null) provider.env.conn else resourceProvider.conn

  def dbName = if (provider != null) provider.env.dbName else resourceProvider.metaData.dbName

  override def table(name: String)(implicit conn: java.sql.Connection) = if (provider != null)
    provider.env.table(name) else resourceProvider.metaData.table(name)(conn)

  def apply(rIdx: Int) = {
    var i = 0
    var e: Env = this
    while (i < rIdx && e != null) {
      if (e.provider != null) e = e.provider.env else e = null
      i += 1
    }
    if (i == rIdx && e != null) e.res.get else error("Result not available at index: " + rIdx)
  }

  def statement = this.st.get

  def contains(name: String): Boolean = if (provider != null) provider.env.contains(name)
  else vars.get.contains(name)

  def update(name: String, value: Any) {
    if (provider != null) provider.env(name) = value else this.vars.get()(name) = value
  }

  def update(vars: Map[String, Any]) {
    if (provider != null) provider.env.update(vars)
    else this.vars set scala.collection.mutable.Map(vars.toList: _*)
  }

  def update(r: Result) = this.res set r
  def update(st: java.sql.PreparedStatement) = this.st set st

  def closeStatement {
    val st = this.st.get
    if (st != null) {
      this.st set null
      st.close
    }
    this.providedEnvs foreach (_.closeStatement)
  }

}

object Env extends ResourceProvider {
  private var logger: (String, Int) => Unit = null
  //meta data object must be thread safe!
  private var md: MetaData = null
  private val threadConn = new ThreadLocal[java.sql.Connection]
  var motherConn: java.sql.Connection = null
  def apply(params: Map[String, String], reusableExpr: Boolean): Env = {
    new Env(params mapValues (Query(_)), Env, reusableExpr)
  }
  def apply(params: Map[String, Any], reusableExpr: Boolean, parseParams: Boolean): Env = {
    if (parseParams) apply(params.asInstanceOf[Map[String, String]], reusableExpr)
    else new Env(params, Env, reusableExpr)
  }
  def conn = { val c = threadConn.get; if (c == null) motherConn else c }
  def metaData = md
  def update(md: MetaData) = this.md = md
  def update(conn: java.sql.Connection) = this.threadConn set conn
  def update(logger: (String, Int) => Unit) = this.logger = logger
  def log(msg: String, level: Int = 0): Unit = if (logger != null) logger(msg, level)
}

trait EnvProvider {
  def env: Env
}

trait ResourceProvider {
  def conn: java.sql.Connection
  def metaData: MetaData
}