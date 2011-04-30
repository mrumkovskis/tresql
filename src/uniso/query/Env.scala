package uniso.query

class Env(private val md: MetaData, private val provider: EnvProvider)
  extends (String => Any) with MetaData {

  private val vars: ThreadLocal[scala.collection.mutable.Map[String, Any]] = new ThreadLocal
  private val connection: ThreadLocal[java.sql.Connection] = new ThreadLocal
  private val res: ThreadLocal[Result] = new ThreadLocal

  def this(provider: EnvProvider) = this(null, provider)
  def this(vars: Map[String, Any], md: MetaData, conn: java.sql.Connection) = {
    this(md, null)
    update(vars)
    update(conn)
  }

  def apply(name: String) = if (provider != null) provider.env(name) else vars.get()(name) match {
    case e: Expr => e()
    case x => x
  }
  
  override implicit def conn = if (provider != null) provider.env.conn else connection.get
  
  def dbName = if (provider != null) provider.env.dbName else md.dbName
  
  override def table(name: String)(implicit conn: java.sql.Connection) = if (provider != null)
    provider.env.table(name) else md.table(name)(conn)
    
  def apply(rIdx: Int) = {
    var i = 0
    var e: Env = this
    while (i < rIdx && e != null) {
      if (e.provider != null) e = e.provider.env else e = null
      i += 1
    }
    if (i == rIdx && e != null) e.res.get else error("Result not available at index: " + rIdx)
  }

  def contains(name: String): Boolean = if (provider != null) provider.env.contains(name)
  else vars.get.contains(name)
  
  def update(name: String, value: Any) {
    if (provider != null) provider.env(name) = value else this.vars.get()(name) = value
  }
  
  def update(vars: Map[String, Any]) {
    if (provider != null) provider.env.update(vars)
    else this.vars set scala.collection.mutable.Map(vars.toList: _*)
  }
  
  def update(conn: java.sql.Connection) {
    if (provider != null) provider.env.update(conn)
    else this.connection set conn
  }
  
  def update(r: Result) = this.res set r
}

object Env {
  private var md: MetaData = null
  private val conn: ThreadLocal[java.sql.Connection] = new ThreadLocal
  def apply(params: Map[String, String]): Env = {
    new Env(params mapValues (Query(_)), md, conn.get)
  }
  def apply(params: Map[String, Any], parseParams: Boolean): Env = {
    if (parseParams) apply(params.asInstanceOf[Map[String, String]])
    else new Env(params, md, conn.get)
  }
  def update(md: MetaData) = this.md = md
  def update(conn: java.sql.Connection) = this.conn set conn
}

trait EnvProvider {
  def env: Env
}