package org.tresql

/** Object Relational Transformations - ORT */
object ORT {
  
  def insert(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val insert = insert_tresql(name, obj, null, resources)
    Env log insert
    Query.build(insert, resources, obj, false)()
  }
  //TODO update where unique key (not only pk specified)
  def update(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {}
  def delete(name:String, id:Any):Any = {}
  def fill(name:String, obj:Map[String, _], fillNames: Boolean = false)
    (implicit resources:Resources = Env):Map[String, Any] = {
    Map(""->"")
  }

  def insert_tresql(name:String, obj:Map[String, _], parent:String, resources:Resources):String = {
    //insert statement column, value map from obj
    obj map (t => {
      val tn = resources.tableName(name)
      val n = t._1
      val cn = resources.colName(name, n)
      val ptn = if (parent != null) resources.tableName(parent) else null
      t._2 match {
        //children
        case Seq(v:Map[String, _], _*) => insert_tresql(n, v, name, resources)->null
        case Array(v:Map[String, _], _*) => insert_tresql(n, v, name, resources)->null
        case v:Map[String, _] => insert_tresql(n, v, name, resources)->null
        //fill pk
        case null if (resources.metaData.table(tn).key == metadata.Key(List(cn))) => cn -> ("#" + tn)
        //fill fk
        case null if (parent != null & resources.metaData.table(tn).refTable.get(
          metadata.Ref(List(cn))) == Some(ptn)) => cn -> (":#" + ptn)
        case v => cn -> (":" + n)
    }})
  }.unzip match {
    case (cols:List[_], vals:List[_]) => cols.mkString("+" + resources.tableName(name) + 
        "{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]") + 
        (if(parent != null) " " + name else "")
  }
  
  def update_tresql(name:String, obj:Map[String, _], parent:String, resources:Resources):String = {
    //update statement column, value map from obj
    obj map (t => {
      val tn = resources.tableName(name)
      val n = t._1
      val cn = resources.colName(name, n)
      val ptn = if (parent != null) resources.tableName(parent) else null
      t._2 match {
        //children
        case Seq(v:Map[String, _], _*) => insert_tresql(n, v, name, resources)->null
        case Array(v:Map[String, _], _*) => insert_tresql(n, v, name, resources)->null
        case v:Map[String, _] => insert_tresql(n, v, name, resources)->null
        //fill pk
        case null if (resources.metaData.table(tn).key == metadata.Key(List(cn))) => cn -> ("#" + tn)
        //fill fk
        case null if (parent != null & resources.metaData.table(tn).refTable.get(
          metadata.Ref(List(cn))) == Some(ptn)) => cn -> (":#" + ptn)
        case v => cn -> (":" + n)
    }})
  }.unzip match {
    case (cols:List[_], vals:List[_]) => cols.mkString("+" + resources.tableName(name) + 
        "{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]") + 
        (if(parent != null) " " + name else "")
  }

  def fill_tresql(name: String, obj: Map[String, _], parent: String, resources: Resources): String = {
    val table = resources.metaData.table(resources.tableName(name))
    var pk: String = null
    var pkVal: Any = null
    obj.map(t => {
      val n = t._1
      val cn = resources.colName(name, n)
      t._2 match {
        //primary key
        case v if (table.key == metadata.Key(List(cn))) => pk = cn; pkVal = v; pk
        //foreign key
        case v if (table.refTable.get(metadata.Ref(List(cn))) != None) => {
          val reft = table.refTable.get(metadata.Ref(List(cn))).get 
          resources.nameExpr(reft).map { nexp =>
            "|"
          }.getOrElse(v)
        }
        case Seq(v: Map[String, _], _*) => "|" + fill_tresql(n, v, name, resources)
        case Array(v: Map[String, _], _*) => "|" + fill_tresql(n, v, name, resources)
        case v: Map[String, _] => "|" + fill_tresql(n, v, name, resources)
      }
    })
    ""
  }
}