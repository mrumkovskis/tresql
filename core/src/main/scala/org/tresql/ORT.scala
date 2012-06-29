package org.tresql

object ORT {
  
  def insert(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val insert = insert_tresql(name, obj, null)(resources)
    Env log insert
    Query.build(insert, resources, obj, false)()
  }
  def update(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {}
  def delete(name:String, id:Any):Any = {}
  def fill(name:String, obj:Map[String, _], fillNames: Boolean = false)
    (implicit resources:Resources = Env):Map[String, Any] = {
    Map(""->"")
  }

  def insert_tresql(name:String, obj:Map[String, _], parent:String)(resources:Resources):String = {
    //insert statement column, value map from obj
    obj map (t => {
      val tn = resources.tableName(name)
      val n = t._1
      val cn = resources.colName(name, n)
      val ptn = if (parent != null) resources.tableName(parent) else null
      t._2 match {
        //children
        case Seq(v:Map[String, _], _*) => insert_tresql(n, v, name)(resources) + " " + n->null
        case Array(v:Map[String, _], _*) => insert_tresql(n, v, name)(resources) + " " + n->null
        case v:Map[String, _] => insert_tresql(n, v, name)(resources) + " " + n->null
        //fill pk
        case null if (resources.metaData.table(tn).key == metadata.Key(List(cn))) => cn -> ("#" + tn)
        //fill fk
        case null if (parent != null & resources.metaData.table(tn).refTable.get(
          metadata.Ref(List(cn))) == Some(ptn)) => cn -> (":#" + ptn)
        case v => cn -> (":" + n)
    }})
  }.unzip match {
    case (cols:List[_], vals:List[_]) => cols.mkString("+" + resources.tableName(name) + 
        "{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]")
  }
}