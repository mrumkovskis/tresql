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

  def fill_tresql(name: String, obj: Map[String, _], fillNames:Boolean, resources: Resources,
      parent:String, ids:Seq[_]): String = {
    def nameExpr(table:String, idVar:String, nameExpr:String) = {
      import QueryParser._
      parseExp(nameExpr) match {
        //replace existing filter with pk filter
        case q:Query => (q.copy(filter = parseExp("[" + ":" + idVar + "]").asInstanceOf[Arr]) match {
          //set default aliases for query columns
          case x@Query(_, _, List(Col(name, null)), _, _, _, _, _) => 
            x.copy(cols = List(Col(name, "\"name\"")))
          case x@Query(_, _, cols@List(Col(id, null), Col(name, null)), _, _, _, _, _) =>
            x.copy(cols = List(Col(name, "\"name\"")))
          case x@Query(_, _, cols@List(Col(id, null), Col(code, null), Col(name, null)), _, _, _, _, _) =>
            x.copy(cols = List(Col(code, "\"code\""), Col(name, "\"name\"")))
          case x => x
        }).tresql + " '" + idVar + "_name'"
        case a:Arr => table + "[" + ":" + idVar + "]" + (a match {
          case Arr(List(name:Exp)) => "{" + name.tresql + " 'name'" + "}"
          case Arr(List(id:Exp, name:Exp)) => "{" + name.tresql + " 'name'" + "}"
          case Arr(List(id:Exp, code:Exp, name:Exp)) => "{" + code.tresql + " 'code', " +
            name.tresql + " 'name'" + "}"
          case _ => a.elements.map(any2tresql(_)).mkString("{", ", ", "}") 
        })
      }
    }
    val table = resources.metaData.table(resources.tableName(name))
    var pk: String = null
    var pkVal: Any = null
    obj.flatMap(t => {
      val n = t._1
      val cn = resources.colName(name, n)
      t._2 match {
        //primary key
        case v if (table.key == metadata.Key(List(cn))) => pk = cn; pkVal = v; List(pk + " '" + n + "'")
        //foreign key
        case v if (table.refTable.get(metadata.Ref(List(cn))) != None) => {
          val reft = table.refTable.get(metadata.Ref(List(cn))).get
          (cn + " '" + n + "'") :: (if (fillNames)
            resources.nameExpr(reft).map(ne => 
              List("|" + nameExpr(reft, n, ne) + " '" + n + "_name'")).getOrElse(Nil) else Nil)
        }
        case s @ Seq(o:Map[String, _], _*) => s.map(fill(n, _, fillNames)(resources))
        case a @ Array(o:Map[String, _], _*) => a.map(fill(n, _, fillNames)(resources))
        case v: Map[String, _] => null
        case _ => List(cn + " '" + n + "'")
      }
    })
    ""
  }
}