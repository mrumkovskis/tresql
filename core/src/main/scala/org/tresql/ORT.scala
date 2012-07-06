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
  def delete(name:String, id:Any)(implicit resources:Resources = Env):Any = {
    val delete = "-" + resources.tableName(name) + "[?]"
    Env log delete
    Query.build(delete, resources, Map("1"->id), false)()
  }
  /** 
   * Fills object properties specified by parameter obj from table specified by parameter name.
   * obj contains table field names to be filled. obj map must contain primary key entry.
   * 
   * If parameter fillNames is true, foreign key properties are resolved to names using
   * resources.nameExpr(tableName). Name expression column aliases are modified according to
   * the following rules:
   *     1. if name expression contains one column without column alias column alias is set to "name"
   *     2. if name expression contains two columns without column aliases, second column is taken
   *        for the name and alias is set to "name". (first column is interpreted as a primary key) 
   *     3. if name expression contains three columns without column aliases, second column is taken
   *        for the code and alias is set to "code", third column is taken for the name and alias
   *        is set to "name". (first column is interpreted as a primary key)
   */
  def fill(name:String, obj:Map[String, _], fillNames: Boolean = false)
    (implicit resources:Resources = Env):Map[String, Any] = {
    val fill = fill_tresql(name, obj, fillNames, resources)
    Env log fill
    Query.select(fill).toListRowAsMap.headOption.getOrElse(Map())
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
      ids:Option[Seq[_]] = None): String = {
    def nameExpr(table:String, idVar:String, nameExpr:String) = {
      import QueryParser._
      parseExp(nameExpr) match {
        //replace existing filter with pk filter
        case q:Query => (q.copy(filter = parseExp("[" + ":1('" + idVar + "')]").asInstanceOf[Arr]) match {
          //set default aliases for query columns
          case x@Query(_, _, List(Col(name, null)), _, _, _, _, _) => 
            x.copy(cols = List(Col(name, "name")))
          case x@Query(_, _, List(Col(id, null), Col(name, null)), _, _, _, _, _) =>
            x.copy(cols = List(Col(name, "name")))
          case x@Query(_, _, List(Col(id, null), Col(code, null), Col(name, null)), _, _, _, _, _) =>
            x.copy(cols = List(Col(code, "code"), Col(name, "name")))
          case x => x
        }).tresql
        case a:Arr => table + "[" + ":1('" + idVar + "')]" + (a match {
          case Arr(List(name:Exp)) => "{" + name.tresql + " name" + "}"
          case Arr(List(id:Exp, name:Exp)) => "{" + name.tresql + " name" + "}"
          case Arr(List(id:Exp, code:Exp, name:Exp)) => "{" + code.tresql + " code, " +
            name.tresql + " name" + "}"
          case _ => a.elements.map(any2tresql(_)).mkString("{", ", ", "}") 
        })
      }
    }
    val table = resources.metaData.table(resources.tableName(name))
    var pk:String = null
    var pkVal:Any = null
    obj.flatMap(t => {
      val n = t._1
      val cn = resources.colName(name, n)
      t._2 match {
        //primary key
        case v if (table.key == metadata.Key(List(cn))) => pk = cn; pkVal = v; List(pk + " " + n)
        //foreign key
        case v if (table.refTable.get(metadata.Ref(List(cn))) != None) => {
          val reft = table.refTable.get(metadata.Ref(List(cn))).get
          (cn + " " + n) :: (if (fillNames) resources.nameExpr(reft).map(ne => 
              List("|" + nameExpr(reft, n, ne) + " " + n + "_name")).getOrElse(Nil) else Nil)
        }
        //children
        case s:Seq[Map[String, _]] if(s.size > 0) => {
          val childTable = resources.metaData.table(resources.tableName(n))
          List("|" + fill_tresql(n, s(0), fillNames, resources, Some(s.flatMap(_.filter(t=>
            metadata.Key(List(resources.colName(childTable.name, t._1))) == 
              childTable.key).map(_._2)))) + " " + n)
        }
        //children
        case a:Array[Map[String, _]] if(a.size > 0) => {
          val childTable = resources.metaData.table(resources.tableName(n))
          List("|" + fill_tresql(n, a(0), fillNames, resources, Some(a.flatMap(_.filter(t=>
            metadata.Key(List(resources.colName(childTable.name, t._1))) == 
              childTable.key).map(_._2)))) + " " + n)
        }
        case v: Map[String, _] => error("not supported yet") 
        case _ => List(cn + " " + n)
      }
    }).mkString(table.name + "[" + pk + " in[" + ids.map(_.mkString(", ")).getOrElse(pkVal) +
        "]]{", ", ", "}")
  }
}