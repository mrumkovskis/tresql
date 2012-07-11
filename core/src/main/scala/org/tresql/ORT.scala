package org.tresql

import sys._

/** Object Relational Transformations - ORT */
object ORT {
  
  def insert(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val insert = insert_tresql(name, obj, null, resources)
    if(insert == null) error("Cannot insert data. Table not found for object: " + name)
    Env log insert
    Query.build(insert, resources, obj, false)()
  }
  //TODO update where unique key (not only pk specified)
  def update(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val update = update_tresql(name, obj, resources)
    if(update == null) error("Cannot update data. Table not found for object: " + name)
    Env log update
    Query.build(update, resources, obj, false)()    
  }
  def delete(name:String, id:Any)(implicit resources:Resources = Env):Any = {
    val delete = "-" + resources.tableName(name) + "[?]"
    Env log delete
    Query.build(delete, resources, Map("1"->id), false)()
  }
  /** 
   * Fills object properties specified by 'obj' parameter from table specified by 'name' parameter.
   * obj contains property names to be filled from database. obj map must contain primary key entry.
   * If no corresponding table column to the property is found property value in returned object
   * remains untouched.
   * 
   * Returned object contains exactly the same property set as the one passed as a parameter.
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
    def merge(obj: Map[String, _], res: Map[String, _]):Map[String, _] = {
      obj.map(t => t._1 -> res.get(t._1).map({
        case rs: Seq[Map[String, _]] => t._2 match {
          case os: Seq[Map[String, _]] => os.zipWithIndex.map(
            ost => rs.lift(ost._2).map(merge(ost._1, _)).getOrElse(ost._1))
          case oa: Array[Map[String, _]] => oa.zipWithIndex.map(
            ost => rs.lift(ost._2).map(merge(ost._1, _)).getOrElse(ost._1))
          case om: Map[String, _] => rs.map(merge(om, _))
          case ox => rs
        }
        case rm: Map[String, _] => t._2 match {
          case om: Map[String, _] => merge(om, rm)
          case ox => rm
        }
        case rx => rx
      }).getOrElse(t._2))
    }
    val fill = fill_tresql(name, obj, fillNames, resources)
    Env log fill
    merge(obj, Query.select(fill).toListRowAsMap.headOption.getOrElse(Map()))
  }

  def insert_tresql(name: String, obj: Map[String, _], parent: String, resources: Resources): String = {    
    //insert statement column, value map from obj
    val Array(objName, refPropName) = name.split(":").padTo(2, null)
    resources.metaData.tableOption(resources.tableName(objName)).map(table => {
      val ptn = if (parent != null) resources.tableName(parent) else null
      val refColName = if (parent == null) null else if (refPropName == null)
        if (table.refs(ptn).size != 1) null else table.refs(ptn)(0).cols(0)
        else resources.colName(objName, refPropName)
      obj.map(t => {
        val n = t._1
        val cn = resources.colName(objName, n)
        t._2 match {
          //children
          case Seq() | Array() => (null, null)
          case Seq(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case Array(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case v: Map[String, _] => (insert_tresql(n, v, objName, resources), null)
          //fill pk
          case null if (table.key == metadata.Key(List(cn))) => cn -> ("#" + table.name)
          //fill fk
          case null if ((refPropName != null && refPropName == n) ||
              (refPropName == null && refColName == cn)) => cn -> (":#" + ptn)
          case v => (table.cols.get(cn).map(_.name).orNull, ":" + n)
        }
      }).filter(_._1 != null).unzip match {
        case (cols: List[_], vals: List[_]) => cols.mkString("+" + table.name +
          "{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]") +
          (if (parent != null) " '" + name + "'" else "")
      }
    }).orNull
  }

  //TODO support child merge update i.e. instead of deleting and inserting all children,
  //separately delete removed children, insert new children and update existing children.
  def update_tresql(name: String, obj: Map[String, _], resources: Resources): String = {
    resources.metaData.tableOption(resources.tableName(name)).flatMap(table => {
      var pkProp: Option[String] = None
      //stupid thing - map find methods conflicting from different traits 
      def findPkProp(objName:String) = pkProp.orElse {
        pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
          n => table.key == metadata.Key(List(resources.colName(objName, n._1)))).map(_._1)
        pkProp
      }
      def childUpdates(n:String, o:Map[String, _]) = {
        val Array(objName, refPropName) = n.split(":").padTo(2, null)
        resources.metaData.tableOption(resources.tableName(objName)).flatMap(childTable => 
          findPkProp(objName).flatMap (pk => {
            Some(childDeletes(childTable, pk, (if(refPropName == null) null 
                else resources.colName(objName, refPropName))) + (if (o == null) "" else ", " +
                insert_tresql(n, o, null, resources) + " '" + n + "'"))
        })).orNull
      }
      def childDeletes(childTable: metadata.Table, pkCol: String, refCol: String) = {
        "-" + childTable.name + "[" + (if (refCol != null) refCol
            else importedKey(table.name, childTable)) + " = :" + pkCol + "]"
      }
      obj.map(t => {
        val n = t._1
        val cn = resources.colName(name, n)
        if (table.key == metadata.Key(List(cn))) pkProp = Some(n)
        t._2 match {
          //children
          case Seq() | Array() => (childUpdates(n, null), null)
          case Seq(v: Map[String, _], _*) => (childUpdates(n, v), null)
          case Array(v: Map[String, _], _*) => (childUpdates(n, v), null)
          case v: Map[String, _] => (childUpdates(n, v), null)
          //do not update pkProp
          case v if (Some(n) == pkProp) => (null, null)
          case v => (table.cols.get(cn).map(_.name).orNull, ":" + n)
        }
      }).filter(_._1 != null).unzip match {
        case (cols: List[_], vals: List[_]) => pkProp.flatMap(pk =>
          Some(cols.mkString("=" + table.name + "[" + ":" + pk + "]" + "{", ", ", "}") + 
            vals.filter(_ != null).mkString(" [", ", ", "]")))
      }
    }).orNull
  }

  def fill_tresql(name: String, obj: Map[String, _], fillNames:Boolean, resources: Resources,
      ids:Option[Seq[_]] = None, parent:Option[(/*objName*/String, /*pkPropName*/String)] = None): String = {
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
        case _ => nameExpr
      }
    }
    val Array(objName, refPropName) = name.split(":").padTo(2, null)
    val table = resources.metaData.table(resources.tableName(objName))
    var pkProp: Option[String] = None
    //stupid thing - map find methods conflicting from different traits 
    def findPkProp(objName: String) = pkProp.orElse {
      pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
        n => table.key == metadata.Key(List(resources.colName(objName, n._1)))).map(_._1)
      pkProp
    }
    var filterCol: String = parent.map(p => if (refPropName != null) 
      resources.colName(objName, refPropName) else importedKey(resources.tableName(p._1), table)).orNull
    var filterVal:Any = if(filterCol != null) ":1('" + parent.get._2 + "')" else null
    obj.flatMap(t => {
      val n = t._1
      val cn = resources.colName(objName, n)
      val refTable = table.refTable.get(metadata.Ref(List(cn)))
      t._2 match {
        //pk
        case v if (table.key == metadata.Key(List(cn))) => {
          if (filterCol == null && v != null) {
            //set filter column if primary key set
            filterCol = cn; filterVal = v
          }
          pkProp = Some(n)
          List(filterCol + " " + n)
        }
        //foreign key
        case v if (refTable != None) => {
          (cn + " " + n) :: (if (fillNames) resources.propNameExpr(objName, n).orElse(
              resources.nameExpr(refTable.get)).map(ne => 
              List("|" + nameExpr(refTable.get, n, ne) + " " + n + "_name")).getOrElse(Nil) else Nil)
        }
        //children
        case s:Seq[Map[String, _]] if(s.size > 0) => {
          List(resources.metaData.tableOption(resources.tableName(n)).map(cht=>
          "|" + fill_tresql(n, s(0), fillNames, resources, Some(s.flatMap(_.filter(t=>
            metadata.Key(List(resources.colName(cht.name, t._1))) == 
              cht.key).map(_._2)))) + " " + n).orNull)
        }
        //children
        case a:Array[Map[String, _]] if(a.size > 0) => {
          List(resources.metaData.tableOption(resources.tableName(n)).map(cht=>
          "|" + fill_tresql(n, a(0), fillNames, resources, Some(a.flatMap(_.filter(t=>
            metadata.Key(List(resources.colName(cht.name, t._1))) == 
              cht.key).map(_._2)))) + " " + n).orNull)
        }
        case v: Map[String, _] => {
          val Array(on, rpn) = n.split(":").padTo(2, null)
          List(resources.metaData.tableOption(resources.tableName(on)).flatMap(cht=>findPkProp.map().orNull)
        } 
        case _ => List(table.cols.get(cn).map(_.name + " " + n).orNull)
      }
    }).filter(_ != null).mkString(table.name +
        "[" + filterCol + " in[" + ids.map(_.mkString(", ")).getOrElse(filterVal) + "]]{", ", ", "}")
  }

  private def importedKey(tableName: String, childTable: metadata.Table) = {
    val refs = childTable.refs(tableName)
    if (refs.size != 1) error("Cannot update child table '" + childTable.name +
      "'. Must be exactly one reference from child to parent table '" + tableName +
      "'. Instead these refs found: " + refs)
    refs(0).cols(0)
  }
  
}