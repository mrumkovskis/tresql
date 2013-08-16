package org.tresql

import scala.collection.immutable.ListMap
import sys._

/** Object Relational Transformations - ORT */
object ORT {
  
  /** <object name | property name>[:<linked property name>][#(insert | update | delete)] */
  val PROP_PATTERN = """(\w+)(:(\w+))?(#(\w+))?"""r
  
  def insert(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val insert = insert_tresql(name, obj, null, resources)
    if(insert == null) error("Cannot insert data. Table not found for object: " + name)
    Env log insert
    Query.build(insert, resources, obj, false)()
  }
  //TODO update where unique key (not only pk specified)
  def update(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val update = update_tresql(name, obj, resources)
    if(update == null) error("Cannot update data. Table not found or primary key not found " +
    		"for the object: " + name)
    Env log update
    Query.build(update, resources, obj, false)()    
  }
  /**
   * Saves object obj specified by parameter name. If object primary key is set object
   * is updated, if object primary key is not set object is inserted. Children are merged
   * with database i.e. new ones are inserted, existing ones updated, deleted ones deleted.
   * Children structure i.e. property set must be identical, since one tresql statement is used
   * for all of the children
   */
  def save(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {
    val (save, saveable) = save_tresql(name, obj, resources)
    Env log save
    Env log saveable.toString
    Query.build(save, resources, saveable, false)()
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
   * If no object is found by primary key None is returned.
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
    (implicit resources:Resources = Env):Option[Map[String, Any]] = {
    def merge(obj: Map[String, _], res: Map[String, _]):Map[String, _] = {
      obj.map((t: (String, _)) => t._1 -> res.get(t._1).map({
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
    Query.select(fill).toListRowAsMap.headOption.map(r=> merge(obj, r))
  }

  def insert_tresql(name: String, obj: Map[String, _], parent: String, resources: Resources): String = {    
    //insert statement column, value map from obj
    val Array(objName, refPropName) = name.split(":").padTo(2, null)
    resources.metaData.tableOption(resources.tableName(objName)).map(table => {
      val ptn = if (parent != null) resources.tableName(parent) else null
      val refColName = if (parent == null) null else if (refPropName == null)
        table.refs(ptn) match {
          case Nil => null
          case List(ref) => ref.cols(0)
          case x => error("Ambiguous references to table: " + ptn + ". Refs: " + x)
      } else resources.colName(objName, refPropName)
      obj.map((t: (String, _)) => {
        val n = t._1
        val cn = resources.colName(objName, n)
        t._2 match {
          //children
          case Seq() | Array() => (null, null)
          case Seq(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case Array(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case v: Map[String, _] => (insert_tresql(n, v, objName, resources), null)
          //fill pk
          case v if (table.key == metadata.Key(List(cn))) => cn -> ("#" + table.name)
          //fill fk
          case null if ((refPropName != null && refPropName == n) ||
              (refPropName == null && refColName == cn)) => cn -> (":#" + ptn)
          case v => (table.cols.get(cn).map(_.name).orNull, ":" + n)
        }
      }).filter(_._1 != null && (parent == null || refColName != null)).unzip match {
        case (Nil, Nil) => null
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
          findPkProp(objName).map (pk => {
            Option(refPropName).map(resources.colName(objName, _)).orElse(
                importedKeyOption(table.name, childTable)).map {
              "-" + childTable.name + "[" + _ + " = :" + pk + "]" + (if (o == null) "" else ", " +
                insert_tresql(n, o, name, resources))
            } orNull
        })).orNull
      }
      obj.map((t: (String, _)) => {
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
  
  def save_tresql(name:String, obj:Map[String, _], resources:Resources) = {
    val x = del_upd_ins_obj(name, obj, resources)
    val (n:String, saveable:ListMap[String, Any]) = x.get(name + "#insert").map(
        (name + "#insert", _))getOrElse(x.get(name + "#update").map((name + "#update", _)).getOrElse(
            error("Cannot save data. "))).asInstanceOf[(String, ListMap[String, Any])]
    del_upd_ins_tresql(null, n, saveable, resources) -> saveable    
  }
  
  //returns ListMap so that it is guaranteed that #delete, #update, #insert props are in correct order
  private def del_upd_ins_obj(name:String, obj:Map[String, _], resources:Resources):ListMap[String, Any] =
  resources.metaData.tableOption(resources.tableName(name.split(":")(0))).map { table => {
    var pk:Any = null
    //convert map to list map which guarantees map entry order
    val tresqlObj = ListMap(obj.toList:_*).flatMap(entry=> {
      val (prop, col) = entry._1->resources.colName(name, entry._1)
      entry._2 match {
        case x if(table.key == metadata.Key(List(col))) => pk = x; ListMap(entry)
        //process child table entry
        case cht:List[Map[String, Any]] => cht.foldLeft(ListMap(
            prop + "#delete" -> List[Any](),
            prop + "#update" -> List[Any](),
            prop + "#insert" -> List[Any]()))((m, v)=> {
              m.map(t=> t._1->(del_upd_ins_obj(prop, v, resources).get(t._1).map(_ :: t._2).getOrElse(t._2)))
            }).filter(t=> t._2.size > 0 || (t._1.endsWith("#delete") &&
                //check that delete table exists
                resources.metaData.tableOption(resources.tableName(t._1.takeWhile(!Set(':', '#').contains(_)))) != None))
        //eliminate props not matching col in database
        case x => table.cols.get(col).map(c=> ListMap(entry)).getOrElse(ListMap())
      }
    })
    if (pk == null) ListMap(name + "#insert" -> tresqlObj)
    else ListMap(name + "#delete" -> pk, name + "#update" -> tresqlObj) 
  }}.getOrElse(ListMap())

  private def del_upd_ins_tresql(parentTable: metadata.Table, name: String, obj: ListMap[String, Any],
    res: Resources): String = {
    val PROP_PATTERN(objName, _, refPropName, _, action) = name
    val table = res.metaData.table(res.tableName(objName))
    var pkProp: String = null
    //stupid thing - map find methods conflicting from different traits 
    def findPkProp = if(pkProp != null) pkProp else {
      pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
        n => table.key == metadata.Key(List(res.colName(objName, n._1)))).map(_._1).get
      pkProp
    }    
    def delete(delkey: String, delvals: Seq[_]) = {
      val PROP_PATTERN(delObj, _, parRefProp, _, delact) = delkey
      val delTable = res.metaData.table(res.tableName(delObj))
      val refCol = if (parRefProp != null) res.colName(delObj, parRefProp) else {
        val rfs = delTable.refs(table.name)
        if (rfs.size == 1) rfs(0).cols(0) else error("Cannot create delete statement. None or too much refs from "
          + delTable.name + " to " + table.name + ". Refs: " + rfs)
      }
      "-" + delTable.name + "[" + refCol + " = :" + findPkProp +
        (if (delvals.size > 0) " & " + delTable.key.cols(0) + " !in(:'" + delkey + "')" else "") + "]"
    }
    obj map {
      case (n, Seq(v: ListMap[String, _], _*)) => (del_upd_ins_tresql(table, n, v, res), null)
      case (n, v: Seq[_]) if (n.endsWith("#delete")) => (delete(n, v), null)
      case (n, value) => {
        val col = res.colName(objName, n)
        action match {
          case "insert" => value match {
            //pk null, get it from sequence
            case x if (table.key == metadata.Key(List(col))) => (col, "#" + table.name)
            //fk null, if unambiguous link to parent found get it from parent sequence reference
            case null if ((refPropName != null && refPropName == n) || (refPropName == null &&
              parentTable != null && table.refs(parentTable.name) ==
              List(metadata.Ref(List(col))))) => (col, ":#" + parentTable.name)
            //value
            case _ => (col, ":" + n)
          }
          case "update" => if (table.key == metadata.Key(List(col))) {
            pkProp = n; (null, null)
          } else (col, ":" + n)
        }
      }
    } filter (_._1 != null) unzip match {
      case (cols: List[_], vals: List[_]) => (action match {
        case "insert" => "+" + table.name
        case "update" => "=" + table.name + "[:" + pkProp + "]"
      }) + cols.mkString("{", ",", "}") + vals.filter(_ != null).mkString("[", ",", "]") +
        (if (parentTable != null) " '" + name + "'" else "")
    }
  }
  
  def fill_tresql(name: String, obj: Map[String, _], fillNames:Boolean, resources: Resources,
      ids:Option[Seq[_]] = None, parent:Option[(/*objName*/String, /*pkPropName*/String)] = None): String = {
    def nameExpr(table:String, idVar:String, nameExpr:String) = {
      import QueryParser._
      parseExp(nameExpr) match {
        //replace existing filter with pk filter
        case q:Query => (q.copy(filter = parseExp("[" + ":1('" + idVar + "')]").asInstanceOf[Arr]) match {
          //set default aliases for query columns
          case x@Query(_, _, List(c@Col(_, null, _)), _, _, _, _, _) => 
            x.copy(cols = List(c.copy(alias = "name")))
          case x@Query(_, _, List(Col(_, null, _), c2@Col(_, null, _)), _, _, _, _, _) =>
            x.copy(cols = List(c2.copy(alias = "name")))
          case x@Query(_, _, List(Col(_, null, _), c2@Col(_, null, _), c3@Col(_, null, _)), _, _, _, _, _) =>
            x.copy(cols = List(c2.copy(alias = "code"), c3.copy(alias = "name")))
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
    def findPkProp = pkProp.orElse {
      pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
        n => table.key == metadata.Key(List(resources.colName(objName, n._1)))).map(_._1)
      pkProp
    }
    var filterCol: String = parent.map(p => if (refPropName != null) 
      resources.colName(objName, refPropName) else importedKey(resources.tableName(p._1), table)).orNull
    var filterVal:Any = if(filterCol != null) ":1('" + parent.get._2 + "')" else null
    obj.flatMap((t: (String, _)) => {
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
        //children linked on this table primary key property and child table only foreign key
        //property or foreign key property specified after colon
        case v: Map[String, _] => {
          val Array(on, rpn) = n.split(":").padTo(2, null)
          List(resources.metaData.tableOption(resources.tableName(on)).flatMap(
            cht => findPkProp.flatMap(pkpr => Some("|" +
              fill_tresql(n, v, fillNames, resources, parent = Some(objName -> pkpr)) +
              " '" + n + "'"))).orNull)
        } 
        case _ => List(table.cols.get(cn).map(_.name + " " + n).orNull)
      }
    }).filter(_ != null).mkString(table.name +
        "[" + filterCol + " in(" + ids.map(_.mkString(", ")).getOrElse(filterVal) + ")]{", ", ", "}")
  }
  
  private def importedKeyOption(tableName: String, childTable: metadata.Table) =
    Option(childTable.refs(tableName)).filter(_.size == 1).map(_(0).cols(0))

  private def importedKey(tableName: String, childTable: metadata.Table) = {
    val refs = childTable.refs(tableName)
    if (refs.size != 1) error("Cannot link child table '" + childTable.name +
      "'. Must be exactly one reference from child to parent table '" + tableName +
      "'. Instead these refs found: " + refs)
    refs(0).cols(0)
  } 
}