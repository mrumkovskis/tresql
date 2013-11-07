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
  
  def insert_tresql(name: String, obj: Map[String, _], parent: String, resources: Resources): String = {    
    //insert statement column, value map from obj
    val Array(objName, refPropName) = name.split(":").padTo(2, null)
    resources.metaData.tableOption(resources.tableName(objName)).map(table => {
      val ptn = if (parent != null) resources.tableName(parent) else null
      val refColName = if (parent == null) null else if (refPropName == null)
        table.refs(ptn) match {
          case Nil => null
          case List(ref) if ref.cols.length == 1 => ref.cols(0)
          case x => error(
              "Ambiguous references to table (reference must be one and must consist of one column): "
              + ptn + ". Refs: " + x)
      } else resources.colName(objName, refPropName)
      var hasRef: Boolean = parent == null
      var hasPk: Boolean = false
      (obj.map((t: (String, _)) => {
        val n = t._1
        val cn = resources.colName(objName, n)
        t._2 match {
          //children
          case Seq() | Array() => (null, null)
          case Seq(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case Array(v: Map[String, _], _*) => (insert_tresql(n, v, objName, resources), null)
          case v: Map[String, _] => (insert_tresql(n, v, objName, resources), null)
          //fill pk (only if it does not match fk prop to parent, in this case fk prop, see below,
          //takes precedence)
          case v if table.key.cols == List(cn) && table.key.cols != List(refColName) =>
            hasPk = true
            cn -> ("#" + table.name)
          //fill fk
          case v if ((refPropName != null && refPropName == n) ||
              (refPropName == null && refColName == cn)) =>
                hasRef = true
                cn -> (":#" + ptn)
          case v => (table.colOption(cn).map(_.name).orNull, resources.valueExpr(objName, n))
        }
      }).filter(_._1 != null && (parent == null || refColName != null)) ++
      (if (hasRef || refColName == null) Map() else Map(refColName -> (":#" + ptn))) ++
      (if (hasPk || table.key.cols.length != 1 || (parent != null && (refColName == null ||
          table.key.cols == List(refColName)))) Map()
       else Map(table.key.cols(0) -> ("#" + table.name)))).unzip match {
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
      def findPkProp = pkProp.orElse {
        pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
          n => table.key == metadata.Key(List(resources.colName(name, n._1)))).map(_._1)
        pkProp
      }
      def childUpdates(n:String, o:Map[String, _]) = {
        val Array(childObjName, refPropName) = n.split(":").padTo(2, null)
        resources.metaData.tableOption(resources.tableName(childObjName)).flatMap(childTable => 
          findPkProp.map (pk => {
            Option(refPropName).map(resources.colName(childObjName, _)).orElse(
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
          case v => (table.colOption(cn).map(_.name).orNull, resources.valueExpr(name, n))
        }
      }).filter(_._1 != null).unzip match {
        case (cols: List[_], vals: List[_]) => pkProp.flatMap(pk =>
          //primary key in update condition is taken from sequence so that currId is updated for
          //child records
          Some(cols.mkString("=" + table.name + "[" + table.key.cols(0) + " = #" + table.name + "]"
              + "{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]")))
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
        case x => table.colOption(col).map(c=> ListMap(entry)).getOrElse(ListMap())
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
            case _ => (col, res.valueExpr(objName, n))
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