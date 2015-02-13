package org.tresql

import scala.collection.immutable.ListMap
import sys._

/** Object Relational Transformations - ORT */
trait ORT {
  
  /** <object name | property name>[:<linked property name>][#(insert | update | delete)] */
  val PROP_PATTERN = """(\w+)(:(\w+))?(#(\w+))?"""r
  
  type ObjToMapConverter[T] = (T) => (String, Map[String, _])
  
  def insert(name: String, obj: Map[String, _])(implicit resources: Resources = Env): Any = {
    val struct = tresql_structure(obj)
    val insert = insert_tresql(name, struct, null, resources)
    if(insert == null) error("Cannot insert data. Table not found for object: " + name)
    Env log (s"Structure: $struct")
    Query.build(insert, resources, obj, false)()
  }
  //TODO update where unique key (not only pk specified)
  def update(name: String, obj: Map[String, _])(implicit resources: Resources = Env): Any = {
    val struct = tresql_structure(obj)
    val update = update_tresql(name, struct, null, null, resources)
    if(update == null) error("Cannot update data. Table not found or primary key not found " +
    		"for the object: " + name)
    Env log (s"Structure: $struct")
    Query.build(update, resources, obj, false)()    
  }
  /**
   * Saves object obj specified by parameter name. If object primary key is set object
   * is updated, if object primary key is not set object is inserted. Children are merged
   * with database i.e. new ones are inserted, existing ones updated, deleted ones deleted.
   * Children structure i.e. property set must be identical, since one tresql statement is used
   * for all of the children
   */
  def save(name: String, obj: Map[String, _])(implicit resources: Resources = Env): Any = {
    val (save, saveable) = save_tresql(name, obj, resources)
    Env log saveable.toString
    Query.build(save, resources, saveable, false)()
  }
  def delete(name: String, id: Any)(implicit resources: Resources = Env): Any = {
    val delete = "-" + resources.tableName(name) + "[?]"
    Query.build(delete, resources, Map("1"->id), false)()
  }
  
  /** insert methods to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def insertMultiple(obj: Map[String, Any], names: String*)(implicit resources: Resources = Env): Any = {
    val o = names.tail.foldLeft(obj)((o, n) => o + (n -> obj))
    insert(names.head, o)
  }
  
  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(implicit resources: Resources = Env): Any = {
    val o = names.tail.foldLeft(obj)((o, n) => o + (n -> obj))
    update(names.head, o)
  }

  //object methods
  def insertObj[T](obj: T)(implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    insert(v._1, v._2) 
  }
  def updateObj[T](obj: T)(implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    update(v._1, v._2) 
  }
  def saveObj[T](obj: T)(implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    save(v._1, v._2) 
  } 
  
  
  def tresql_structure(obj: Map[String, _]): Map[String, Any] = {
    def merge(lm: Seq[Map[String, _]]): Map[String, Any] =
      lm.tail.foldLeft(tresql_structure(lm.head))((l, m) => {
        val x = tresql_structure(m)
        l map (t => (t._1, (t._2, x.getOrElse(t._1, null)))) map {
          case (k, (v1: Map[String, _], v2: Map[String, _])) if v1.size > 0 && v2.size > 0 =>
            (k, merge(List(v1, v2)))
          case (k, (v1: Map[String, _], _)) if v1.size > 0 => (k, v1)
          case (k, (_, v2: Map[String, _])) if v2.size > 0 => (k, v2)
          case (k, (v1, _)) => (k, v1)
        }
      })
    obj map {
      case (k, Seq() | Array()) => (k, Map())
      case (k, l: Seq[Map[String, _]]) => (k, merge(l))
      case (k, l: Array[Map[String, _]]) => (k, merge(l))
      case (k, m: Map[String, _]) => (k, tresql_structure(m))
      case x => x
    }
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
      obj.flatMap((t: (String, _)) => {
        val n = t._1
        val cn = resources.colName(objName, n)
        t._2 match {
          //children or lookup
          case v: Map[String, _] => lookupObject(cn, table).map(lookupTable =>
            lookup_tresql(n, cn, lookupTable, v, resources)).getOrElse {
            List(insert_tresql(n, v, objName, resources) -> null)
          }
          //fill pk (only if it does not match fk prop to parent, in this case fk prop, see below,
          //takes precedence)
          case _ if table.key.cols == List(cn) && table.key.cols != List(refColName) =>
            hasPk = true
            List(cn -> ("#" + table.name))
          //fill fk
          case _ if ((refPropName != null && refPropName == n) ||
              (refPropName == null && refColName == cn)) =>
                hasRef = true
                List(cn -> (":#" + ptn))
          case _ => List(table.colOption(cn).map(_.name).orNull -> resources.valueExpr(objName, n))
        }
      }).groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          //lookup edit tresql
          val lookupTresql = m.get("l").map(_.map((x: Any) => String.valueOf(x) + ", ").mkString).orNull
          //base table tresql
          val tresql = (m.getOrElse("b", Nil).asInstanceOf[List[(String, String)]].filter(_._1 != null /*check if prop->col mapping found*/ &&
            (parent == null /*first level obj*/ || refColName != null /*child obj (must have reference to parent)*/ )) ++
            (if (hasRef || refColName == null) Map() else Map(refColName -> (":#" + ptn) /*add fk col to parent*/ )) ++
            (if (hasPk || table.key.cols.length != 1 /*multiple col pk not supported*/ ||
              (parent != null && (refColName == null || table.key.cols == List(refColName) /*fk to parent matches pk*/ ))) Map()
            else Map(table.key.cols(0) -> ("#" + table.name) /*add primary key col*/ ))).unzip match {
              case (Nil, Nil) => null
              case (cols: List[String], vals: List[String]) =>
                cols.mkString(s"+${table.name}{", ", ", "}") + vals.filter(_ != null).mkString(" [", ", ", "]")             
            }
          val alias = (if (parent != null) " '" + name + "'" else "")
          Option(tresql).map(t => Option(lookupTresql).map(lt => s"[$lt$t]$alias").getOrElse(t + alias)).orNull
      }
    }).orNull
  }

  def update_tresql(name: String, obj: Map[String, _], parent: String, firstPkProp: String,
      resources: Resources): String = {
    val md = resources.metaData
    md.tableOption(resources.tableName(name)).flatMap(table => {
      var pkProp: Option[String] = None
      //stupid thing - map find methods conflicting from different traits 
      def findPkProp = pkProp.orElse {
        pkProp = obj.asInstanceOf[TraversableOnce[(String, _)]].find(
          n => table.key == metadata.Key(List(resources.colName(name, n._1)))).map(_._1)
        pkProp
      }
      def childUpdates(n:String, o:Map[String, _]) = {
        val Array(childObjName, refPropName) = n.split(":").padTo(2, null)
        md.tableOption(resources.tableName(childObjName)).flatMap(childTable => 
          (findPkProp orElse Option(firstPkProp)).map (pk => {
            Option(refPropName).map(resources.colName(childObjName, _)).orElse(
                importedKeyOption(table.name, childTable)).map {
              "-" + childTable.name + "[" + _ + " = :" + pk + "]" + (if (o == null) "" else ", " +
                insert_tresql(n, o, name, resources))
            } orNull
        })).orNull
      }
      def isOneToOne(childName: String) = md.tableOption(resources.tableName(childName)).flatMap {
        t => importedKeyOption(table.name, t).map(t.key.cols.size == 1 && _ == t.key.cols.head) } getOrElse false
      obj.flatMap((t: (String, _)) => {
        val n = t._1
        val cn = resources.colName(name, n)
        if (table.key == metadata.Key(List(cn))) pkProp = Some(n)
        t._2 match {
          //children
          case v: Map[String, _] => lookupObject(cn, table).map(lookupTable =>
            lookup_tresql(n, cn, lookupTable, v, resources)).getOrElse {
            List((if (isOneToOne(n))
              update_tresql(n, v, name, if (parent == null) findPkProp.orNull else firstPkProp, resources)
            else childUpdates(n, v)) -> null)
          }
          //do not update pkProp
          case _ if (Some(n) == pkProp) => List((null, null))
          case _ => List(table.colOption(cn).map(_.name).orNull -> resources.valueExpr(name, n))
        }
      }).groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          (m("b").asInstanceOf[List[(String, String)]].filter(_._1 != null).unzip match {
            case (cols: List[String], vals: List[String]) => Option(firstPkProp).orElse(pkProp).map(pk => {
              val lookupTresql = m.get("l").map(_.map((x: Any)=> String.valueOf(x) + ", ").mkString).orNull
              //primary key in update condition is taken from sequence so that currId is updated for
              //child records
              val tresql = cols.mkString(s"=${table.name}[${table.key.cols(0)} = ${
                if (firstPkProp == pk) ":" + firstPkProp else "#" + table.name
              }]{", ", ", "}") +
                vals.filter(_ != null).mkString(" [", ", ", "]")
              val alias = if (parent != null) " '" + name + "'" else ""
              if (cols.size > 0)
                Option(lookupTresql).map(lt => s"[$lt$tresql]$alias").getOrElse(tresql + alias)
              else error(s"Column clause empty: $tresql")
            })
          })
      }
    }).orNull
  }
  
  def lookup_tresql(refPropName: String, refColName: String, objName: String, obj: Map[String, _], resources: Resources) =
    resources.metaData.tableOption(resources.tableName(objName)).filter(_.key.cols.size == 1).map(table => {
      val pk = table.key.cols.head
      obj.find(t => resources.colName(objName, t._1) == pk).map(_._1).map(pkProp => { //update
        List( /*lookup object update*/
          s"|_changeEnv('$refPropName', ${update_tresql(objName, obj, null, null, resources)})",
          /*reference to lookup object primary key*/
          refColName -> resources.valueExpr(objName, s"$refPropName.$pkProp"))
      }).getOrElse { /*insert*/
        List(/*lookup object insert, set reference property variable to inserted object primary key*/
            s":$refPropName = |_changeEnv('$refPropName', ${insert_tresql(objName, obj, null, resources)}), :$refPropName = :$refPropName.2",
          /*reference column set to reference property variable value*/
          refColName -> resources.valueExpr(objName, refPropName))
      }
    }).orNull  
  
  //TODO returns lookup table name not object name. lookup_tresql requires object name, so the
  //two must be equal.
  def lookupObject(refColName: String, table: metadata.Table) = table.refTable.get(List(refColName))
  
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
              parentTable != null && (table.refs(parentTable.name) match {
                case List(metadata.Ref(List(c), _)) => c == col
                case _ => false
              }))) => (col, ":#" + parentTable.name)
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

object ORT extends ORT