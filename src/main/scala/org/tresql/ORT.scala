package org.tresql

import scala.collection.immutable.ListMap
import sys._

/** Object Relational Transformations - ORT */
trait ORT {
  
  case class OneToOne(rootTable: String, keys: Set[String])
  case class OneToOneBag(relations: OneToOne, obj: Map[String, Any])
  
  /** <object name | property name>[:<linked property name>][#(insert | update | delete)] */
  val PROP_PATTERN = """(\w+)(:(\w+))?(#(\w+))?"""r
  
  type ObjToMapConverter[T] = (T) => (String, Map[String, _])
  
  def insert(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any =
      insertInternal(name, obj, tresql_structure(obj), filter)
  private def insertInternal(name: String, obj: Map[String, Any], struct: Map[String, Any], filter: String)
    (implicit resources: Resources = Env): Any = {
    val insert = insert_tresql(name, struct, null, null, null, filter, resources)
    if(insert == null) error("Cannot insert data. Table not found for object: " + name)
    Env log (s"\nStructure: $struct")
    Query.build(insert, obj, false)(resources)()
  }
  
  def update(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any = 
    updateInternal(name, obj, tresql_structure(obj), filter)
  
  //TODO update where unique key (not only pk specified)
  private def updateInternal(name: String, obj: Map[String, Any], struct: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any = {
    val update = update_tresql(name, struct, null, null, null, filter, resources)
    if(update == null) error(s"Cannot update data. Table not found or no primary key or no updateable columns found for the object: $name")
    Env log (s"\nStructure: $struct")
    Query.build(update, obj, false)(resources)()    
  }
  /**
   * Saves object obj specified by parameter name. If object primary key is set object
   * is updated, if object primary key is not set object is inserted. Children are merged
   * with database i.e. new ones are inserted, existing ones updated, deleted ones deleted.
   * Children structure i.e. property set must be identical, since one tresql statement is used
   * for all of the children
   */
  def save(name: String, obj: Map[String, _], filter: String = null)(implicit resources: Resources = Env): Any = {
    val (save, saveable) = save_tresql(name, obj, resources)
    Env log saveable.toString
    Query.build(save, saveable, false)(resources)()
  }
  def delete(name: String, id: Any, filter: String = null)(implicit resources: Resources = Env): Any = {
    val delete = "-" + resources.tableName(name) + s"[?${Option(filter).map(f => s" & (f)").getOrElse("")}]"
    Query.build(delete, Map("1"->id), false)(resources)()
  }
  
  /** insert methods to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def insertMultiple(obj: Map[String, Any], names: String*)(filter: String = null)(
      implicit resources: Resources = Env): Any = {
    val (nobj, struct) = multipleOneToOneTransformation(obj, names: _*) 
    insertInternal(names.head, nobj, tresql_structure(struct), filter)
  }
  
  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(filter: String = null)(
      implicit resources: Resources = Env): Any = {
    val (nobj, struct) = multipleOneToOneTransformation(obj, names: _*) 
    updateInternal(names.head, nobj, tresql_structure(struct), filter)
  }

  /* For each name started with second is generated OneToOne object which contains name's references
   * to all of previous names */
  def multipleOneToOneTransformation(obj: Map[String, Any], names: String*)(
    implicit resources: Resources = Env): (Map[String, Any], ListMap[String, Any]) =
    names.tail.foldLeft((
        obj,
        ListMap(obj.toSeq: _*), //use list map so that names are ordered as specified in parameters
        List(names.head))) { (x, n) =>
      val name = n.split(":").head
      (x._1 + (name -> obj),
       x._2 + (name -> OneToOneBag(OneToOne(names.head, importedKeys(n, x._3, resources)), obj)),
       name :: x._3)
    } match {
      case (obj, struct, _) => obj -> struct
    }

  //object methods
  def insertObj[T](obj: T, filter: String = null)(
      implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    insert(v._1, v._2, filter) 
  }
  def updateObj[T](obj: T, filter: String = null)(
      implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    update(v._1, v._2, filter) 
  }
  def saveObj[T](obj: T, filter: String = null)(
      implicit resources: Resources = Env, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    save(v._1, v._2, filter) 
  }

  def tresql_structure[M <: Map[String, Any]](obj: M)(
    /* ensure that returned map is of the same type as passed.
     * For example in the case of ListMap when key ordering is important. */
    implicit bf: scala.collection.generic.CanBuildFrom[M, (String, Any), M]): M = {
    def merge(lm: Seq[Map[String, Any]]): Map[String, Any] =
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
    obj.map {
      case (k, Seq() | Array()) => (k, Map())
      case (k, l: Seq[Map[String, _]]) => (k, merge(l))
      case (k, l: Array[Map[String, _]]) => (k, merge(l))
      case (k, m: Map[String, Any]) => (k, tresql_structure(m))
      case (k, b: OneToOneBag) => (k, b.copy(obj = tresql_structure(b.obj)))
      case x => x
    }(bf.asInstanceOf[scala.collection.generic.CanBuildFrom[Map[String, Any], (String, Any), M]]) //somehow cast is needed
  }
  
  def insert_tresql(
      name: String,
      obj: Map[String, Any],
      parent: String,
      oneToOneRoot: String,
      oneToOne: OneToOne,
      filter: String,
      resources: Resources): String = {    
    //insert statement column, value map from obj
    val Array(objName, refPropName) = name.split(":").padTo(2, null)
    resources.metaData.tableOption(resources.tableName(objName)).map(table => {
      val ptn = if (parent != null) resources.tableName(parent) else null
      val refColName = if (parent == null) null else if (refPropName == null)
        table.refs(ptn).filter(_.cols.size == 1) match { //process refs consisting of only one column
          case Nil => null
          case List(ref) => ref.cols.head
          case x => error(
              s"""Ambiguous references from table '${table.name}' to table '$ptn'.
              Reference must be one and must consist of one column. Found: $x""")
      } else resources.colName(objName, refPropName)
      obj.flatMap((t: (String, _)) => {
        val n = t._1
        val cn = resources.colName(objName, n)
        t._2 match {
          //children or lookup
          case v: Map[String, _] => lookupObject(cn, table).map(lookupTable =>
            lookup_tresql(n, cn, lookupTable, v, resources)).getOrElse {
            List(insert_tresql(n, v, objName, Option(oneToOne).map(_.rootTable).orNull,
                null, filter, resources) -> null)
          }
          //oneToOne child
          case b: OneToOneBag => List(insert_tresql(n, b.obj, objName, b.relations.rootTable,
              b.relations, filter, resources) -> null)
          //pk or fk, one to one relationship
          case _ if table.key.cols == List(cn) /*pk*/ || refPropName == n || refColName == cn /*fk*/
            || oneToOne != null && oneToOne.keys.contains(cn) =>
            //defer one to one relationship setting, pk and fk to parent setting
            Nil
          //ordinary field
          case _ => List(table.colOption(cn).map(_.name).orNull -> resources.valueExpr(objName, n))
        }
      }).groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          //lookup edit tresql
          val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
          //base table tresql
          val tresql =
            (m.getOrElse("b", Nil).asInstanceOf[List[(String, String)]]
             .filter(_._1 != null /*check if prop->col mapping found*/ &&
              (parent == null /*first level obj*/ || refColName != null || oneToOne != null /*child obj (must have reference to parent)*/ )) ++
              (if (refColName == null || oneToOne != null) Map()
                  else Map(refColName -> (s":#${if (oneToOneRoot != null) oneToOneRoot else ptn}") /*add fk col to parent*/ )) ++
              (if (oneToOne != null) oneToOne.keys.map(_ -> s":#${oneToOne.rootTable}").toMap else Map() /* set one to one relationships */) ++
              (if (table.key.cols.length != 1 /*multiple col pk not supported*/ ||
                (parent != null && ((refColName == null && oneToOne == null) /*no relation to parent found*/ ||
                  table.key.cols == List(refColName) /*fk to parent matches pk*/ ) ||
                  (oneToOne != null && oneToOne.keys.contains(table.key.cols.head)/* fk of one to one relations matches pk */))) Map()
              else Map(table.key.cols.head -> (if (oneToOne == null) "#" + table.name else ":#" + oneToOne.rootTable) /*add primary key col*/ )))
              .unzip match {
                case (Nil, Nil) => null
                case (cols: List[String], vals: List[String]) =>
                  cols.mkString(s"+${table.name}{", ", ", "}") +
                  //values
                  Option(filter)
                  //filter is not null so values is select statement
                  .map(f => vals.filter(_ != null).mkString(s" ${table.name} [$f] {", ", ", "} @(1)"))
                  .getOrElse(vals.filter(_ != null).mkString(" [", ", ", "]"))
              }
          val alias = (if (parent != null) " '" + name + "'" else "")
          Option(tresql).map(t => Option(lookupTresql).map(lt => s"[$lt$t]$alias").getOrElse(t + alias)).orNull
      }
    }).orNull
  }

  def update_tresql(
      name: String,
      obj: Map[String, Any],
      parent: String,
      firstPkProp: String,
      oneToOne: OneToOne,
      filter: String,
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
                insert_tresql(n, o, name, Option(oneToOne).map(_.rootTable).orNull, null, filter, resources))
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
          case v: Map[String, _] => lookupObject(cn, table).map{lookupTable =>
            lookup_tresql(n, cn, lookupTable, v, resources)}.getOrElse {
            List((if (isOneToOne(n))
              update_tresql(n, v, name, if (parent == null) findPkProp.orNull else
                firstPkProp, null, filter, resources)
            else childUpdates(n, v)) -> null)
          }
          case b: OneToOneBag =>
            List(update_tresql(n, b.obj, name,
                md.table(b.relations.rootTable).key.cols.head /*TODO may be get rid of firstPkProp*/,
                b.relations, filter, resources) -> null)
          //do not update pkProp
          case _ if (Some(n) == pkProp) => Nil
          case _ if oneToOne != null && oneToOne.keys.contains(cn) => List(cn -> s":#${oneToOne.rootTable}")
          case _ => List(table.colOption(cn).map(_.name).orNull -> resources.valueExpr(name, n))
        }
      }).groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          (m("b").asInstanceOf[List[(String, String)]].filter(_._1 != null).unzip match {
            case (cols: List[String], vals: List[String]) =>
              Option(firstPkProp).orElse(pkProp).map(pk => {
              val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
              //primary key in update condition is taken from sequence so that currId is updated for
              //child records
              val tresql = cols.mkString(s"=${table.name}[${table.key.cols.head} = ${
                if (oneToOne != null) ":#" + oneToOne.rootTable
                else if (firstPkProp == pk) ":" + firstPkProp else "#" + table.name
              }${Option(filter).map(f => s" & ($f)").getOrElse("")}]{", ", ", "}") +
                vals.filter(_ != null).mkString(" [", ", ", "]")
              val alias = if (parent != null) " '" + name + "'" else ""
              if (cols.size > 0)
                Option(lookupTresql).map(lt => s"[$lt$tresql]$alias").getOrElse(tresql + alias)
              else null
            })
          })
      }
    }).orNull
  }
  
  def lookup_tresql(refPropName: String, refColName: String, objName: String, obj: Map[String, _], resources: Resources) =
    resources.metaData.tableOption(resources.tableName(objName)).filter(_.key.cols.size == 1).map(table => {
      val pk = table.key.cols.head
      obj.find(t => resources.colName(objName, t._1) == pk).map(_._1)
         .filter(obj(_) != null)
         .map(pkProp => { //update
        List( /*lookup object update*/
          s"|_changeEnv('$refPropName', ${update_tresql(objName, obj, null, null, null, null, resources)})",
          /*reference to lookup object primary key*/
          refColName -> resources.valueExpr(objName, s"$refPropName.$pkProp"))
      }).getOrElse { /*insert*/
        List(/*lookup object insert, set reference property variable to inserted object primary key*/
            s":$refPropName = |_lookupInsert('$refPropName', ${insert_tresql(objName, obj, null, null, null, null, resources)})",
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
  
  /* Returns zero or one imported key from table for each relation. In the case of multiple
   * imported keys pointing to the same relation the one specified after : symbol is chosen
   * or exception is thrown.
   * This is used to find relation columns for insert/update multiple methods. */
  def importedKeys(tableName: String, relations: List[String], resources: Resources) = {
    val x = tableName split ":"
    val table = resources.metaData.table(resources.tableName(x.head))
    relations.foldLeft(x.tail.toSet) { (keys, relation) =>
      val refs = table.refs(resources.tableName(relation))
      if (refs.size == 1) keys + refs.head.cols.head
      else if (refs.size == 0 || refs.exists(r => keys.contains(r.cols.head))) keys
      else error(s"Ambiguous refs: $refs from table ${table.name} to table $relation")
    }  
  }
}

object ORT extends ORT