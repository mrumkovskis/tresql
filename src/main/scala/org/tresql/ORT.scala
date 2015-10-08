package org.tresql

import scala.collection.immutable.ListMap
import sys._

/** Object Relational Transformations - ORT */
trait ORT extends Query {

  case class OneToOne(rootTable: String, keys: Set[String])
  case class OneToOneBag(relations: OneToOne, obj: Map[String, Any])

  case class TableLink(table: String, refs: Set[String])
  case class Property(tables: List[TableLink],
    insert: Boolean, update: Boolean, delete: Boolean, alias: String)

  val PROP_PATTERN = {
    val ident = """[^:\[\]\s#]+"""
    val table = s"$ident(?::$ident)*"
    val tables = s"""($table(?:#$table)*)"""
    val options = """(?:\[(\+?-?=?)\])?"""
    val alias = """(?:\s+(\w+))?"""
    (tables + options + alias)r
  }
  /** <table | property name>[:<reference to parent or root>*][root table][actions in form <[+-=]> indicating insert, update, delete] [alias]
  *  Example: table:ref1:ref2->root_table[+-=] alias
  */
  val PROP_PATTERN_OLD = {
    val ident = """[^:\[\]\s->]+"""
    val table = s"""(?<table>$ident)"""
    val refs = s"""(?<refs>(?:\\s*:\\s*$ident)*)"""
    val roottable = s"""(?:\\s*->\\s*(?<roottable>$ident))?"""
    val options = """(?:\s*\[(?<options>\+?-?=?)\])?"""
    val alias = """(?:\s+(?<alias>\w+))?"""
    (table + refs + roottable + options + alias)r
  }

  type ObjToMapConverter[T] = (T) => (String, Map[String, _])

  /** QueryBuilder methods **/
  override private[tresql] def newInstance(e: Env, depth: Int, idx: Int) =
    new ORT {
      override def env = e
      override private[tresql] def queryDepth = depth
      override private[tresql] var bindIdx = idx
    }

  /* Expression is built only from macros to ensure ORT lookup editing. */
  case class LookupEditExpr(
    obj: String,
    idName: String,
    insertExpr: Expr,
    updateExpr: Expr)
  extends BaseExpr {
    override def apply() = env(obj) match {
      case m: Map[String, Any] =>
        if (idName != null && (m contains idName) && m(idName) != null) {
          val lookupObjId = m(idName)
          updateExpr(m)
          lookupObjId
        } else extractId(insertExpr(m))
      case null => null
      case x => error(s"Cannot set environment variables for the expression. $x is not a map.")
    }
    def extractId(result: Any) = result match {
      case (_, id) => id //insert expression
      case s: Seq[_] => s.last match { case (_, id) => id } //array expression
      case x => error(s"Unable to extract id from expr result: $x, expr: $insertExpr")
    }
    def defaultSQL = s"LookupEditExpr($obj, $idName, $insertExpr, $updateExpr)"
  }
  /* Expression is built from macros to ensure ORT children editing */
  case class InsertOrUpdateExpr(table: String, insertExpr: Expr, updateExpr: Expr)
  extends BaseExpr {
    val idName = env.table(table).key.cols.headOption.orNull
    override def apply() =
      if (idName != null && env.containsNearest(idName) && env(idName) != null)
        updateExpr() else insertExpr()
    def defaultSQL = s"InsertOrUpdateExpr($idName, $insertExpr, $updateExpr)"
  }
  /* Expression is built from macros to ensure ORT children editing */
  case class DeleteChildrenExpr(obj: String, table: String, expr: Expr)
  extends BaseExpr {
    val idName = env.table(table).key.cols.headOption.orNull
    override def apply() = {
      env(obj) match {
        case s: Seq[Map[String, _]] =>
          expr(if (idName != null)
            Map("ids" -> s.map(_(idName)).filter(_ != null)) else Map[String, Any]())
      }
    }
    override def defaultSQL = s"DeleteChildrenExpr($obj, $idName, $expr)"
  }
  /* Expression is built from macro.
   * Effectively env.currId(idSeq, IdRefExpr(idRefSeq)())*/
  case class IdRefIdExpr(idRefSeq: String, idSeq: String) extends BaseExpr {
    val idRefExpr = IdRefExpr(idRefSeq)
    override def apply() = {
      val id = idRefExpr()
      env.currId(idSeq, id)
      id
    }
    override def defaultSQL = idRefExpr.defaultSQL
    override def toString = s"$idRefExpr#$idSeq"
  }

  def insert(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any =
      insertInternal(name, obj, tresql_structure(obj), Map(), filter)
  private def insertInternal(
      name: String,
      obj: Map[String, Any],
      struct: Map[String, Any],
      refsToRoot: Map[String, String],
      filter: String)
    (implicit resources: Resources = Env): Any = {
    val insert = insert_tresql(name, struct, Nil, refsToRoot, null, filter, resources)
    if(insert == null) error("Cannot insert data. Table not found for object: " + name)
    Env log (s"\nStructure: $struct")
    build(insert, obj, false)(resources)()
  }

  def update(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any =
    updateInternal(name, obj, tresql_structure(obj), Map(), filter)

  //TODO update where unique key (not only pk specified)
  private def updateInternal(
      name: String,
      obj: Map[String, Any],
      struct: Map[String, Any],
      refsToRoot: Map[String, String],
      filter: String = null)
    (implicit resources: Resources = Env): Any = {
    val update = update_tresql(name, struct, Nil, refsToRoot, null, filter, resources)
    if(update == null) error(s"Cannot update data. Table not found or no primary key or no updateable columns found for the object: $name")
    Env log (s"\nStructure: $struct")
    build(update, obj, false)(resources)()
  }

  def delete(name: String, id: Any, filter: String = null, filterParams: Map[String, Any] = null)
  (implicit resources: Resources = Env): Any = {
    val Array(tableName, alias) = name.split("\\s+").padTo(2, null)
    (for {
      table <- resources.metaData.tableOption(tableName)
      pk <- table.key.cols.headOption
      if table.key.cols.size == 1
    } yield {
      val delete = s"-${table.name}${Option(alias).map(" " + _).getOrElse("")}[$pk = ?${Option(filter)
        .map(f => s" & ($f)").getOrElse("")}]"
      build(delete, Map("1" -> id) ++ Option(filterParams).getOrElse(Map()), false)(resources)()
    }) getOrElse {
      error(s"Table $name not found or table primary key not found or table primary key consists of more than one column")
    }
  }

  /** insert methods to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def insertMultiple(obj: Map[String, Any], names: String*)(filter: String = null)(
      implicit resources: Resources = Env): Any = {
    val (nobj, struct, refsToRoot) = multipleOneToOneTransformation(obj, names: _*)
    insertInternal(names.head, nobj, tresql_structure(struct), refsToRoot, filter)
  }

  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(filter: String = null)(
      implicit resources: Resources = Env): Any = {
    val (nobj, struct, refsToRoot) = multipleOneToOneTransformation(obj, names: _*)
    updateInternal(names.head, nobj, tresql_structure(struct), refsToRoot, filter)
  }

  def multiSaveProp(names: Seq[String])(implicit resources: Resources = Env) =
    names.tail.foldLeft(List(names.head)) { (ts, t) =>
      (t.split(":").head + importedKeysAndPks(t, ts, resources)
        .mkString(":", ":", "")) :: ts
  }.reverse.mkString("#")

  /* For each name started with second is generated OneToOne object which contains name's references
   * to all of previous names */
  def multipleOneToOneTransformation(obj: Map[String, Any], names: String*)(
    implicit resources: Resources = Env): (Map[String, Any], ListMap[String, Any], Map[String, String]) =
    names.tail.foldLeft((
        obj,
        ListMap(obj.toSeq: _*), //use list map so that names are ordered as specified in parameters
        List(names.head))) { (x, n) =>
      val name = n.split(":").head
      (x._1 + (name -> obj),
       x._2 + (name -> OneToOneBag(OneToOne(names.head, importedKeys(n, x._3, resources)), obj)),
       name :: x._3)
    } match {
      case (obj, struct, n) =>
      (obj, struct, n.reverse match { case head :: tail => tail.map(_ -> head).toMap })
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

  def insert_tresql_new(
    name: String,
    obj: Map[String, Any],
    parents: List[TableLink],
    filter: String)(implicit resources: Resources): String = {
    def insert(
      table: metadata.Table,
      alias: String,
      refsAndPk: Set[(String, String)]): String =
      obj.flatMap { case (n, v) => v match {
        //children or lookup
        case o: Map[String, Any] => lookupObject(n, table).map(lookupTable =>
          lookup_tresql(n, lookupTable, o, resources)).getOrElse {
          List(insert_tresql_new(n, o, TableLink(table.name,
            refsAndPk.map(_._1)) :: parents, null) -> null)
        }
        //pk or fk
        case _ if refsAndPk.exists(_._1 == n) => Nil
        //ordinary field
        case _ => List(table.colOption(n).map(_.name).orNull -> resources.valueExpr(table.name, n))
      }
    }.groupBy { case _: String => "l" case _ => "b"} match {
      case m: Map[String, List[_]] =>
      val tableName = table.name
      //lookup edit tresql
      val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
      //base table tresql
      val tresql =
        (m.getOrElse("b", Nil).asInstanceOf[List[(String, String)]]
         .filter(_._1 != null /*check if prop->col mapping found*/) ++ refsAndPk)
         match {
           case x if x.size == 0 => null //no insertable columns found
           case x =>
             val (cols, vals) = x.unzip
             val tn = tableName + alias
             cols.mkString(s"+$tableName {", ", ", "}") +
             (vals.filter(_ != null) match {
               case vs if filter == null => vs.mkString(" [", ", ", "]")
               case vs => vs.mkString(s" $tn [$filter] {", ", ", "} @(1)")
             })
         }
       val tresqlAlias = parents.headOption.map(_ => s" '$name'").getOrElse("")
       Option(tresql).map(t => Option(lookupTresql).map(lt => s"[$lt$t]$tresqlAlias")
         .getOrElse(t + tresqlAlias)).orNull
    }
    val Property(tables, _, _, _, alias) = parseProperty(name)
    val parent = parents.headOption.orNull
    val md = resources.metaData
    (for {
      propTable <- tables.headOption
      table <- md.tableOption(propTable.table)
      refs <- Some(parent).filter(_ == null).map(_ => Set[String]()) /*no parent no refs*/ orElse
        Some(propTable.refs).filter(_.size > 0) /* refs in prop */orElse {
          //calculate ref
          table.refs(parent.table).filter(_.cols.size == 1) match {
            case Nil => None //no refs to parent found
            case List(ref) => ref.cols.headOption.map(Set(_))
            case x => error(
                s"""Ambiguous references from table '${table.name}' to table '${parent.table}'.
                Reference must be one and must consist of one column. Found: $x""")
          }
        }
    } yield {
      val pk = table.key.cols.headOption.orNull
      def idRefId(idRef: String, id: String) = s"_id_ref_id($idRef, $id)"
      val baseTresql = insert(table, alias,
        refs.map(r=> r -> (if (r == pk) idRefId(parent.table, table.name) //pk matches ref to parent
          else s":#${parent.table}")) ++
        (if (pk == null || refs.contains(pk)) Set() else Set(pk -> s"#${table.name}")))
      val linkedTresql =
        (for(linkedTable <- tables.tail if !md.tableOption(linkedTable.table).isEmpty)
         yield ", " + insert(md.table(linkedTable.table), alias, linkedTable.refs
         .map(_ -> idRefId(table.name, linkedTable.table)))
        ).mkString
      baseTresql + linkedTresql
    }).orNull
  }

  def insert_tresql(
      name: String,
      obj: Map[String, Any],
      parentChain: List[(String, String)], //tableName -> refColName
      refsToRoot: Map[String, String],
      oneToOne: OneToOne,
      filter: String,
      resources: Resources): String = {
    val (tableName, refPropName, _, _, _, alias) =
      parsePropertyOld(name)
    val parent = parentChain.headOption.map(_._1).orNull
    (for {
      table <- resources.metaData.tableOption(tableName)
      /*child obj (must have reference to parent)*/
      refColName <- Some(parent).filter(_ == null)/*null if no parent*/ orElse
        Option(oneToOne).map(_ => null)/*null if one to one*/ orElse Option(refPropName) orElse(
          table.refs(parent).filter(_.cols.size == 1) match { //process refs consisting of only one column
            case Nil => None
            case List(ref) => ref.cols.headOption
            case x => error(
                s"""Ambiguous references from table '$tableName' to table '$parent'.
                Reference must be one and must consist of one column. Found: $x""")
          })
    } yield {
      def parentIdRef = s":#${refsToRoot.getOrElse(parent, parent)}"
      obj.flatMap { case (n, v) => v match {
          //children or lookup
          case v: Map[String, _] => lookupObject(n, table).map(lookupTable =>
            lookup_tresql(n, lookupTable, v, resources)).getOrElse {
            List(insert_tresql(n, v, (tableName, refColName) :: parentChain,
              refsToRoot, null, null /*do not pass filter further*/,
              resources) -> null)
          }
          //oneToOne child
          case b: OneToOneBag => List(
            insert_tresql(n, b.obj, (tableName, refColName) :: parentChain,
            refsToRoot, b.relations, null /*do no pass filter further*/, resources) -> null)
          //pk or fk, one to one relationship
          case _ if table.key.cols == List(n) /*pk*/ || refColName == n /*fk*/
            || oneToOne != null && oneToOne.keys.contains(n) =>
            //defer one to one relationship setting, pk and fk to parent setting
            Nil
          //ordinary field
          case _ => List(table.colOption(n).map(_.name).orNull -> resources.valueExpr(tableName, n))
        }
      }.groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          //lookup edit tresql
          val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
          //base table tresql
          val tresql =
            (m.getOrElse("b", Nil).asInstanceOf[List[(String, String)]]
             .filter(_._1 != null /*check if prop->col mapping found*/) ++
              (if (refColName == null || oneToOne != null) Map()
                  else Map(refColName -> parentIdRef /*add fk col to parent*/ )) ++
              (if (oneToOne != null) oneToOne.keys.map(_ -> s":#${oneToOne.rootTable}").toMap else Map() /* set one to one relationships */) ++
              (if (table.key.cols.length != 1 /*multiple col pk not supported*/ ||
                (parent != null && ((refColName == null && oneToOne == null) /*no relation to parent found*/ ||
                  table.key.cols == List(refColName) /*fk to parent matches pk*/ ) ||
                  (oneToOne != null && oneToOne.keys.contains(table.key.cols.head)/* fk of one to one relations matches pk */))) Map()
              else Map(table.key.cols.head -> (if (oneToOne == null) "#" + tableName else ":#" + oneToOne.rootTable) /*add primary key col*/ )))
              match {
                case x if x.size == 0 => null
                case x =>
                  val (cols, vals) = x.unzip
                  val tn = tableName + (if (alias == null) "" else " " + alias)
                  cols.mkString(s"+$tableName {", ", ", "}") +
                  (vals.filter(_ != null) match {
                    case vs if filter == null => vs.mkString(" [", ", ", "]")
                    case vs => vs.mkString(s" $tn [$filter] {", ", ", "} @(1)")
                  })
              }
          val tresqlAlias = (if (parent != null) " '" + name + "'" else "")
          Option(tresql).map(t => Option(lookupTresql).map(lt => s"[$lt$t]$tresqlAlias")
            .getOrElse(t + tresqlAlias)).orNull
      }
    }).orNull
  }

  def update_tresql(
      name: String,
      obj: Map[String, Any],
      parentChain: List[(String, String)], //tableName -> refColName
      refsToRoot: Map[String, String],
      oneToOne: OneToOne,
      filter: String,
      resources: Resources): String = {
    val (tableName, refPropName, insertAction, updateAction, deleteAction, alias) =
      parsePropertyOld(name)
    val parent = parentChain.headOption.map(_._1).orNull
    val md = resources.metaData
    md.tableOption(tableName).map{table =>
      val refColName = Option(refPropName)
        .orElse(Option(parent)
          .filter(_ => oneToOne == null) //refCol not relevant in oneToOne case
          .flatMap(importedKeyOption(_, table)))
        .orNull
      def parentIdRef = s":#${refsToRoot.getOrElse(parent, parent)}"
      def deleteAllChildren = s"-$tableName[$refColName = $parentIdRef]"
      def deleteMissingChildren = {
        val filter = table.key.cols.headOption.map(k => s" & $k !in :ids").getOrElse("")
        s"""_delete_children('$name', '$tableName', -${table
          .name}[$refColName = $parentIdRef$filter])"""
      }
      def oneToOneTable(tname: String) = (for {
        t <- md.tableOption(tname)
        ref <- importedKeyOption(table.name, t)
        if t.key.cols.size == 1 && ref == t.key.cols.head
      } yield t).orNull
      def update = (for {pk <- table.key.cols.headOption} yield obj.flatMap {
        case (n, v) => v match {
          //children
          case v: Map[String, _] =>
            lookupObject(n, table).map(lookupTable =>
              lookup_tresql(n, lookupTable, v, resources))
              .getOrElse {
                val extTable = oneToOneTable(n)
                List((
                  if (extTable != null)
                    update_tresql(n, v, (tableName, refColName) :: parentChain,
                      Map(extTable.name -> table.name),
                      OneToOne(table.name, Set(extTable.key.cols.head)),
                      null /* do no pass filter further */, resources)
                  else
                    update_tresql(n, v, (tableName, refColName) :: parentChain,
                      refsToRoot, null, null, resources)) -> null)
              }
          case b: OneToOneBag => List(
            update_tresql(n, b.obj, (tableName, refColName) :: parentChain, refsToRoot,
                b.relations, null /* do no pass filter further */, resources) -> null)
          case _ if table.key == metadata.Key(List(n)) => Nil //do not update pk
          case _ if oneToOne != null && oneToOne.keys.contains(n) =>
            List(n -> s":#${oneToOne.rootTable}")
          case _ => List(table.colOption(n).map(_.name).orNull -> resources.valueExpr(tableName, n))
        }
      }.groupBy { case _: String => "l" case _ => "b" } match {
        case m: Map[String, List[_]] =>
          m("b").asInstanceOf[List[(String, String)]].filter(_._1 != null).unzip match {
            case (cols: List[String], vals: List[String]) =>
              val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
              val tn = tableName + (if (alias == null) "" else " " + alias)
              //primary key in update condition is taken from sequence so that currId is updated for
              //child records
              val tresql = cols.mkString(s"=$tn [$pk = ${
                refsToRoot.get(tableName).map(":#" + _).getOrElse("#" + table.name) +
                (if (refColName != null)
                  s" & $refColName = $parentIdRef" else "") //make sure record belongs to parent
              }${Option(filter).map(f => s" & ($f)").getOrElse("")}]{", ", ", "}") +
                vals.filter(_ != null).mkString(" [", ", ", "]")
              val tresqlAlias = if (parent != null) " '" + name + "'" else ""
              val finalTresql = if (cols.size > 0) Option(lookupTresql)
                .map(lt => s"[$lt$tresql]$tresqlAlias")
                .getOrElse(tresql + tresqlAlias)
              else null
              finalTresql
          }
      }).orNull
      def insert = insert_tresql(name, obj, parentChain, refsToRoot, null, null, resources)
      def stripTrailingAlias(tresql: String, alias: String) =
        if (tresql != null && tresql.endsWith(alias))
          tresql.dropRight(alias.length) else tresql
      def insertOrUpdate = s"""|_insert_or_update('$tableName', ${
        stripTrailingAlias(insert, s" '$name'")}, ${
        stripTrailingAlias(update, s" '$name'")}) '$name'"""
      if (parent != null && oneToOne == null) { //children with no one to one relationships
        if (refColName == null) null //no relation to parent found
        else {
          val deleteTresql = if (deleteAction)
            Option(if(!updateAction) deleteAllChildren else deleteMissingChildren)
            else None
          val editTresql = (insertAction, updateAction) match {
            case (true, true) => Option(insertOrUpdate)
            case (true, false) => Option(insert)
            case (false, true) => Option(update)
            case (false, false) => None
          }
          (deleteTresql ++ editTresql).mkString(", ")
        }
      } else update
    }.orNull
  }

  def lookup_tresql(
    refColName: String,
    name: String,
    obj: Map[String, _],
    resources: Resources) =
    resources.metaData.tableOption(name).filter(_.key.cols.size == 1).map {
      table =>
      val pk = table.key.cols.headOption.filter(obj contains).orNull
      val insert = insert_tresql(name, obj, Nil, Map(), null, null, resources)
      val update = update_tresql(name, obj, Nil, Map(), null, null, resources)
      List(
        s":$refColName = |_lookup_edit('$refColName', ${
          if (pk == null) "null" else s"'$pk'"}, $insert, $update)",
        refColName -> resources.valueExpr(name, refColName))
    }.orNull

  def lookupObject(refColName: String, table: metadata.Table) =
    table.refTable.get(List(refColName))

  private def parsePropertyOld(name: String) = {
    val PROP_PATTERN_OLD(tableName, refs, rootTable, action, alias) = name
    //insert action, update action, delete action
    val (ia, ua, da) = Option(action).map (a =>
      (action contains "+", action contains "=", action contains "-")
    ).getOrElse {(true, false, true)}
    val refPropName = (refs split ":").tail.headOption.orNull
    (tableName, refPropName, ia, ua, da, alias)
  }

  private def parseProperty(name: String) = {
    val PROP_PATTERN(tables, options, alias) = name
    //insert update delete option
    val (i, u, d) = Option(options).map (_ =>
      (options contains "+", options contains "=", options contains "-")
    ).getOrElse {(true, false, true)}
    Property((tables split "#").map{ t =>
      val x = t split ":"
      TableLink(x.head, x.tail.toSet)
    }.toList, i, u, d, if(alias != null) alias else "")
  }

  private def importedKeyOption(tableName: String, childTable: metadata.Table) =
    Option(childTable.refs(tableName).filter(_.cols.size == 1))
      .filter(_.size == 1)
      .map(_.head.cols.head)

  private def importedKey(tableName: String, childTable: metadata.Table) = {
    val refs = childTable.refs(tableName).filter(_.cols.size == 1)
    if (refs.size != 1) error("Cannot link child table '" + childTable.name +
      "'. Must be exactly one reference from child to parent table '" + tableName +
      "'. Instead these refs found: " + refs)
    refs.head.cols.head
  }

  /* Returns zero or one imported key from table for each relation. In the case of multiple
   * imported keys pointing to the same relation the one specified after : symbol is chosen
   * or exception is thrown.
   * This is used to find relation columns for insert/update multiple methods. */
  def importedKeysAndPks(tableName: String, relations: List[String], resources: Resources) = {
    val x = tableName split ":"
    val table = resources.metaData.table(x.head)
    relations.foldLeft(x.tail.toSet) { (keys, rel) =>
      val relation = rel.split(":").head
      val refs = table.refs(relation).filter(_.cols.size == 1)
      (if (refs.size == 1) keys + refs.head.cols.head
      else if (refs.size == 0 || refs.exists(r => keys.contains(r.cols.head))) keys
      else error(s"Ambiguous refs: $refs from table ${table.name} to table $relation")) ++
      table.key.cols.headOption.map(Set(_)).getOrElse(Set())
    }
  }

  def importedKeys(tableName: String, relations: List[String], resources: Resources) = {
    val x = tableName split ":"
    val table = resources.metaData.table(x.head)
    relations.foldLeft(x.tail.toSet) { (keys, relation) =>
      val refs = table.refs(relation)
      if (refs.size == 1) keys + refs.head.cols.head
      else if (refs.size == 0 || refs.exists(r => keys.contains(r.cols.head))) keys
      else error(s"Ambiguous refs: $refs from table ${table.name} to table $relation")
    }
  }
}

object ORT extends ORT
