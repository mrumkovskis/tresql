package org.tresql

import sys._

/** Object Relational Transformations - ORT */
trait ORT extends Query {

  /* table[:ref]*[#table[:ref]*]*[[options]] [alias]
  *  Options are to be specified in such order: insert, delete, update, i.e. '+-='
  *  Examples:
  *    dept#car:deptnr:nr#tyres:carnr:nr
  *    dept[+=] alias
  */
  val PROP_PATTERN = {
    val ident = """[^:\[\]\s#]+"""
    val table = s"$ident(?::$ident)*"
    val tables = s"""($table(?:#$table)*)"""
    val options = """(?:\[(\+?-?=?)\])?"""
    val alias = """(?:\s+(\w+))?"""
    (tables + options + alias)r
  }

  type ObjToMapConverter[T] = (T) => (String, Map[String, _])

  /** QueryBuilder methods **/
  override private[tresql] def newInstance(e: Env, depth: Int, idx: Int, chIdx: Int) =
    new ORT {
      override def env = e
      override private[tresql] def queryDepth = depth
      override private[tresql] var bindIdx = idx
      override private[tresql] def childIdx = chIdx
    }

  case class TableLink(table: String, refs: Set[String])
  case class ParentRef(table: String, ref: String)
  case class Property(
    tables: List[TableLink],
    insert: Boolean,
    update: Boolean,
    delete: Boolean,
    alias: String)
  case class SaveContext(
    name: String,
    struct: Map[String, Any],
    parents: List[ParentRef],
    filter: String,
    tables: List[TableLink],
    insertOption: Boolean,
    updateOption: Boolean,
    deleteOption: Boolean,
    alias: String,
    parent: String,
    table: metadata.Table,
    refToParent: String,
    pk: String)

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
          expr(
            if (idName != null) Map("ids" -> s.map(_(idName)).filter(_ != null))
            else Map[String, Any]())
        case m: Map[String, _] => expr(
          if (idName != null) Map("ids" -> m.get(idName).filter(_ != null).toList)
          else Map[String, Any]())
      }
    }
    override def defaultSQL = s"DeleteChildrenExpr($obj, $idName, $expr)"
  }
  case class NotDeleteIdsExpr(expr: Expr) extends BaseExpr {
    override def defaultSQL = env.get("ids").map {
      case ids: Seq[_] if !ids.isEmpty => expr.sql
      case _ => "true"
    }.getOrElse("true")
  }
  /* Expression is built from macro.
   * Effectively env.currId(idSeq, IdRefExpr(idRefSeq)())*/
  case class IdRefIdExpr(idRefSeq: String, idSeq: String) extends BaseVarExpr {
    val idRefExpr = IdRefExpr(idRefSeq)
    override def apply() = {
      val id = idRefExpr()
      env.currId(idSeq, id)
      id
    }
    override def toString = s"$idRefExpr#$idSeq"
  }

  def insert(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any = {
    save(name, obj, filter, insert_tresql, "Cannot insert data. Table not found for object: " + name)
  }

  def update(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources = Env): Any = {
    save(name, obj, filter, update_tresql,
      s"Cannot update data. Table not found or no primary key or no updateable columns found for the object: $name")
  }

  private def save(
    name: String,
    obj: Map[String, Any],
    filter: String,
    save_tresql_fun: SaveContext => String,
    errorMsg: String)
    (implicit resources: Resources): Any = {
    val struct = tresql_structure(obj)
    Env log (s"\nStructure: $name - $struct")
    val tresql = save_tresql(name, struct, Nil, filter, save_tresql_fun)
    if(tresql == null) error(errorMsg)
    build(tresql, obj, false)(resources)()
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
  def insertMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
    (implicit resources: Resources = Env): Any = insert(multiSaveProp(names), obj, filter)

  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
    (implicit resources: Resources = Env): Any = update(multiSaveProp(names), obj, filter)

  private def multiSaveProp(names: Seq[String])(implicit resources: Resources = Env) = {
    /* Returns zero or one imported key from table for each relation. In the case of multiple
     * imported keys pointing to the same relation the one specified after : symbol is chosen
     * or exception is thrown.
     * This is used to find relation columns for insert/update multiple methods.
     * Additionaly primary key of table is returned if it consist only of one column */
    def importedKeysAndPks(tableName: String, relations: List[String]) = {
      val x = tableName split ":"
      val table = resources.metaData.table(x.head)
      relations.foldLeft(x.tail.toSet) { (keys, rel) =>
        val relation = rel.split(":").head
        val refs = table.refs(relation).filter(_.cols.size == 1)
        (if (refs.size == 1) keys + refs.head.cols.head
        else if (refs.isEmpty || refs.exists(r => keys.contains(r.cols.head))) keys
        else error(s"Ambiguous refs: $refs from table ${table.name} to table $relation")) ++
        (table.key.cols match {case List(k) => Set(k) case _ => Set()})
      }
    }
    names.tail.foldLeft(List(names.head)) { (ts, t) => (t.split(":")
      .head + importedKeysAndPks(t, ts).mkString(":", ":", "")) :: ts
    }.reverse.mkString("#")
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

  private def tresql_structure[M <: Map[String, Any]](obj: M)(
    /* ensure that returned map is of the same type as passed.
     * For example in the case of ListMap when key ordering is important. */
    implicit bf: scala.collection.generic.CanBuildFrom[M, (String, Any), M]): M = {
    def merge(lm: Seq[Map[String, Any]]): Map[String, Any] =
      lm.tail.foldLeft(tresql_structure(lm.head))((l, m) => {
        val x = tresql_structure(m)
        l map (t => (t._1, (t._2, x.getOrElse(t._1, null)))) map {
          case (k, (v1: Map[String, _], v2: Map[String, _])) if !v1.isEmpty && !v2.isEmpty =>
            (k, merge(List(v1, v2)))
          case (k, (v1: Map[String, _], _)) if !v1.isEmpty => (k, v1)
          case (k, (_, v2: Map[String, _])) if !v2.isEmpty => (k, v2)
          case (k, (v1, _)) => (k, v1)
        }
      })
    obj.map { kv =>
      (kv._1, kv._2 match {
        case Seq() | Array() => Map()
        case l: Seq[Map[String, _]] => merge(l)
        case l: Array[Map[String, _]] => merge(l)
        case m: Map[String, Any] => tresql_structure(m)
        case x => x
      })
    }(bf.asInstanceOf[scala.collection.generic.CanBuildFrom[Map[String, Any], (String, Any), M]]) //somehow cast is needed
  }

  def save_tresql(
    name: String,
    struct: Map[String, Any],
    parents: List[ParentRef],
    filter: String,
    save_tresql_func: SaveContext => String)
    (implicit resources: Resources): String = {
    def parseProperty(name: String) = {
      def setLinks(name: String) =
        if (parents.isEmpty || name.indexOf("#") == -1) name else {
          val optionIdx = name.indexOf("[")
          if (optionIdx != -1) multiSaveProp(name.substring(0, optionIdx)
            .split("#")) + name.substring(optionIdx) else multiSaveProp(name.split("#"))
        }
      val PROP_PATTERN(tables, options, alias) = setLinks(name)
      //insert update delete option
      val (i, u, d) = Option(options).map (_ =>
        (options contains "+", options contains "=", options contains "-")
      ).getOrElse {(true, false, true)}
      Property((tables split "#").map{ t =>
        val x = t split ":"
        TableLink(x.head, x.tail.toSet)
      }.toList, i, u, d, alias)
    }
    val Property(tables, insertOption, updateOption, deleteOption, alias) =
      parseProperty(name)
    val parent = parents.headOption.map(_.table).orNull
    val md = resources.metaData
    //find first imported key to parent in list of table links passed as a param.
    def importedKeyOption(tables: List[TableLink]): Option[(metadata.Table, String)] = {
      def refInSet(refs: Set[String], child: metadata.Table) = refs.find(r =>
        child.refs(parent).filter(_.cols.size == 1).exists(_.cols.head == r))
      def imported_key_option(childTable: metadata.Table) =
        Option(childTable.refs(parent).filter(_.cols.size == 1)).flatMap {
          case Nil => None
          case List(ref) => ref.cols.headOption
          case x => error(
            s"""Ambiguous references from table '${childTable.name}' to table '$parent'.
             |Reference must be one and must consist of one column. Found: $x"""
              .stripMargin)
        }
      def processLinkedTables(linkedTables: List[TableLink]): Option[(metadata.Table, String)] =
        linkedTables match {
          case table :: tail => md.tableOption(table.table)
            .flatMap(t =>
              imported_key_option(t)
                .filterNot(table.refs.contains) //linked table has ref to parent
                .map((t, _))) orElse processLinkedTables(tail)
          case Nil => None //no ref to parent
        }
      md.tableOption(tables.head.table) //no parent no ref to parent
       .filter(_ => parent == null)
       .map((_, null)) orElse
      md.tableOption(tables.head.table) //first table has ref to parent
        .flatMap(table => (refInSet(tables.head.refs, table) orElse imported_key_option(table))
          .map((table, _))) orElse
      processLinkedTables(tables.tail) //look for ref to parent in linked tables
    }
    (for {
      (table, ref) <- importedKeyOption(tables)
      pk <- Some(table.key.cols).filter(_.size == 1).map(_.head) orElse Some(null)
    } yield save_tresql_func(SaveContext(name, struct, parents, filter, tables,
      insertOption, updateOption, deleteOption, alias, parent, table, ref, pk))
    ).orNull
  }

  private def insert_tresql(ctx: SaveContext)
    (implicit resources: Resources): String = {
    def table_save_tresql(tableName: String, alias: String,
      cols_vals: List[(String, String)], refsAndPk: Set[(String, String)],
      filter: String) = (cols_vals ++ refsAndPk) match {
        case cols_vals =>
          val (cols, vals) = cols_vals.unzip
          cols.mkString(s"+$tableName {", ", ", "}") +
          (vals.filter(_ != null) match {
            case vs if filter == null => vs.mkString(" [", ", ", "]")
            case vs =>
              val toa = if (alias == null) tableName else alias
              val cv = cols_vals filter (_._2 != null)
              val sel = s"(null{${cv.map(c => c._2 + " " + c._1).mkString(", ")}} @(1)) $toa"
              cv.map(c => s"$toa.${c._1}").mkString(s" $sel [$filter] {", ", ", "}")
          })
      }
    save_tresql_internal(ctx, table_save_tresql,
      save_tresql(_, _, _, null, insert_tresql))
  }

  private def update_tresql(ctx: SaveContext)
    (implicit resources: Resources): String = {
    def table_save_tresql(tableName: String, alias: String,
      cols_vals: List[(String, String)], refsAndPk: Set[(String, String)],
      filter: String) = cols_vals.unzip match {
        case (cols: List[String], vals: List[String]) if !cols.isEmpty =>
          val tn = tableName + (if (alias == null) "" else " " + alias)
          val updateFilter = refsAndPk.map(t=> s"${t._1} = ${t._2}")
            .mkString("[", " & ", s"${if (filter != null) s" & ($filter)" else ""}]")
          cols.mkString(s"=$tn $updateFilter {", ", ", "}") +
          vals.filter(_ != null).mkString("[", ", ", "]")
        case _ => null
    }
    import ctx._
    val tableName = table.name
    def upd: String = save_tresql_internal(ctx, table_save_tresql,
      save_tresql(_, _, _, null, update_tresql))
    def stripTrailingAlias(tresql: String, alias: String) =
      if (tresql != null && tresql.endsWith(alias))
        tresql.dropRight(alias.length) else tresql
    def delAllChildren = s"-$tableName[$refToParent = :#$parent]"
    def delMissingChildren =
      s"""_delete_children('$name', '$tableName', -${table
        .name}[$refToParent = :#$parent & _not_delete_ids($pk !in :ids)])"""
    def ins = save_tresql(name, struct, parents, null /*do not pass filter*/,
      insert_tresql)
    def insOrUpd = s"""|_insert_or_update('$tableName', ${
      stripTrailingAlias(ins, s" '$name'")}, ${
      stripTrailingAlias(upd, s" '$name'")}) '$name'"""
    if (parent == null) if (pk == null) null else upd
    else if (refToParent == pk) upd else
      if (pk == null) {
        (Option(deleteOption).filter(_ == true).map(_ => delAllChildren) ++
        Option(insertOption).filter(_ == true)
          .flatMap(_ => Option(ins))).mkString(", ")
      } else {
        (Option(deleteOption).filter(_ == true).map(_ =>
          if(!updateOption) delAllChildren else delMissingChildren) ++
        ((insertOption, updateOption) match {
          case (true, true) => Option(insOrUpd)
          case (true, false) => Option(ins)
          case (false, true) => Option(upd)
          case (false, false) => None
        })).mkString(", ")
      }
  }

  private def save_tresql_internal(
    ctx: SaveContext,
    table_save_tresql: (
      String, //table name
      String, //alias
      List[(String, String)], //cols vals
      Set[(String, String)], //refsPk & values
      String //filter
    ) => String,
    children_save_tresql: (
      String, //table property
      Map[String, Any], //saveable structure
      List[ParentRef] //parent chain
    ) => String)
    (implicit resources: Resources) = {
    def lookup_tresql(
      refColName: String,
      name: String,
      struct: Map[String, _])(implicit resources: Resources) =
      resources.metaData.tableOption(name).filter(_.key.cols.size == 1).map {
        table =>
        val pk = table.key.cols.headOption.filter(struct contains).orNull
        val insert = save_tresql(name, struct, Nil, null, insert_tresql)
        val update = save_tresql(name, struct, Nil, null, update_tresql)
        List(
          s":$refColName = |_lookup_edit('$refColName', ${
            if (pk == null) "null" else s"'$pk'"}, $insert, $update)",
          refColName -> resources.valueExpr(name, refColName))
    }.orNull
    import ctx._
    def tresql_string(table: metadata.Table,
      alias: String,
      refsAndPk: Set[(String, String)],
      children: List[String],
      filter: String,
      tresqlColAlias: String) = struct.flatMap {
      case (n, v) => v match {
        //children
        case o: Map[String, Any] => table.refTable.get(List(n)).map(lookupTable =>
          lookup_tresql(n, lookupTable, o)).getOrElse {
          List(children_save_tresql(n, o,
            ParentRef(table.name, refToParent) :: parents) -> null)
        }
        //pk or ref to parent
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
           .filter(_._1 != null /*check if prop->col mapping found*/) ++
             children.map(_ -> null)/*add same level one to one children*/)
           match {
             case x if x.isEmpty && refsAndPk.isEmpty => null //no columns & refs found
             case cols_vals => table_save_tresql(tableName, alias, cols_vals,
               refsAndPk, filter)
           }
        (for {
          base <- Option(tresql)
          tresql <- Option(lookupTresql).map(lookup =>
              s"([$lookup$base])") //put lookup in braces and array,
              //so potentially not to conflict with insert expr with multiple values arrays
            .orElse(Some(base))
        } yield tresql + tresqlColAlias).orNull
    }
    val md = resources.metaData
    val headTable = tables.head
    val linkedTables = tables.tail
    def idRefId(idRef: String, id: String) = s"_id_ref_id($idRef, $id)"
    def refsAndPk(tbl: metadata.Table, refs: Set[String]): Set[(String, String)] =
      //ref table (set fk and pk)
      (if (tbl.name == table.name && refToParent != null) if (refToParent == pk)
        Set(pk -> idRefId(parent, tbl.name)) else Set(refToParent -> s":#$parent") ++
          (if (pk == null || refs.contains(pk)) Set() else Set(pk -> s"#${tbl.name}"))
      //not ref table (set pk)
      else Option(tbl.key.cols)
        .filter(k=> k.size == 1 && !refs.contains(k.head)).map(_.head -> s"#${tbl.name}").toSet) ++
      //set refs
      (if(tbl.name == headTable.table) Set() else refs
          //filter pk of the linked table in case it matches refToParent
          .filterNot(tbl.name == table.name && _ == refToParent)
          .map(_ -> idRefId(headTable.table, tbl.name)))
    val linkedTresqls = for{ linkedTable <- linkedTables
      tableDef <- md.tableOption(linkedTable.table) } yield tresql_string(tableDef, alias,
          refsAndPk(tableDef, linkedTable.refs), Nil, null, "") //no children & do not pass filter
    md.tableOption(headTable.table).map {tableDef =>
      tresql_string(tableDef, alias, refsAndPk(tableDef, Set()),
        linkedTresqls.filter(_ != null), filter, Option(parent)
          .map(_ => s" '$name'").getOrElse(""))
    }.orNull
  }
}

object ORT extends ORT
