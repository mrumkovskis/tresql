package org.tresql

import sys._

/** Object Relational Transformations - ORT */
trait ORT extends Query {

  /**
  *  Children property format:
  *  table[:ref]*[#table[:ref]*]*[[options]] [alias][|insertFilterTresql, deleteFilterTresql, updateFilterTresql]
  *  Options are to be specified in such order: insert, delete, update, i.e. '+-='
  *  Examples:
  *    dept#car:deptnr:nr#tyres:carnr:nr
  *    dept[+=] alias
  *    emp[+-=] e|:deptno in currentUserDept(:current_user), null /* no statement */, e.deptno in currentUserDept(:current_user)
  */
  val PropPattern = {
    val ident = """[^:\[\]\s#]+"""
    val table = s"$ident(?::$ident)*"
    val tables = s"""($table(?:#$table)*)"""
    val options = """(?:\[(\+?-?=?)\])?"""
    val alias = """(?:\s+(\w+))?"""
    val filters = """(?:\|(.+))?"""
    (tables + options + alias + filters)r
  }
  /**
  *  Resolvable property key value format:
  *  key: "saveable_property->", value: "saveable_column=tresql_expression"
  *  Resolver tresqls where clause can contain placeholder _
  *  which will be replaced with bind variable ':saveable_property'.
  *  Statement must return no more than one row.
  *  Example:
  *    "dept_name->" -> "dept_id=dept[dname = _]{id}"
  */
  val ResolverPropPattern = "(.*?)->"r
  val ResolverExpPattern = "([^=]+)=(.*)"r

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
  case class Filters(insert: Option[String], delete: Option[String], update: Option[String])
  case class Property(
    tables: List[TableLink],
    insert: Boolean,
    update: Boolean,
    delete: Boolean,
    alias: String,
    filters: Option[Filters]
  )
  case class SaveContext(
    name: String,
    struct: Map[String, Any],
    parents: List[ParentRef],
    filters: Option[Filters],
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
      case m: Map[String @unchecked, Any @unchecked] =>
        if (idName != null && (m contains idName) && m(idName) != null) {
          val lookupObjId = m(idName)
          updateExpr(m)
          lookupObjId
        } else extractId(insertExpr(m))
      case null => null
      case x => error(s"Cannot set environment variables for the expression. $x is not a map.")
    }
    def extractId(result: Any) = result match {
      case i: InsertResult => i.id.get //insert expression
      case a: ArrayResult[_] => a.values.last match { case i: InsertResult => i.id.get } //array expression
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
        case s: Seq[Map[String, _] @unchecked] =>
          expr(
            if (idName != null) Map("ids" -> s.map(_.getOrElse(idName, null)).filter(_ != null))
            else Map[String, Any]())
        case m: Map[String @unchecked, _] => expr(
          if (idName != null) Map("ids" -> m.get(idName).filter(_ != null).toList)
          else Map[String, Any]())
      }
    }
    override def defaultSQL = s"DeleteChildrenExpr($obj, $idName, $expr)"
  }
  case class NotDeleteIdsExpr(expr: Expr) extends BaseExpr {
    override def defaultSQL = env.get("ids").map {
      case ids: Seq[_] if ids.nonEmpty => expr.sql
      case _ =>
        //add 'ids' bind variable to query builder so it can be bound later in the case
        expr.sql
        "true"
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
    Env log s"\nStructure: $name - $struct"
    val tresql = save_tresql(name, struct, Nil, filter, save_tresql_fun)
    if(tresql == null) error(errorMsg)
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  def delete(name: String, id: Any, filter: String = null, filterParams: Map[String, Any] = null)
  (implicit resources: Resources = Env): Any = {
    val Array(tableName, alias) = name.split("\\s+").padTo(2, null)
    (for {
      table <- resources.metadata.tableOption(tableName)
      pk <- table.key.cols.headOption
      if table.key.cols.size == 1
    } yield {
      val delete = s"-${table.name}${Option(alias).map(" " + _).getOrElse("")}[$pk = ?${Option(filter)
        .map(f => s" & ($f)").getOrElse("")}]"
      build(delete, Map("1" -> id) ++ Option(filterParams).getOrElse(Map()), reusableExpr = false)(resources)()
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
      val table = resources.metadata.table(x.head)
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
    implicit bf: scala.collection.generic.CanBuildFrom[Map[String, Any], (String, Any), M]): M = {
    def merge(lm: Seq[Map[String, Any]]): Map[String, Any] =
      lm.tail.foldLeft(tresql_structure(lm.head)) {(l, m) =>
        val x = tresql_structure(m)
        l map (t => (t._1, (t._2, x.getOrElse(t._1, null)))) map {
          case (k, (v1: Map[String @unchecked, _], v2: Map[String @unchecked, _])) if v1.nonEmpty && v2.nonEmpty =>
            (k, merge(List(v1, v2)))
          case (k, (v1: Map[String @unchecked, _], _)) if v1.nonEmpty => (k, v1)
          case (k, (_, v2: Map[String @unchecked, _])) if v2.nonEmpty => (k, v2)
          case (k, (v1, _)) => (k, v1 match {
            case _: Map[_, _] | _: Seq[_] => v1
            case _ if k endsWith "->" => v1
            case _ => "***"
          })
        }
      }
    var resolvableProps = Set[String]()
    val struct: M = obj.map { case (key, value) =>
      (key, value match {
        case Seq() | Array() => Map()
        case l: Seq[Map[String, _] @unchecked] => merge(l)
        case l: Array[Map[String, _] @unchecked] => merge(l)
        case m: Map[String @unchecked, Any @unchecked] => tresql_structure(m)
        case x =>
          //filter out properties which are duplicated because of resolver
          if (key endsWith "->") {
            resolvableProps += key.substring(0, key.length - "->".length)
            value
          } else "***"
      })
    } (bf)
    if (resolvableProps.isEmpty) struct
    else struct.flatMap { case (k, _) if resolvableProps(k) => Nil case x => List(x) } (bf)
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
      val PropPattern(tables, options, alias, filterStr) = setLinks(name)
      //insert update delete option
      val (i, u, d) = Option(options).map (_ =>
        (options contains "+", options contains "=", options contains "-")
      ).getOrElse {(true, false, true)}
      val filters = Option(filter).map {
        f => Filters(Option(f), Option(f), Option(f))
      } orElse {
        import QueryParser._
        Option(filterStr).flatMap(parseExp(_) match {
          case Arr(List(insert, delete, update)) => Some(ORT.this.Filters(
            insert = Some(insert).filter(_ != Null).map(_.tresql),
            delete = Some(delete).filter(_ != Null).map(_.tresql),
            update = Some(update).filter(_ != Null).map(_.tresql)
          ))
          case x => error(s"""Unrecognized filter declaration '$filterStr'.
            |Must consist of 3 comma separated tresql expressions: insertFilter, deleteFilter, updateFilter.
            |In the case expression is not needed it must be set to 'null'.""".stripMargin)
        })
      }
      Property((tables split "#").map{ t =>
        val x = t split ":"
        TableLink(x.head, x.tail.toSet)
      }.toList, i, u, d, alias, filters)
    }
    val Property(tables, insertOption, updateOption, deleteOption, alias, filters) =
      parseProperty(name)
    val parent = parents.headOption.map(_.table).orNull
    val md = resources.metadata
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
        .map((_, null))
        .orElse {
          if (tables.head.refs.nonEmpty) //ref to parent in first table found must be resolved!
            md.tableOption(tables.head.table).flatMap(table => refInSet(tables.head.refs, table).map(table -> _))
          else
            processLinkedTables(tables) //search for ref to parent
        }
    }
    (for {
      (table, ref) <- importedKeyOption(tables)
      pk <- Some(table.key.cols).filter(_.size == 1).map(_.head) orElse Some(null)
    } yield
        save_tresql_func(SaveContext(name, struct, parents, filters, tables,
          insertOption, updateOption, deleteOption, alias, parent, table, ref, pk)
        )
    ).orNull
  }

  private def insert_tresql(ctx: SaveContext)
    (implicit resources: Resources): String = {
    def table_save_tresql(tableName: String, alias: String,
      cols_vals: List[(String, String)],
      refsAndPk: Set[(String, String)]) = cols_vals ++ refsAndPk match {
        case cols_vals =>
          val (cols, vals) = cols_vals.unzip
          cols.mkString(s"+$tableName {", ", ", "}") +
            (for {
              filters <- ctx.filters
              filter <- filters.insert
            } yield {
              val toa = if (alias == null) tableName else alias
              val cv = cols_vals filter (_._2 != null)
              val sel = s"(null{${cv.map(c => c._2 + " " + c._1).mkString(", ")}} @(1)) $toa"
              cv.map(c => s"$toa.${c._1}").mkString(s" $sel [$filter] {", ", ", "}")
            }).getOrElse(vals.filter(_ != null).mkString(" [", ", ", "]"))
      }
    save_tresql_internal(ctx, table_save_tresql,
      save_tresql(_, _, _, null, insert_tresql)) //do not pass filter since it may come from child property
  }

  private def update_tresql(ctx: SaveContext)
    (implicit resources: Resources): String = {
    def table_save_tresql(tableName: String, alias: String,
      cols_vals: List[(String, String)],
      refsAndPk: Set[(String, String)]) = cols_vals.unzip match {
        case (cols: List[String], vals: List[String]) if cols.nonEmpty =>
          val filter = ctx.filters.flatMap(_.update).map(f => s" & ($f)").getOrElse("")
          val tn = tableName + (if (alias == null) "" else " " + alias)
          val updateFilter = refsAndPk.map(t=> s"${t._1} = ${t._2}").mkString("[", " & ", s"$filter]")
          cols.mkString(s"=$tn $updateFilter {", ", ", "}") +
          vals.filter(_ != null).mkString("[", ", ", "]")
        case _ => null
    }
    import ctx._
    val tableName = table.name
    def upd: String = save_tresql_internal(ctx, table_save_tresql, save_tresql(
      _, _, _, null /* do not pass filter since it may come from child property */, update_tresql))
    def stripTrailingAlias(tresql: String, alias: String) =
      if (tresql != null && tresql.endsWith(alias))
        tresql.dropRight(alias.length) else tresql
    val delFilter = ctx.filters.flatMap(_.delete).map(f => s" & ($f)").getOrElse("")
    def delAllChildren = s"-$tableName[$refToParent = :#$parent$delFilter]"
    def delMissingChildren =
      s"""_delete_children('$name', '$tableName', -${table
        .name}[$refToParent = :#$parent & _not_delete_ids($pk !in :ids)$delFilter])"""
    val insFilter = ctx.filters.flatMap(_.insert).orNull
    def ins = save_tresql(name, struct, parents, insFilter, insert_tresql)
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
      Set[(String, String)] //refsPk & values
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
      resources.metadata.tableOption(name).filter(_.key.cols.size == 1).map {
        table =>
          val pk = table.key.cols.headOption.filter(struct contains).orNull
          val insert = save_tresql(name, struct, Nil, null, insert_tresql)
          val update = save_tresql(name, struct, Nil, null, update_tresql)
          List(
            s":$refColName = |_lookup_edit('$refColName', ${
              if (pk == null) "null" else s"'$pk'"}, $insert, $update)",
            refColName -> resources.valueExpr(name, refColName))
      }.orNull
    def resolver_tresql(table: metadata.Table, property: String, resolverExp: String) = {
      import QueryParser._
      val ResolverPropPattern(prop) = property
      val ResolverExpPattern(col, exp) = resolverExp
      table.colOption(col).map(_.name).map { _ -> transformer {
          case Ident(List("_")) => Variable(prop, Nil, opt = false)
        } (parseExp(if (exp startsWith "(" ) exp else s"($exp)")).tresql
      }// TODO .orElse(sys.error(s"Resolver target not found for property '$prop'. Column '$col', expression '$resolverExp'"))
       .toList
    }
    import ctx._
    def tresql_string(
      table: metadata.Table,
      alias: String,
      refsAndPk: Set[(String, String)],
      children: List[String],
      tresqlColAlias: String
    ) = struct.flatMap {
          case (n, v) => v match {
            //children
            case o: Map[String @unchecked, Any @unchecked] => table.refTable.get(List(n)).map(lookupTable =>
              lookup_tresql(n, lookupTable, o)).getOrElse {
              List(children_save_tresql(n, o,
                ParentRef(table.name, refToParent) :: parents) -> null)
            }
            //pk or ref to parent
            case _ if refsAndPk.exists(_._1 == n) => Nil
            //resolvable field check
            case v: String if n.indexOf("->") != -1 => resolver_tresql(table, n, v)
            //ordinary field
            case _ => List(table.colOption(n).map(_.name).orNull -> resources.valueExpr(table.name, n))
          }
        }.groupBy { case _: String => "l" case _ => "b"} match {
          case m: Map[String @unchecked, List[_] @unchecked] =>
            val tableName = table.name
            //lookup edit tresql
            val lookupTresql = m.get("l").map(_.asInstanceOf[List[String]].map(_ + ", ").mkString).orNull
            //base table tresql
            val tresql =
              m.getOrElse("b", Nil).asInstanceOf[List[(String, String)]]
                .filter(_._1 != null /*check if prop->col mapping found*/) ++
                children.map(_ -> null) /*add same level one to one children*/
               match {
                 case x if x.isEmpty && refsAndPk.isEmpty => null //no columns & refs found
                 case cols_vals => table_save_tresql(tableName, alias, cols_vals, refsAndPk)
               }
            (for {
              base <- Option(tresql)
              tresql <- Option(lookupTresql).map(lookup =>
                  s"([$lookup$base])") //put lookup in braces and array,
                  //so potentially not to conflict with insert expr with multiple values arrays
                .orElse(Some(base))
            } yield tresql + tresqlColAlias).orNull
        }
    val md = resources.metadata
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
          refsAndPk(tableDef, linkedTable.refs), Nil, "") //no children
    md.tableOption(headTable.table).map {tableDef =>
      tresql_string(tableDef, alias, refsAndPk(tableDef, Set()),
        linkedTresqls.filter(_ != null), Option(parent)
          .map(_ => s" '$name'").getOrElse(""))
    }.orNull
  }
}

object ORT extends ORT
