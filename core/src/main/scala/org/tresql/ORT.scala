package org.tresql

import org.tresql.OrtMetadata.{Filters, KeyValue, LookupViewValue, SaveOptions, SaveTo, TresqlValue, View, ViewValue}

import scala.util.matching.Regex
import sys._

/** Object Relational Transformations - ORT */
trait ORT extends Query {

  type ObjToMapConverter[T] = T => (String, Map[String, _])

  /** QueryBuilder methods **/
  override private[tresql] def newInstance(e: Env, depth: Int, idx: Int, chIdx: Int) =
    new ORT {
      override def env = e
      override private[tresql] def queryDepth = depth
      override private[tresql] var bindIdx = idx
      override private[tresql] def childIdx = chIdx
    }

  case class ParentRef(table: String, ref: String)
  sealed trait KeyPart { def name: String }
  case class KeyCol(name: String) extends KeyPart
  case class RefKeyCol(name: String) extends KeyPart
  case class SaveContext(name: String,
                         view: View,
                         parents: List[ParentRef],
                         table: metadata.Table,
                         saveTo: SaveTo,
                         refToParent: String)
  case class ColVal(col: String, value: String, forInsert: Boolean, forUpdate: Boolean)
  case class SaveData(table: String,
                      pk: List[String],
                      alias: String,
                      colsVals: List[ColVal],
                      refsPkVals: Set[(String, String)],
                      keyVals: Seq[(KeyPart, String)],
                      filters: Option[Filters],
                      upsert: Boolean,
                      db: String)


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
  /** Expression is built from macros to ensure ORT children editing
   * Differs from {{{UpsertExpr}}} in a way that if pk value is not available in the environment do directly insert
   * statement not trying to execute update statement before.
   * */
  case class UpdateOrInsertExpr(table: String, updateExpr: Expr, insertExpr: Expr)
  extends BaseExpr {
    val idName = env.table(table).key.cols.headOption.orNull
    private val upsertExpr = UpsertExpr(updateExpr, insertExpr)
    override def apply() =
      if (idName != null && env.containsNearest(idName) && env(idName) != null)
        upsertExpr() else insertExpr()
    def defaultSQL = s"InsertOrUpdateExpr($idName, $updateExpr, $insertExpr)"
  }
  /**
   * Try to execute {{{updateExpr}}} and if no rows affected execute {{{insertExpr}}}
   * Expression is built from macros to ensure ORT one to one relationship setting
   * */
  case class UpsertExpr(updateExpr: Expr, insertExpr: Expr) extends BaseExpr {
    override def apply() = updateExpr() match {
      case r: DMLResult if r.count.getOrElse(0) > 0 || r.children.nonEmpty => r // return result if update affects some row
      case _ => insertExpr() // execute insert if update affects no rows
    }
    def defaultSQL = s"UpsertExpr($updateExpr, $insertExpr)"
  }
  /* Expression is built from macros to ensure ORT children editing */
  case class DeleteMissingChildrenExpr(obj: String,
                                       key: List[String],
                                       key_val_exprs: List[Expr],
                                       delExpr: Expr)
  extends BaseExpr {
    override def apply() = {
      // clear bind variables in the case expression is repeatedly executed and bind variable count has changed
      delExpr.builder.clearRegisteredBindVariables()
      key_val_exprs match {
        case List(_: VarExpr | _: IdExpr) =>
          delExpr(Map("keys" -> {
            env(obj) match {
              case s: Seq[Map[String, _] @unchecked] =>
                s.map(_.getOrElse(key.head, null)).filter(_ != null)
              case m: Map[String @unchecked, _] =>
                List(m.getOrElse(key.head, null))
            }
          }))
        case _ =>
          delExpr(Map("keys" -> {
            env(obj) match {
              case s: Seq[Map[String, _] @unchecked] => s
              case m: Map[String @unchecked, _] =>
                List(m.getOrElse(key.head, null))
            }
          }))
      }
    }
    override def defaultSQL = s"DeleteChildrenExpr($obj, $key, $delExpr)"
  }
  case class NotDeleteKeysExpr(key: List[Expr], key_val_exprs: List[Expr]) extends BaseExpr {
    override def defaultSQL = env.get("keys").map {
      case keyVals: Seq[_] if keyVals.isEmpty => "true"
      case keyVals: Seq[_] => key_val_exprs match {
        case List(_: VarExpr | _: IdExpr) =>
          InExpr(key.head, List(VarExpr("keys", Nil, false)), true).sql
        case _ =>
          def comp(key_part_and_expr: (Expr, Expr), i: Int) =
            BinExpr("=", key_part_and_expr._1, transform(key_part_and_expr._2, {
              case VarExpr(n, Nil, opt) => VarExpr("keys", List(i.toString, n), opt)
            }))
          def and(l: List[(Expr, Expr)], i: Int): Expr = l match {
            case Nil  => null
            case k :: Nil => comp(k, i)
            case k :: tail => BracesExpr(BinExpr("&", comp(k, i), and(tail, i)))
          }
          def or(k: List[(Expr, Expr)], i: Int): Expr =
            if (i < 0) null
            else if (i == 0) and(k, i)
            else BinExpr("|", and(k, i), or(k, i - 1))
          val kvs = keyVals.size
          val orRes = or(key zip key_val_exprs, kvs - 1)
          UnExpr("!", if (kvs > 1 || key.size == 1) BracesExpr(orRes) else orRes).sql
      }
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

  case class IdByKeyExpr(idExpr: Expr) extends BaseExpr {
    import CoreTypes._
    override def apply(): Any = {
      idExpr() match {
        case r: Result[_] => r.headOption[Any].orNull
        case _ => null
      }
    }
    override def defaultSQL: String = s"IdByKeyExpr($idExpr)"
  }

  case class UpdateByKeyExpr(table: String, setIdExpr: Expr, updateExpr: Expr) extends BaseExpr {
    override def apply(): Any = {
      setIdExpr()
      env.table(table).key.cols.headOption
        .flatMap(env.get)
        .collect { case id if id != null => updateExpr() }
        .getOrElse(new UpdateResult()) // empty update result
    }
    override def defaultSQL: String = s"UpdateByKeyExpr($table, $setIdExpr, $updateExpr)"
  }

  def insert(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources): Any = {
    val name_with_filter = if (filter == null) name else s"$name|$filter, null, null"
    save(name_with_filter, obj, insert_tresql, "Cannot insert data. Table not found for object: " + name)
  }

  def update(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources): Any = {
    val name_with_filter = if (filter == null) name else s"$name|null, null, $filter"
    save(name_with_filter, obj, update_tresql,
      s"Cannot update data. Table not found or no primary key or no updateable columns found for the object: $name")
  }

  def delete(name: String, id: Any, filter: String = null, filterParams: Map[String, Any] = null)
            (implicit resources: Resources): Any = {
    val OrtMetadata.Patterns.prop(db, tableName, _, alias, _) = name
    val md = tresqlMetadata(db)
    val delete =
      (for {
        table <- md.tableOption(tableName)
        pk <- table.key.cols.headOption
        if table.key.cols.size == 1
      } yield {
        s"-${tableWithDb(db, table.name)}${Option(alias).map(" " + _).getOrElse("")}[$pk = ?${Option(filter)
          .map(f => s" & ($f)").getOrElse("")}]"
      }) getOrElse {
        error(s"Table $name not found or table primary key not found or table primary key consists of more than one column")
      }
    build(delete, Map("1" -> id) ++ Option(filterParams).getOrElse(Map()),
      reusableExpr = false)(resources)()
  }

  /** insert methods to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def insertMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
                    (implicit resources: Resources): Any = insert(names.mkString("#"), obj, filter)

  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
                    (implicit resources: Resources): Any = update(names.mkString("#"), obj, filter)

  //object methods
  def insertObj[T](obj: T, filter: String = null)(
    implicit resources: Resources, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    insert(v._1, v._2, filter)
  }
  def updateObj[T](obj: T, filter: String = null)(
    implicit resources: Resources, conv: ObjToMapConverter[T]): Any = {
    val v = conv(obj)
    update(v._1, v._2, filter)
  }

  def insert(metadata: View, obj: Map[String, Any])(implicit resources: Resources): Any = {
    val tresql = insertTresql(metadata)
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  def update(metadata: View, obj: Map[String, Any])(implicit resources: Resources): Any = {
    val tresql = updateTresql(metadata)
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  def save(metadata: View, obj: Map[String, Any])(implicit resources: Resources): Any = {
    val tresql = s"_upsert(${updateTresql(metadata)}, ${insertTresql(metadata)})"
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  def delete(name: String, key: List[String], params: Map[String, Any], filter: String)(implicit
                                                                                        resources: Resources): Any = {
    val tresql = deleteTresql(name, key, filter)
    build(tresql, params, reusableExpr = false)(resources)()
  }

  def insertTresql(metadata: View)(implicit resources: Resources): String = {
    save_tresql(null, metadata, Nil, insert_tresql)
  }

  def updateTresql(metadata: View)(implicit resources: Resources): String = {
    save_tresql(null, metadata, Nil, update_tresql)
  }

  def deleteTresql(name: String, key: List[String], filter: String)(implicit resources: Resources): String = {
    val OrtMetadata.Patterns.prop(db, tableName, _, alias, _) = name
    s"-${tableWithDb(db, tableName)}${if (alias == null) "" else " " + alias}[${
      key.map(c => c + " = :" + c).mkString(" & ")}${if (filter == null) "" else s" & ($filter)"}]"
  }

  private def save(name: String,
                   obj: Map[String, Any],
                   save_tresql_fun: SaveContext => String,
                   errorMsg: String)(implicit resources: Resources): Any = {
    val view = ortMetadata(name, obj)
    resources log s"$view"
    val tresql = save_tresql(null, view, Nil, save_tresql_fun)
    if(tresql == null) error(errorMsg)
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  /** This method is for debugging purposes. */
  def ortMetadata(tables: String, saveableMap: Map[String, Any])(implicit resources: Resources): View = {
    def tresql_structure(obj: Map[String, Any]): Map[String, Any] = {
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
      val struct: Map[String, Any] = obj.map { case (key, value) =>
        (key, value match {
          case Seq() | Array() => Map()
          case l: Seq[Map[String, _] @unchecked] => merge(l)
          case l: Array[Map[String, _] @unchecked] => merge(l.toIndexedSeq)
          case m: Map[String, Any] @unchecked => tresql_structure(m)
          case x =>
            //filter out properties which are duplicated because of resolver
            if (key endsWith "->") {
              resolvableProps += key.substring(0, key.length - "->".length)
              value
            } else "***"
        })
      }
      if (resolvableProps.isEmpty) struct
      else struct.flatMap { case (k, _) if resolvableProps(k) => Nil case x => List(x) }
    }
    def parseTables(name: String) = {
      def saveTo(tables: String, md: Metadata) = {
        def multiSaveProp(names: Seq[String]) = {
          /* Returns zero or one imported key from table for each relation. In the case of multiple
           * imported keys pointing to the same relation the one specified after : symbol is chosen
           * or exception is thrown.
           * This is used to find relation columns for insert/update multiple methods.
           * Additionaly primary key of table is returned if it consist only of one column */
          def importedKeysAndPks(tableName: String, relations: List[String]) = {
            val x = tableName split ":"
            val table = md.table(x.head)
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
          }.reverse
        }
        tables.split("#").map { t =>
          val ki = t.indexOf("[")
          if (ki != -1) {
            (t.substring(0, ki), t.substring(ki + 1, t.length - 1).split("\\s*,\\s*").toList)
          } else (t, Nil)
        }.unzip match {
          case (names, keys) => multiSaveProp(names.toIndexedSeq) zip keys map { case (table, key) =>
            val t = table.split(":")
            SaveTo(t.head, t.tail.toSet, key)
          }
        }
      }
      val OrtMetadata.Patterns.prop(db, tables, options, alias, filterStr) = name
      //insert update delete option
      val (i, u, d) = Option(options).map (_ =>
        (options contains "+", options contains "=", options contains "-")
      ).getOrElse {(true, false, true)}
      import parsing.{Arr, Null}
      val filters =
        Option(filterStr).flatMap(new QueryParser(resources, resources.cache).parseExp(_) match {
          case Arr(List(insert, delete, update)) => Some(Filters(
            insert = Some(insert).filter(_ != Null).map(_.tresql),
            delete = Some(delete).filter(_ != Null).map(_.tresql),
            update = Some(update).filter(_ != Null).map(_.tresql)
          ))
          case _ => error(s"""Unrecognized filter declaration '$filterStr'.
                             |Must consist of 3 comma separated tresql expressions: insertFilter, deleteFilter, updateFilter.
                             |In the case expression is not needed it must be set to 'null'.""".stripMargin)
        })
      View(saveTo(tables, tresqlMetadata(db)), SaveOptions(i, u, d), filters, alias, Nil, db)
    }
    def resolver_tresql(property: String, resolverExp: String) = {
      import parsing._
      val OrtMetadata.Patterns.resolverProp(prop) = property
      val OrtMetadata.Patterns.resolverExp(col, exp) = resolverExp
      val parser = new QueryParser(resources, resources.cache)
      OrtMetadata.Property(col, TresqlValue(
        parser.transformer {
          case Ident(List("_")) => Variable(prop, Nil, opt = false)
        } (parser.parseExp(if (exp startsWith "(" ) exp else s"($exp)")).tresql, true, true
      ))
    }
    val struct = tresql_structure(saveableMap)
    val props =
      struct.map {
        case (prop, value) => value match {
          case m: Map[String, Any]@unchecked => OrtMetadata.Property(prop, ViewValue(ortMetadata(prop, m)))
          case v: String if prop.indexOf("->") != -1 => resolver_tresql(prop, v)
          case _ => OrtMetadata.Property(prop, TresqlValue(s":$prop", true, true))
        }
      }.toList
    parseTables(tables).copy(properties = props)
  }

  private def tresqlMetadata(db: String)(implicit resources: Resources) =
    if (db == null) resources.metadata else resources.extraResources(db).metadata

  private def save_tresql(name: String,
                          view: View,
                          parents: List[ParentRef],
                          save_tresql_func: SaveContext => String)(implicit
                                                                   resources: Resources): String = {
    val parent = parents.headOption.map(_.table).orNull
    val md = tresqlMetadata(view.db)
    //find first imported key to parent in list of table links passed as a param.
    def importedKeyOption(tables: List[SaveTo]): Option[(metadata.Table, SaveTo, String)] = {
      def processLinkedTables(linkedTables: List[SaveTo]): Option[(metadata.Table, SaveTo, String)] = {
        def imported_key_option(childTable: metadata.Table) =
          Option(childTable.refs(parent).filter(_.cols.size == 1)).flatMap {
            case Nil => None
            case List(ref) => ref.cols.headOption
            case x => error(
              s"""Ambiguous references from table '${childTable.name}' to table '$parent'.
                 |Reference must be one and must consist of one column. Found: $x"""
                .stripMargin)
          }
        linkedTables match {
          case st :: tail => md.tableOption(st.table)
            .flatMap(t =>
              imported_key_option(t)
                .filterNot(st.refs.contains) //linked table has ref to parent
                .map((t, st, _))) orElse processLinkedTables(tail)
          case Nil => None //no ref to parent
        }
      }
      val st = tables.head
      md.tableOption(st.table) //no parent no ref to parent
        .filter(_ => parent == null)
        .map((_, st, null))
        .orElse {
          if (st.refs.nonEmpty) //ref to parent in first table found must be resolved!
            md.tableOption(st.table).flatMap(table =>
              st.refs.
                find(r => table.refs(parent).filter(_.cols.size == 1).exists(_.cols.head == r))
                .map((table, st, _)))
          else processLinkedTables(tables) //search for ref to parent
        }
    }

    (for {
      (table, st, ref) <- importedKeyOption(view.saveTo)
    } yield
        save_tresql_func(SaveContext(name, view, parents, table, st, ref))
    ).orNull
  }

  private def tableWithDb(db: String, table: String) = {
    (if (db == null) "" else db + ":") + table
  }

  private def table_insert_tresql(saveData: SaveData) = {
    import saveData._
    colsVals.filter(_.forInsert).map(cv => cv.col -> cv.value) ++ refsPkVals match {
      case cols_vals =>
        val (cols, vals) = cols_vals.unzip
        cols.mkString(s"+${tableWithDb(db, table)} {", ", ", "}") +
          (for {
            filters <- saveData.filters
            filter <- filters.insert
          } yield {
            val toa = if (alias == null) table else alias
            val cv = cols_vals filter (_._2 != null)
            val sel = s"(null{${cv.map(c => c._2 + " " + c._1).mkString(", ")}} @(1)) $toa"
            cv.map(c => s"$toa.${c._1}").mkString(s" $sel [$filter] {", ", ", "}")
          }).getOrElse(vals.filter(_ != null).mkString(" [", ", ", "]"))
    }
  }

  private def insert_tresql(ctx: SaveContext)(implicit resources: Resources): String = {
    save_tresql_internal(ctx, table_insert_tresql, save_tresql(_, _, _, insert_tresql))
  }

  private def update_tresql(ctx: SaveContext)(implicit resources: Resources): String = {

    def filterString(filters: Option[Filters], extraction: Filters => Option[String]): String =
      filters.flatMap(extraction).map(f => s" & ($f)").getOrElse("")

    import ctx._
    val parent = parents.headOption.map(_.table).orNull
    val tableName = table.name

    def delAllChildren =
      s"-${tableWithDb(view.db, tableName)}[$refToParent = :#$parent${filterString(ctx.view.filters, _.delete)}]"
    def delMissingChildren = {
      def del_children_tresql(data: SaveData) = {
        val (refCols, keyCols) =
          if (data.keyVals.nonEmpty) data.keyVals.partition(_._1.isInstanceOf[RefKeyCol]) match {
            case (refCols: List[(RefKeyCol, String)@unchecked], keyCols: List[(KeyCol, String)@unchecked]) =>
              (refCols.map { case (k, v) => (k.name, v) }, keyCols.map { case (k, v) => (k.name, v) })
          } else {
            val pk = data.pk.headOption.orNull
            data.refsPkVals.partition(_._1 != pk) match {
              case (refs: Set[(String, String)@unchecked], pk: Set[(String, String)@unchecked]) =>
                (refs.toList, pk.toList)
            }
          }
          val refColsFilter = refCols.map { case (n, v) => s"$n = $v"}.mkString("&")
          val (key_arr, key_val_expr_arr) = keyCols.unzip match {
            case (kc, kv) => (s"[${kc.mkString(", ")}]", s"[${kv.mkString(", ")}]")
          }
          val filter = filterString(data.filters, _.delete)
          if (refCols.isEmpty || keyCols.isEmpty) null
          else
            s"""_delete_missing_children('$name', $key_arr, $key_val_expr_arr, -${tableWithDb(data.db,
              data.table)}[$refColsFilter & _not_delete_keys($key_arr, $key_val_expr_arr)$filter])"""
      }
      save_tresql_internal(ctx.copy(view = ctx.view.copy(saveTo = List(saveTo))),
        del_children_tresql, null)
    }
    def upd = {
      def table_save_tresql(data: SaveData) = {
        import data._
        val filteredColsVals =  // filter out key columns from updatable columns where value match key search value
          (if (keyVals.nonEmpty)
            colsVals.collect {
              case cv @ ColVal(c, v, _, true)
                if !keyVals.exists { case (k, kv) => k.name == c && v == kv } => cv
            }
          else colsVals.filter(_.forUpdate))
            .map(cv => cv.col -> cv.value)
        val (upd_tresql, hasChildren) = Option(filteredColsVals.unzip match {
          case (cols: List[String], vals: List[String]) if cols.nonEmpty =>
            val tn = table + (if (alias == null) "" else " " + alias)
            val filter = filterString(data.filters, _.update)
            val filteredVals = vals.filter(_ != null) // filter out child queries vals
            val hasChildren = cols.size > filteredVals.size
            def updFilter(f: Iterable[(String, String)]) = f.map(fc => s"${fc._1} = ${fc._2}")
            val updateFilter =
              updFilter(if (keyVals.nonEmpty && !hasChildren) keyVals.map(kp => (kp._1.name, kp._2)) else refsPkVals)
                .mkString("[", " & ", s"$filter]")
            (cols.mkString(s"=${tableWithDb(db, tn)} $updateFilter {", ", ", "}") +
              filteredVals.mkString("[", ", ", "]"), hasChildren)
          case _ => (null, false)
        }).filter(_._1 != null)
          .map { case ut_hc@(ut, hc) =>
            if (upsert) {
              (s"""|_upsert($ut, ${table_insert_tresql(data)})""", hc)
            } else ut_hc
          }
          .getOrElse((null, false))

        if (data.pk.size == 1 && keyVals.nonEmpty && hasChildren) { // if has key and children calculate and set pk for children use
          val pr_col = data.pk.head
          s"|${if (db != null) db + ":" else ""}_update_by_key($table, :$pr_col = _id_by_key(|${
            tableWithDb(db, table)}[${keyVals.map(kv => s"${kv._1.name} = ${kv._2}")
            .mkString(" & ")}]{$pr_col}), $upd_tresql)"
        } else upd_tresql
      }
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, update_tresql))
    }
    def ins = {
      def table_save_tresql(data: SaveData) = {
        val ndata =
          if (data.filters.flatMap(_.update).nonEmpty) {
            Option(
              if (data.keyVals.nonEmpty) data.keyVals.map(kv => s"${kv._1.name} = ${kv._2}").mkString(" & ")
              else table.key.cols match { case List(c) => s"$c = #$tableName" case _ => null }
            )
              .map(insf => s"!exists($tableName[$insf]{1})${filterString(data.filters, _.insert)}")
              .map(f => data.copy(filters = Some(Filters(insert = Some(f)))))
              .getOrElse(data)
          } else data
        table_insert_tresql(ndata)
      }
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, insert_tresql))
    }
    def updOrIns = {
      if (saveTo.key.nonEmpty)
        s"|_upsert($upd, $ins)" //upsert for save by key since primary key is not accessible
      else
        s"""|${if (view.db != null) view.db + ":" else ""}_update_or_insert('$tableName', $upd, $ins)"""
    }

    import view.options._
    val pk = table.key.cols
    if (parent == null) if (saveTo.key.isEmpty && pk.isEmpty) null else upd
    else if (pk.size == 1 && refToParent == pk.head) upd else
      if (saveTo.key.isEmpty && pk.isEmpty) {
        (Option(doDelete).filter(_ == true).map(_ => delAllChildren) ++
          Option(doInsert).filter(_ == true)
            .flatMap(_ => Option(ins))).mkString(", ")
      } else {
        (Option(doDelete).filter(_ == true).map(_ =>
          if(!doUpdate) delAllChildren else delMissingChildren) ++
          ((doInsert, doUpdate) match {
            case (true, true) => Option(updOrIns)
            case (true, false) => Option(ins)
            case (false, true) => Option(upd)
            case (false, false) => None
          })).mkString(", ")
      }
  }

  private def save_tresql_internal(
    ctx: SaveContext,
    table_save_tresql: SaveData => String,
    children_save_tresql: (
      String, //table property
      View,
      List[ParentRef] //parent chain
    ) => String)
    (implicit resources: Resources) = {

    def tresql_string(
      table: metadata.Table,
      alias: String,
      refsPkAndKey: (Set[(String, String)], Seq[(KeyPart, String)]),
      children: List[String],
      upsert: Boolean
    ) = {
      def lookup_tresql(view: View,
                        refColName: String)(implicit resources: Resources) = {
        view.saveTo.headOption.map(_.table).flatMap(tresqlMetadata(view.db).tableOption).map {
          table =>
            val pk = table.key.cols.headOption.filter(pk => view.properties
              .exists { case OrtMetadata.Property(col, _) => pk == col case _ => false }).orNull
            val insert = save_tresql(null, view, Nil, insert_tresql)
            val update = save_tresql(null, view, Nil, update_tresql)
            List(
              s":$refColName = |_lookup_edit('$refColName', ${
                if (pk == null) "null" else s"'$pk'"}, $insert, $update)",
              ColVal(refColName, s":$refColName", true, true))
        }.orNull
      }

      val (refsAndPk, key) = refsPkAndKey
      ctx.view.properties.flatMap {
        case OrtMetadata.Property(col, _) if refsAndPk.exists(_._1 == col) => Nil
        case OrtMetadata.Property(col, KeyValue(_, valueTresql)) =>
          List(ColVal(table.colOption(col).map(_.name).orNull, valueTresql, true, true))
        case OrtMetadata.Property(col, TresqlValue(tresql, forInsert, forUpdate)) =>
          List(ColVal(table.colOption(col).map(_.name).orNull, tresql, forInsert, forUpdate))
        case OrtMetadata.Property(prop, ViewValue(v)) =>
          if (children_save_tresql != null) {
            table.refTable.get(List(prop)).map(lookupTable => // FIXME perhaps take lookup table from metadata since 'prop' may not match fk name
              lookup_tresql(v.copy(saveTo = List(SaveTo(lookupTable, Set(), Nil))), prop)).getOrElse {
              val chtresql = children_save_tresql(prop, v, ParentRef(table.name, ctx.refToParent) :: ctx.parents)
              List(ColVal(Option(chtresql).map(_ + s" '$prop'").orNull, null, true, true))
            }
          } else Nil
        case OrtMetadata.Property(refColName, LookupViewValue(v)) =>
          if (children_save_tresql != null) {
            lookup_tresql(v, refColName)
          } else Nil
      }.partition(_.isInstanceOf[String]) match {
        case (lookups: List[String@unchecked], colsVals: List[ColVal@unchecked]) =>
          val tableName = table.name
          //lookup edit tresql
          val lookupTresql = Option(lookups).filter(_.nonEmpty).map(_.map(_ + ", ").mkString)
          //base table tresql
          val tresql = colsVals.filter(_.col != null /*check if prop->col mapping found*/) ++
              children.map(c => ColVal(c, null, true, true)) /*add same level one to one children*/
            match {
              case x if x.isEmpty && refsAndPk.isEmpty => null //no columns & refs found
              case cols_vals =>
                table_save_tresql(SaveData(tableName,
                  table.key.cols, alias, cols_vals, refsAndPk, key, ctx.view.filters, upsert, ctx.view.db))
            }

          (for {
            base <- Option(tresql)
            tresql <- lookupTresql.map(lookup =>
              s"([$lookup$base])") //put lookup in braces and array,
              //so potentially not to conflict with insert expr with multiple values arrays
              .orElse(Some(base))
          } yield tresql).orNull
      }
    }

    val md = tresqlMetadata(ctx.view.db)
    val headTable = ctx.view.saveTo.head
    val parent = ctx.parents.headOption.map(_.table).orNull
    def refsPkAndKey(tbl: metadata.Table,
                     refs: Set[String],
                     key: Seq[String]): (Set[(String, String)], Seq[(KeyPart, String)]) = {
      def idRefId(idRef: String, id: String) = s"_id_ref_id($idRef, $id)"
      val pk = ctx.table.key.cols match { case List(c) => c case _ => null }
      val refsPk =
        //ref table (set fk and pk)
        (if (tbl.name == ctx.table.name && ctx.refToParent != null) if (ctx.refToParent == pk)
          Set(pk -> idRefId(parent, tbl.name)) else Set(ctx.refToParent -> s":#$parent") ++
            (if (pk == null || refs.contains(pk)) Set() else Set(pk -> s"#${tbl.name}"))
        //not ref table (set pk)
        else Option(tbl.key.cols)
          .filter(k=> k.size == 1 && !refs.contains(k.head)).map(_.head -> s"#${tbl.name}").toSet) ++
        //set refs
        (if(tbl.name == headTable.table) Set() else refs
            //filter out pk of the linked table in case it matches refToParent
            .filterNot(tbl.name == ctx.table.name && _ == ctx.refToParent)
            .map(_ -> idRefId(headTable.table, tbl.name)))
      (refsPk,
        key.map(k => refsPk.collectFirst { case (n, v) if n == k && k != pk => RefKeyCol(n) -> v }
          .getOrElse(KeyCol(k) ->
            ctx.view.properties
              .collectFirst {
                case OrtMetadata.Property(p, TresqlValue(v, _, _)) if p == k => v
                case OrtMetadata.Property(p, KeyValue(whereTresql, _)) if p == k => whereTresql
              }
              .getOrElse(s":$k")))
      )
    }

    md.tableOption(headTable.table).flatMap { tableDef =>
      val linkedTresqls =
        for {
          linkedTable <- ctx.view.saveTo.tail
          tableDef <- md.tableOption(linkedTable.table)
        } yield
          tresql_string(tableDef, ctx.view.alias,
            refsPkAndKey(tableDef, linkedTable.refs, linkedTable.key),
            Nil, true) //no children, set upsert flag for linked table

      Option(tresql_string(tableDef, ctx.view.alias, refsPkAndKey(tableDef, Set(), headTable.key),
        linkedTresqls.filter(_ != null), false))
    }.orNull
  }
}

object OrtMetadata {
  sealed trait OrtValue

  /** Column value
   * @param tresql        tresql statement
   * @param forInsert     Column is to be included into insert statement if true
   * @param forUpdate     Column is to be included into update statement if true
   * */
  case class TresqlValue(tresql: String, forInsert: Boolean, forUpdate: Boolean) extends OrtValue

  /** Column value
   * @param view          child view
   * */
  case class ViewValue(view: View) extends OrtValue

  /** Column value
   * @param view          child view
   * */
  case class LookupViewValue(view: View) extends OrtValue

  /** Column value
   * @param whereTresql   key find tresql
   * @param valueTresql   key value tresql
   * */
  case class KeyValue(whereTresql: String, valueTresql: String) extends OrtValue

  /** Saveable column.
   * @param col           Goes to dml column clause or child tresql alias
   * @param value         Goes to dml values clause or column clause as child tresql
   * */
  case class Property(col: String, value: OrtValue)

  /** Saveable view.
   * @param saveTo        destination tables. If first table has {{{refs}}} parameter
   *                      it indicates reference field to parent.
   * @param options       saveable options [+-=]. Relevant for child views
   * @param filters       horizontal authentication filters
   * @param alias         table alias in DML statement
   * @param properties    saveable fields
   * @param db            database name (can be null)
   * */
  case class View(saveTo: List[SaveTo],
                  options: SaveOptions,
                  filters: Option[Filters],
                  alias: String,
                  properties: List[Property],
                  db: String)

  /** Table to save data to.
   * @param table         table name
   * @param refs          imported keys of table from parent or linked tables to be set
   * @param key           column names uniquely identifying table record
   * */
  case class SaveTo(table: String, refs: Set[String], key: Seq[String])

  /** Save options
   *  @param doInsert     insert new children data
   *  @param doUpdate     update existing children data
   *  @param doDelete     delete absent children data
   * */
  case class SaveOptions(doInsert: Boolean, doUpdate: Boolean, doDelete: Boolean)

  /** Horizontal authentication filters
   *  @param insert       insert statement where clause
   *  @param delete       delete statement where clause
   *  @param update       update statement where clause
   * */
  case class Filters(insert: Option[String] = None, delete: Option[String] = None, update: Option[String] = None)

  /**
   * Patterns are for debugging purposes to create {{{View}}} from {{{Map}}} using {{{ortMetadata}}} method.
   *
   * Children property format:
   *  [@db:]table[:ref]*[[key[,key]*]][#table[:ref]*[[key[,key]*]]]*[[options]] [alias][|insertFilterTresql, deleteFilterTresql, updateFilterTresql]
   *  Options are to be specified in such order: insert, delete, update, i.e. '+-='
   *  Examples:
   *    dept#car:deptnr:nr#tyres:carnr:nr
   *    dept[dname]#car:deptnr:nr#tyres:carnr:nr
   *    dept[+=] alias
   *    emp[ename][+-=] e|:deptno in currentUserDept(:current_user), null /* no statement */, e.deptno in currentUserDept(:current_user)
   */
  case class PropPatterns(prop: Regex,
                          tables: Regex,
                          resolverProp: Regex,
                          resolverExp: Regex)

  val Patterns = {
    val ident = """[^:\[\]\s#@]+"""
    val key = """\[\s*\w+(?:\s*,\s*\w+\s*)*\s*\]"""
    val db = s"(?:@($ident):)?"
    val table = s"$ident(?::$ident)*(?:$key)?"
    val tables = s"""($table(?:#$table)*)"""
    val options = """(?:\[(\+?-?=?)\])?"""
    val alias = """(?:\s+(\w+))?"""
    val filters = """(?:\|(.+))?"""

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

    PropPatterns((db + tables + options + alias + filters)r, tables.r, ResolverPropPattern, ResolverExpPattern)
  }
}

object ORT extends ORT
