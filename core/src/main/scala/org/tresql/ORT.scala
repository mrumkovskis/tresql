package org.tresql

import org.tresql.OrtMetadata.{Filters, KeyValue, LookupViewValue, Property, SaveOptions, SaveTo, TresqlValue, View, ViewValue}
import org.tresql.ast.Exp

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
  sealed trait IdOrRefVal { def name: String; def value: String }
  /** This is used in insert expression */
  case class IdVal(name: String, value: String) extends IdOrRefVal
  /** This is use in update and missing children delete expression */
  case class IdRefVal(name: String, value: String) extends IdOrRefVal
  /** This is used for save to multiple linked tables or if reference to parent matches primary key */
  case class IdRefIdVal(name: String, value: String) extends IdOrRefVal
  /** This is used to set child reference to parent */
  case class IdParentVal(name: String, value: String) extends IdOrRefVal
  sealed trait KeyPart { def name: String }
  case class KeyCol(name: String) extends KeyPart
  case class RefKeyCol(name: String) extends KeyPart
  case class SaveContext(name: String,
                         view: View,
                         parents: List[ParentRef],
                         saveOptions: SaveOptions,
                         table: metadata.Table,
                         saveTo: SaveTo,
                         refToParent: String)
  case class ColVal(col: String, value: String, forInsert: Boolean, forUpdate: Boolean, optional: Boolean)
  case class SaveData(table: String,
                      pk: List[String],
                      alias: String,
                      colsVals: List[ColVal],
                      refsPkVals: Set[IdOrRefVal],
                      keyVals: Seq[(KeyPart, String)],
                      filters: Option[Filters],
                      upsert: Boolean,
                      db: String)


  /** Expression is built only from macros to ensure ORT lookup editing.
   * Expression executes {{{upsertExpr}}} on {{{objProp}}} environment variable
   * which is expected to be {{{Map[String, Any]}}} and tries to return id of inserted or updated object.
   * */
  case class LookupUpsertExpr(objProp: String,
                              idProp: String,
                              upsertExpr: Expr,
                              idSelExpr: Expr) extends BaseExpr {
    override def apply() = {
      env(objProp) match {
        case m: Map[String @unchecked, Any @unchecked] =>
          import CoreTypes._
          def id(res: Any) = res match {
            case r: Result[_] => r.uniqueOption[Any].orNull
            case x => x
          }
          def res(r: Any): Any = r match {
            case i: InsertResult => i.id.orNull
            case u: UpdateResult =>
              if (u.count.getOrElse(0) > 0) m.getOrElse(idProp, id(idSelExpr(m))) else null
            case a: ArrayResult[_] if a.values.nonEmpty => res(a.values.last)
            case x => error(s"Unexpected $this result: $x")
          }
          res(upsertExpr(m))
        case null => null
        case x => x
      }
    }
    def defaultSQL = s"LookupUpsertExpr($objProp, $idProp, $upsertExpr, $idSelExpr)"
  }
  /**
   * Try to execute {{{updateExpr}}} and if no rows affected execute {{{insertExpr}}}
   * Expression is built from macros to ensure ORT one to one relationship setting
   * */
  case class UpsertExpr(updateExpr: Expr, insertExpr: Expr) extends BaseExpr {
    override def apply() = {
      def resOrInsert(res: Any): Any = res match {
        case r: DMLResult if r.count.getOrElse(0) > 0 || r.children.nonEmpty => r // return result if update affects some row
        case a: ArrayResult[_] if a.values.nonEmpty => resOrInsert(a.values.last)
        case _ => insertExpr() // execute insert if update affects no rows
      }
      resOrInsert(updateExpr())
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
        case List(_: BaseVarExpr) =>
          delExpr(Map("keys" -> {
            env(obj) match {
              case s: Seq[Map[String, _] @unchecked] =>
                s.map(_.getOrElse(key.head, null)).filter(_ != null)
              case m: Map[String @unchecked, _] =>
                List(m.getOrElse(key.head, null))
              case null => Nil
            }
          }))
        case _ =>
          delExpr(Map("keys" -> {
            env(obj) match {
              case s: Seq[Map[String, _] @unchecked] => s
              case m: Map[String @unchecked, _] =>
                List(m.getOrElse(key.head, null))
              case null => Nil
            }
          }))
      }
    }
    override def defaultSQL = s"DeleteChildrenExpr($obj, $key, $delExpr)"
  }
  case class NotDeleteKeysExpr(key: List[Expr], key_val_exprs: List[Expr]) extends BaseExpr {
    override def defaultSQL = env.get("keys").map {
      case keyVals: Seq[_] if keyVals.isEmpty => ConstExpr(true).sql
      case keyVals: Seq[_] => key_val_exprs match {
        case List(_: BaseVarExpr) =>
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
    }.getOrElse(ConstExpr(true).sql)
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

  case class DeferredBuildExpr(exp: Exp) extends BaseExpr {
    override def apply(params: Map[String, Any]): Any = {
      val b =
        newInstance(
          new Env(ORT.this, env.db, false),
          queryDepth + 1, bindIdx, childrenCount
        )
      if (params != null) b.env.update(params)
      val expr = b.buildExpr(exp)
      if (expr != null) expr() else null // build fun may return null expression due to optional binding
    }

    override def apply(): Any = apply(null: Map[String, Any])

    override def defaultSQL: String = s"DeferredBuildExpr($exp)"
  }

  def insert(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources): InsertResult = {
    val name_with_filter = if (filter == null) name else s"$name|$filter, null, null"
    unwrapDMLResult(
      save(name_with_filter, obj, insert_tresql, "Cannot insert data. Table not found for object: " + name)
    )
  }

  def update(name: String, obj: Map[String, Any], filter: String = null)
    (implicit resources: Resources): UpdateResult = {
    val name_with_filter = if (filter == null) name else s"$name|null, null, $filter"
    unwrapDMLResult(
      save(name_with_filter, obj, update_tresql,
      s"Cannot update data. Table not found or no primary key or no updateable columns found for the object: $name")
    )
  }

  def delete(name: String, id: Any, filter: String = null, filterParams: Map[String, Any] = null)
            (implicit resources: Resources): DeleteResult = {
    val OrtMetadata.Patterns.prop(db, tableName, _, alias, _) = name
    val md = tresqlMetadata(db)
    val delete =
      (for {
        table <- md.tableOption(tableName)
        pk <- table.key.cols.headOption
        if table.key.cols.size == 1
      } yield {
        s"-${tableWithDb(db, table.name, alias)}[$pk = ?${Option(filter)
          .map(f => s" & ($f)").getOrElse("")}]"
      }) getOrElse {
        error(s"Table $name not found or table primary key not found or table primary key consists of more than one column")
      }
    unwrapDMLResult(build(delete, Map("1" -> id) ++ Option(filterParams).getOrElse(Map()),
      reusableExpr = false)(resources)())
  }

  /** insert methods to multiple tables
   * Tables must be ordered in parent -> child direction. */
  def insertMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
                    (implicit resources: Resources): InsertResult =
    unwrapDMLResult(insert(names.mkString("#"), obj, filter))

  /** update to multiple tables
   *  Tables must be ordered in parent -> child direction. */
  def updateMultiple(obj: Map[String, Any], names: String*)(filter: String = null)
                    (implicit resources: Resources): UpdateResult =
    unwrapDMLResult(update(names.mkString("#"), obj, filter))

  //object methods
  def insertObj[T](obj: T, filter: String = null)(
    implicit resources: Resources, conv: ObjToMapConverter[T]): InsertResult = {
    val v = conv(obj)
    insert(v._1, v._2, filter)
  }

  def updateObj[T](obj: T, filter: String = null)(
    implicit resources: Resources, conv: ObjToMapConverter[T]): UpdateResult = {
    val v = conv(obj)
    update(v._1, v._2, filter)
  }

  def insert(metadata: View, obj: Map[String, Any])(implicit resources: Resources): InsertResult = {
    resources.log(s"$metadata", Nil, LogTopic.ort)
    val tresql = insertTresql(metadata)
    unwrapDMLResult(build(tresql, obj, reusableExpr = false)(resources)())
  }

  def update(metadata: View, obj: Map[String, Any])(implicit resources: Resources): UpdateResult = {
    resources.log(s"$metadata", Nil, LogTopic.ort)
    val tresql = updateTresql(metadata)
    unwrapDMLResult(build(tresql, obj, reusableExpr = false)(resources)())
  }

  def save(metadata: View, obj: Map[String, Any])(implicit resources: Resources): DMLResult = {
    resources.log(s"$metadata", Nil, LogTopic.ort)
    val tresql = s"_upsert(${updateTresql(metadata)}, ${insertTresql(metadata)})"
    unwrapDMLResult(build(tresql, obj, reusableExpr = false)(resources)())
  }

  def delete(name: String, key: Seq[String],
             params: Map[String, Any],
             filter: String)(implicit
                             resources: Resources): DeleteResult = {
    val tresql = deleteTresql(name, key, filter)
    unwrapDMLResult(build(tresql, params, reusableExpr = false)(resources)())
  }

  def insertTresql(metadata: View)(implicit resources: Resources): String = {
    saveTresql(metadata, SaveOptions(doInsert = true, doUpdate = false, doDelete = true), insert_tresql)
  }

  def updateTresql(metadata: View)(implicit resources: Resources): String = {
    saveTresql(metadata, SaveOptions(doInsert = true, doUpdate = false, doDelete = true), update_tresql)
  }

  def deleteTresql(name: String, key: Seq[String], filter: String)(implicit resources: Resources): String = {
    val OrtMetadata.Patterns.prop(db, tableName, _, alias, _) = name
    s"-${tableWithDb(db, tableName, alias)}[${
      key.map(c => c + " = :" + c).mkString(" & ")}${if (filter == null) "" else s" & ($filter)"}]"
  }

  private def save(name: String,
                   obj: Map[String, Any],
                   save_tresql_fun: SaveContext => String,
                   errorMsg: String)(implicit resources: Resources): Any = {
    val (view, saveOptions) = OrtMetadata.ortMetadata(name, obj)
    resources.log(s"$view", Nil, LogTopic.ort)
    val tresql = saveTresql(view, saveOptions, save_tresql_fun)
    if(tresql == null) error(errorMsg)
    build(tresql, obj, reusableExpr = false)(resources)()
  }

  private def tresqlMetadata(db: String)(implicit resources: Resources) =
    if (db == null) resources.metadata else resources.extraResources(db).metadata

  private def unwrapDMLResult[T <: DMLResult](res: Any): T = res match {
    case _: InsertResult | _: UpdateResult | _: DeleteResult | null => res.asInstanceOf[T]
    case a: ArrayResult[_] if a.values.nonEmpty => unwrapDMLResult(a.values.last)
    case x => sys.error(s"Not dml result: $x")
  }

  private def saveTresql(view: View,
                         saveOptions: SaveOptions,
                         saveFunc: SaveContext => String)(implicit
                                                          resources: Resources): String = {
    save_tresql(null, view, Nil, saveOptions, saveFunc)
  }

  private def save_tresql(name: String,
                          view: View,
                          parents: List[ParentRef],
                          saveOptions: SaveOptions,
                          save_tresql_func: SaveContext => String)(implicit
                                                                   resources: Resources): String = {
    val parent = parents.headOption.map(_.table).orNull
    val md = tresqlMetadata(view.db)
    //find first imported key to parent in list of table links passed as a param.
    def importedKeyOption(tables: Seq[SaveTo]): Option[(metadata.Table, SaveTo, String)] = {
      def processLinkedTables(linkedTables: Seq[SaveTo]): Option[(metadata.Table, SaveTo, String)] = {
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
      val saveTo = tables.head
      md.tableOption(saveTo.table) //no parent no ref to parent
        .filter(_ => parent == null)
        .map((_, saveTo, null))
        .orElse {
          if (saveTo.refs.nonEmpty) //ref to parent in first table found must be resolved!
            md.tableOption(saveTo.table).flatMap(table =>
              saveTo.refs.
                find(r => table.refs(parent).filter(_.cols.size == 1).exists(_.cols.head == r))
                .map((table, saveTo, _)))
          else processLinkedTables(tables) //search for ref to parent
        }
    }

    (for {
      (table, saveTo, ref) <- importedKeyOption(view.saveTo)
    } yield
        save_tresql_func(SaveContext(name, view, parents, saveOptions, table, saveTo, ref))
    ).orNull
  }

  private def tableWithDb(db: String, table: String, alias: String) = {
    (if (db == null) "" else db + ":") + table + (if (alias == null) "" else " " + alias)
  }

  private def hasOptionalFields(view: View) =
    view.properties.exists {
      case Property(_, t: TresqlValue) => t.optional
      case Property(_, l: LookupViewValue) => l.view.optional
      case _ => false
    }

  private def table_insert_tresql(saveData: SaveData) = {
    import saveData._
    val refsAndPk = refsPkVals.collect {case x if !x.isInstanceOf[IdRefVal] => (x.name, x.value)}
    colsVals.filter(_.forInsert).map(cv => cv.col -> cv.value) ++ refsAndPk match {
      case cols_vals =>
        val (cols, vals) = cols_vals.unzip
        cols.mkString(s"+${tableWithDb(db, table, null)} {", ", ", "}") +
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
    if (ctx.view.forInsert) {
      val tresql = save_tresql_internal(ctx, table_insert_tresql, save_tresql(_, _, _, _, insert_tresql))
      if (ctx.name != null && (ctx.view.optional || hasOptionalFields(ctx.view)))
        s"_deferred_build(if_defined(:${ctx.name}?, $tresql))"
      else tresql
    } else null
  }

  private def update_tresql(ctx: SaveContext)(implicit resources: Resources): String = {
    if (!ctx.view.forUpdate) return null

    def filterString(filters: Option[Filters], extraction: Filters => Option[String]): String =
      filters.flatMap(extraction).map(f => s" & ($f)").getOrElse("")

    import ctx._
    val parent = parents.headOption.map(_.table).orNull
    val tableName = table.name

    def delAllChildren =
      s"-${tableWithDb(view.db, tableName, view.alias)}[$refToParent = :#$parent${filterString(ctx.view.filters, _.delete)}]"
    def delMissingChildren = {
      def del_children_tresql(data: SaveData) = {
        def refsAndKey(rk: Set[IdOrRefVal]) = {
          val rkf = rk.collect {case x if !x.isInstanceOf[IdVal] => (x.name, x.value)}
          val pk = data.pk.headOption.orNull
          rkf.partition(_._1 != pk) match {
            case (refs: Set[(String, String)@unchecked], pk: Set[(String, String)@unchecked]) =>
              (refs.toList, pk.toList)
          }
        }
        val (refCols, keyCols) =
          if (data.keyVals.nonEmpty) data.keyVals.partition(_._1.isInstanceOf[RefKeyCol]) match {
            case (refCols: List[(RefKeyCol, String)@unchecked], keyCols: List[(KeyCol, String)@unchecked]) =>
              val (rc, kc) =
                (refCols.map { case (k, v) => (k.name, v) }, keyCols.map { case (k, v) => (k.name, v) })
              if (rc.nonEmpty) (rc, kc) else (refsAndKey(data.refsPkVals)._1, kc)
          } else refsAndKey(data.refsPkVals)

          val refColsFilter = refCols.map { case (n, v) => s"$n = $v"}.mkString(" & ")
          val (key_arr, key_val_expr_arr) = keyCols.unzip match {
            case (kc, kv) => (s"[${kc.mkString(", ")}]", s"[${kv.mkString(", ")}]")
          }
          val filter = filterString(data.filters, _.delete)
          if (refCols.isEmpty || keyCols.isEmpty) null
          else
            s"""_delete_missing_children('$name', $key_arr, $key_val_expr_arr, -${tableWithDb(data.db,
              data.table, data.alias)}[$refColsFilter & _not_delete_keys($key_arr, $key_val_expr_arr)$filter])"""
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
              case cv @ ColVal(c, v, _, true, _)
                if !keyVals.exists { case (k, kv) => k.name == c && v == kv } => cv
            }
          else colsVals.filter(_.forUpdate))
            .map(cv => cv.col -> cv.value)
        val refsAndPk = refsPkVals.collect {case x if !x.isInstanceOf[IdVal] => (x.name, x.value)}
        val (upd_tresql, hasChildren) = Option(filteredColsVals.unzip match {
          case (cols: List[String], vals: List[String]) if cols.nonEmpty =>
            val filter = filterString(data.filters, _.update)
            val filteredVals = vals.filter(_ != null) // filter out child queries vals
            val hasChildren = cols.size > filteredVals.size
            def updFilter(f: Iterable[(String, String)]) = f.map(fc => s"${fc._1} = ${fc._2}")
            val updateFilter =
              updFilter(if (keyVals.nonEmpty && !hasChildren) keyVals.map(kp => (kp._1.name, kp._2)) else refsAndPk)
                .mkString("[", " & ", s"$filter]")
            (cols.mkString(s"=${tableWithDb(db, table, alias)} $updateFilter {", ", ", "}") +
              filteredVals.mkString("[", ", ", "]"), hasChildren)
          case _ => // no updatable columns, update primary key with self to return create empty update in the case this is used in upsert expression
            val pkVals =
              if (keyVals.nonEmpty) keyVals.map { case (kp, v) => (kp.name, v)}
              else refsAndPk.filter(x => data.pk.contains(x._1))
            val u =
              if (pkVals.isEmpty) null
              else {
                val pkStr = data.pk.mkString(", ")
                val filter = filterString(data.filters, _.update)
                s"=${tableWithDb(db, table, alias)} [${
                  pkVals.map {case (c, v) => s"$c = $v"} mkString(" & ")}$filter] {$pkStr} [$pkStr]"
              }
            (u, false)
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
            tableWithDb(db, table, null)}[${keyVals.map(kv => s"${kv._1.name} = ${kv._2}")
            .mkString(" & ")}]{$pr_col}), $upd_tresql)"
        } else upd_tresql
      }
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, _, update_tresql))
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
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, _, insert_tresql))
    }
    def updOrIns = s"|_upsert($upd, $ins)"

    def mayBe(tresql: String) =
      if (ctx.name != null && (view.optional || hasOptionalFields(view)))
        s"_deferred_build(if_defined(:${ctx.name}?, $tresql))"
      else tresql

    import ctx.saveOptions._
    val pk = table.key.cols
    if (parent == null) if (saveTo.key.isEmpty && pk.isEmpty) null else mayBe(upd)
    else if (pk.size == 1 && refToParent == pk.head) mayBe(upd) else
      if (saveTo.key.isEmpty && pk.isEmpty) {
        (Option(doDelete).filter(_ == true).map(_ => mayBe(delAllChildren)) ++
          Option(doInsert).filter(_ == true)
            .flatMap(_ => Option(mayBe(ins)))).mkString(", ")
      } else {
        (Option(doDelete).filter(_ == true).map(_ =>
          mayBe(if(!doUpdate) delAllChildren else delMissingChildren)) ++
          ((doInsert, doUpdate) match {
            case (true, true) => Option(mayBe(updOrIns))
            case (true, false) => Option(mayBe(ins))
            case (false, true) => Option(mayBe(upd))
            case (false, false) => None
          })).mkString(", ")
      }
  }

  private def save_tresql_internal(
    ctx: SaveContext,
    table_save_tresql: SaveData => String,
    children_save_tresql: (
      String, //children property name
      View,
      List[ParentRef], //parent chain
      SaveOptions
    ) => String)
    (implicit resources: Resources) = {

    def tresql_string(
      table: metadata.Table,
      alias: String,
      refsPkAndKey: (Set[IdOrRefVal], Seq[(KeyPart, String)]),
      children: Seq[String],
      upsert: Boolean
    ) = {
      val (refsAndPk, key) = refsPkAndKey
      ctx.view.properties.flatMap {
        case OrtMetadata.Property(col, _) if refsAndPk.exists(_.name == col) => Nil
        case OrtMetadata.Property(col, KeyValue(_, TresqlValue(valueTresql, forInsert, forUpdate, optional))) =>
          List(ColVal(table.colOption(col).map(_.name).orNull, valueTresql, forInsert, forUpdate, optional))
        case OrtMetadata.Property(col, TresqlValue(tresql, forInsert, forUpdate, optional)) =>
          val (t, c) = col.split("\\.") match {
            case Array(a, b) => (a, b)
            case Array(a) => (table.name, a)
            case a => (a.dropRight(1).mkString("."), a.last)
          }
          if (t == table.name)
            List(ColVal(table.colOption(c).map(_.name).orNull, tresql, forInsert, forUpdate, optional))
          else Nil
        case OrtMetadata.Property(prop, ViewValue(v, so)) =>
          if (children_save_tresql != null) {
            val chtresql = children_save_tresql(prop, v, ParentRef(table.name, ctx.refToParent) :: ctx.parents, so)
            val chtresql_alias = Option(prop).map(p => s" '$p'").mkString
            List(ColVal(Option(chtresql).map(_ + chtresql_alias).orNull, null, true, true, v.optional))
          } else Nil
        case OrtMetadata.Property(refColName, LookupViewValue(propName, lookupView)) =>
          (for {
            // check whether refColName exists in table, only then generate lookup tresql
            _ <- table.colOption(refColName)
            lookupTableName <- lookupView.saveTo.headOption.map(_.table)
            lookupTable <- tresqlMetadata(lookupView.db).tableOption(lookupTableName)
          } yield {
            def pkCol(t: metadata.Table) = t.key.cols match { case List(c) => c case _ => null }
            def saveTo(v: View, tn: String) = v.saveTo.find(_.table == tn)
            def idSelExpr(v: View, t: metadata.Table) = {
              def key_val(v: View, tn: String) = {
                val key = saveTo(v, tn).map(_.key.toSet).getOrElse(Set())
                v.properties.flatMap {
                  case OrtMetadata.Property(col, TresqlValue(v, _, _, _)) if key.contains(col) =>
                    List((col, v))
                  case _ => Nil
                }
              }
              val pk_col = pkCol(t)
              if (pk_col != null) {
                val where = s"${key_val(v, t.name).map { case (k, v) => s"$k = $v"}.mkString(" & ") }"
                if (where.nonEmpty) s"(${t.name}[$where]{$pk_col})"
                else "null"
              } else "null"
            }
            def idProp(v: View, t: metadata.Table) = {
              Option(pkCol(t)).flatMap { pk =>
                v.properties.collectFirst {
                  case OrtMetadata.Property(`pk`, TresqlValue(v, _, _, _)) =>
                    if (v.startsWith(":")) v.substring(1) else pk
                }
              }.orNull
            }
            val idPropName = idProp(lookupView, lookupTable)
            val update = save_tresql(null, lookupView, Nil, SaveOptions(true, false, true), update_tresql)
            val insert = save_tresql(null, lookupView, Nil, SaveOptions(true, false, true), insert_tresql)
            val lookupUpsert = {
              val tr = s"|_upsert($update, $insert)"
              if (hasOptionalFields(lookupView)) s"_deferred_build($tr)"
              else tr
            }
            val lookupIdSel = idSelExpr(lookupView, lookupTable)
            val (lookupTresql, refColTresql) = {
              val tr = s":$refColName = |_lookup_upsert('$propName', ${
                if (idPropName == null) "null" else s"'$idPropName'"}, $lookupUpsert, $lookupIdSel)"
              if (lookupView.optional)
                (s"_deferred_build(if_defined(:$propName?, $tr))", s"if_defined(:$propName?, :$refColName)")
              else
                (tr, s":$refColName")
            }
            List(lookupTresql, ColVal(refColName, refColTresql, true, true, false))
          }).getOrElse(Nil)
      }.partition(_.isInstanceOf[String]) match {
        case (lookups: List[String@unchecked], colsVals: List[ColVal@unchecked]) =>
          val tableName = table.name
          //lookup edit tresql
          val lookupTresql = Option(lookups).filter(_.nonEmpty).map(_.map(_ + ", ").mkString)
          //base table tresql
          val tresql = colsVals.filter(_.col != null /*check if prop->col mapping found*/) ++
              children.map(c => ColVal(c, null, true, true, false)) /*add same level one to one children*/
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
                     key: Seq[String]): (Set[IdOrRefVal], Seq[(KeyPart, String)]) = {
      def idRefId(idRef: String, id: String) = s"_id_ref_id($idRef, $id)"
      def getPk(t: metadata.Table) = t.key.cols match { case List(c) => c case _ => null }
      def idExp(t: String, pk: String): Set[IdOrRefVal] = {
        ctx.view.properties.collectFirst {
          case OrtMetadata.Property(`pk`, TresqlValue(tresql, _, _, _))
            if tresql.startsWith(":") && tresql.substring(1) != pk =>
            Set[IdOrRefVal](IdVal(pk, s"#$t$tresql"), IdRefVal(pk, s":#$t$tresql"))
        }.getOrElse(Set[IdOrRefVal](IdVal(pk, s"#$t"), IdRefVal(pk, s":#$t:$pk")))
      }
      val pk = getPk(ctx.table)
      val refsPk: Set[IdOrRefVal] =
        //ref table (set fk and pk)
        (if (tbl.name == ctx.table.name && ctx.refToParent != null)
          if (ctx.refToParent == pk)
            Set(IdRefIdVal(pk, idRefId(parent, tbl.name)))
          else
            Set(IdParentVal(ctx.refToParent, s":#$parent")) ++
              (if (pk == null || refs.contains(pk)) Set() else idExp(tbl.name, pk))
        //not ref table (set pk)
        else
          Option(tbl.key.cols)
            .collectFirst{ case k if k.size == 1 && !refs.contains(k.head) => k.head }
            .map(idExp(tbl.name, _)).getOrElse(Set())
        ) ++ //set refs
        ( if(tbl.name == headTable.table)
            Set()
          else //filter out pk of the linked table in case it matches refToParent
            refs
              .filterNot(tbl.name == ctx.table.name && _ == ctx.refToParent)
              .map(IdRefIdVal(_, idRefId(headTable.table, tbl.name)))
        )
      (refsPk,
        key.map(k =>
          refsPk.collectFirst { case x if x.name == k && k != pk =>
            RefKeyCol(x.name) -> x.value
          }
          .getOrElse(
            KeyCol(k) -> ctx.view.properties
              .collectFirst {
                case OrtMetadata.Property(`k`, TresqlValue(v, _, _, _)) => v
                case OrtMetadata.Property(`k`, KeyValue(whereTresql, _)) => whereTresql
              }
              .getOrElse(s":$k")
          )
        )
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
   * @param optional      Indicates whether value may be absent
   * */
  case class TresqlValue(tresql: String, forInsert: Boolean, forUpdate: Boolean, optional: Boolean) extends OrtValue

  /** Column value
   * @param view          child view
   * @param saveOptions   saveable options [+-=]
   * */
  case class ViewValue(view: View, saveOptions: SaveOptions) extends OrtValue

  /** Column value
   * @param propName      property name (from environment)
   * @param view          child view
   * */
  case class LookupViewValue(propName: String, view: View) extends OrtValue

  /** Column value
   * @param whereTresql   key find tresql
   * @param valueTresql   key value tresql
   * */
  case class KeyValue(whereTresql: String, valueTresql: TresqlValue) extends OrtValue

  /** Saveable column.
   * @param col           Goes to dml column clause or child tresql alias
   * @param value         Goes to dml values clause or column clause as child tresql
   * */
  case class Property(col: String, value: OrtValue)

  /** Saveable view.
   * @param saveTo        destination tables. If first table has {{{refs}}} parameter
   *                      it indicates reference field to parent.
   * @param filters       horizontal authentication filters
   * @param alias         table alias in DML statement
   * @param forInsert     view is only designated for insert statement
   * @param forUpdate     view is only designated for update statement (according to save options in case of child view)
   * @param optional      Indicates whether value may be absent
   * @param properties    saveable fields
   * @param db            database name (can be null)
   * */
  case class View(saveTo: Seq[SaveTo],
                  filters: Option[Filters],
                  alias: String,
                  forInsert: Boolean,
                  forUpdate: Boolean,
                  optional: Boolean,
                  properties: Seq[Property],
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
  case class Filters(insert: Option[String] = None, update: Option[String] = None, delete: Option[String] = None)

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

  /** This method is for debugging purposes. */
  def ortMetadata(tables: String, saveableMap: Map[String, Any])(implicit resources: Resources): (View, SaveOptions) = {
    def tresqlMetadata(db: String)(implicit resources: Resources) =
      if (db == null) resources.metadata else resources.extraResources(db).metadata
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
      import ast.{Arr, Null}
      val filters =
        Option(filterStr).flatMap(new QueryParser(resources, resources.cache).parseExp(_) match {
          case Arr(List(insert, delete, update)) => Some(Filters(
            insert = Some(insert).filter(_ != Null).map(_.tresql),
            update = Some(update).filter(_ != Null).map(_.tresql),
            delete = Some(delete).filter(_ != Null).map(_.tresql),
          ))
          case _ => error(s"""Unrecognized filter declaration '$filterStr'.
                             |Must consist of 3 comma separated tresql expressions: insertFilter, deleteFilter, updateFilter.
                             |In the case expression is not needed it must be set to 'null'.""".stripMargin)
        })
      (View(saveTo(tables, tresqlMetadata(db)), filters, alias, true, true, false, Nil, db), SaveOptions(i, u, d))
    }
    def resolver_tresql(property: String, resolverExp: String) = {
      import ast._
      val OrtMetadata.Patterns.resolverProp(prop) = property
      val OrtMetadata.Patterns.resolverExp(col, exp) = resolverExp
      val parser = new QueryParser(resources, resources.cache)
      OrtMetadata.Property(col, TresqlValue(
        parser.transformer {
          case Ident(List("_")) => Variable(prop, Nil, opt = false)
        } (parser.parseExp(if (exp startsWith "(" ) exp else s"($exp)")).tresql, true, true, false
      ))
    }
    val struct = tresql_structure(saveableMap)
    val (view, saveOptions) = parseTables(tables)
    val props =
      struct.map {
        case (prop, value) => value match {
          case m: Map[String, Any]@unchecked =>
            val md = tresqlMetadata(view.db)
            view.saveTo.collectFirst {
              case st if md.tableOption(st.table).exists(_.refTable.contains(List(prop))) => //lookup table found
                val lookupTable = md.table(st.table).refTable(List(prop))
                OrtMetadata.Property(prop, LookupViewValue(prop, ortMetadata(lookupTable, m)._1))
            }.getOrElse {
              val (v, so) = ortMetadata(prop, m)
              OrtMetadata.Property(prop, ViewValue(v, so))
            }
          case v: String if prop.indexOf("->") != -1 => resolver_tresql(prop, v)
          case _ => OrtMetadata.Property(prop, TresqlValue(s":$prop", true, true, false))
        }
      }.toList
    (view.copy(properties = props), saveOptions)
  }
}

object ORT extends ORT
