package org.tresql

import org.tresql.OrtMetadata.{AutoValue, Filters, KeyValue, LookupViewValue, OrtSimpleValue, Property, SaveOptions, SaveTo, TresqlValue, View, ViewValue}
import org.tresql.ast.Exp

import scala.util.matching.Regex
import sys._

/** Object Relational Transformations - ORT */
trait ORT extends Query {

  type ObjToMapConverter[T] = T => (String, Map[String, _])

  /** QueryBuilder methods **/
  override private[tresql] def newInstance(e: Env, idx: Int, chIdx: Int) = {
    val qpos = chIdx :: queryPos
    new ORT {
      override def env = e
      override private[tresql] def queryPos = qpos
      override private[tresql] var bindIdx = idx
    }
  }

  case class ParentRef(table: String, ref: String)
  sealed trait IdOrRefVal { def name: String; def value: String }
  /** This is used in insert expression */
  case class IdVal(name: String, value: String) extends IdOrRefVal
  /** This is use in update and missing children delete expression */
  case class IdRefVal(name: String, value: String) extends IdOrRefVal
  /** This is used to set child reference to parent or linked reference */
  case class IdParentVal(name: String, value: String) extends IdOrRefVal
  sealed trait KeyPart { def name: String }
  case class KeyCol(name: String) extends KeyPart
  case class RefKeyCol(name: String) extends KeyPart
  case class SaveContext(name: String,
                         view: View,
                         parents: List[ParentRef],
                         saveOptions: SaveOptions,
                         optional: Boolean,
                         forInsert: Boolean,
                         forUpdate: Boolean,
                         table: metadata.Table,
                         saveTo: SaveTo,
                         refToParent: String)
  case class ColVal(col: String, value: String,
                    forInsert: Boolean, forUpdate: Boolean,
                    updateValue: Option[String] = None)
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
            case r: Result[_] => r.uniqueOption[Any](CoreTypes.convAny).orNull
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
          InExpr(key.head, List(VarExpr("keys", Nil, opt = false, allowArrBind = true)), not = true).sql
        case _ =>
          def comp(key_part_and_expr: (Expr, Expr), i: Int) =
            BinExpr("=", key_part_and_expr._1, transform(key_part_and_expr._2, {
              case VarExpr(n, Nil, opt, allowArrBind) => VarExpr("keys", List(i.toString, n), opt, allowArrBind)
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
        case r: Result[_] => r.headOption[Any](CoreTypes.convAny).orNull
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
          bindIdx, childrenCount
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
    save_tresql(null, view, Nil, saveOptions, false, true, true, saveFunc)
  }

  private def save_tresql(name: String,
                          view: View,
                          parents: List[ParentRef],
                          saveOptions: SaveOptions,
                          optional: Boolean,
                          forInsert: Boolean,
                          forUpdate: Boolean,
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
        save_tresql_func(
          SaveContext(name, view, parents, saveOptions, optional, forInsert, forUpdate, table, saveTo, ref))
    ).orNull
  }

  private def tableWithDb(db: String, table: String, alias: String) = {
    (if (db == null) "" else db + ":") + table + (if (alias == null) "" else " " + alias)
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

  private def insert_tresql(ctx: SaveContext)(implicit resources: Resources): String = if (ctx.forInsert) {
    mayBeDeferredTresql(
      ctx,
      save_tresql_internal(ctx, table_insert_tresql, save_tresql(_, _, _, _, _, _, _, insert_tresql))
    )
  } else null

  private def mayBeDeferredTresql(ctx: SaveContext, tresql: String): String =
    if (ctx.name != null && ctx.optional)
      s"_deferred_build(if_defined(:${ctx.name}?, $tresql))"
    else tresql

  private def update_tresql(ctx: SaveContext)(implicit resources: Resources): String = if (ctx.forUpdate) {
    def filterString(filters: Option[Filters], extraction: Filters => Option[String]): String =
      filters.flatMap(extraction).map(f => s" & ($f)").getOrElse("")

    import ctx._
    val parent = parents.headOption.map(_.table).orNull
    val tableName = table.name

    def delAllChildren =
      s"-${tableWithDb(view.db, tableName, view.alias)}[$refToParent = :#$parent${filterString(ctx.view.filters, _.delete)}]"
    def delMissingChildren = {
      val delCtx = ctx.copy(view = ctx.view.copy(saveTo = List(saveTo)))
      val md = tresqlMetadata(delCtx.view.db)
      md.tableOption(delCtx.saveTo.table).map { delTable =>
        val (refsPkVals, keyVals) = refsPkAndKey(delCtx, delTable, Set(), saveTo.key)
        def refsAndKey(rk: Set[IdOrRefVal]) = rk.filter(!_.isInstanceOf[IdVal])
          .partition(_.isInstanceOf[IdParentVal]) match {
          case (refs, pks) =>
            val pk = delTable.key.cols
            (refs.map(r => (r.name, r.value)).toList,
              if (pk.forall(c => pks.exists(_.name == c) || refs.exists(_.name == c)))
                pks.map(p => (p.name, p.value)).toList
              else Nil
            )
        }

        val (refCols: List[(String, String)], keyCols: List[(String, String)]) =
          if (keyVals.nonEmpty) keyVals.partition(_._1.isInstanceOf[RefKeyCol]) match {
            case (refCols: List[(RefKeyCol, String)@unchecked], keyCols: List[(KeyCol, String)@unchecked]) =>
              val (rc, kc) =
                (refCols.map { case (k, v) => (k.name, v) }, keyCols.map { case (k, v) => (k.name, v) })
              if (rc.nonEmpty) (rc, kc) else (refsAndKey(refsPkVals)._1, kc)
          } else refsAndKey(refsPkVals)

        val refColsFilter = refCols.map { case (n, v) => s"$n = $v" }.mkString(" & ")
        val (key_arr, key_val_expr_arr) = keyCols.unzip match {
          case (kc, kv) => (s"[${kc.mkString(", ")}]", s"[${kv.mkString(", ")}]")
        }
        val filter = filterString(delCtx.view.filters, _.delete)
        if (refCols.isEmpty || keyCols.isEmpty) null
        else
          s"""_delete_missing_children('$name', $key_arr, $key_val_expr_arr, -${
            tableWithDb(delCtx.view.db,
              delCtx.saveTo.table, delCtx.view.alias)
          }[$refColsFilter & _not_delete_keys($key_arr, $key_val_expr_arr)$filter])"""
      }.orNull
    }
    def upd = {
      def table_save_tresql(data: SaveData) = {
        import data._
        val filteredColsVals =  // filter out key columns from updatable columns where value match key search value
          (if (keyVals.nonEmpty)
            colsVals.collect {
              case cv @ ColVal(c, v, _, true, uvo)
                if !keyVals.exists { case (k, kv) => k.name == c && v == kv } =>
                uvo.map(uv => cv.copy(value = uv)).getOrElse(cv)
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
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, _, _, _, _, update_tresql))
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
      save_tresql_internal(ctx, table_save_tresql, save_tresql(_, _, _, _, _, _, _, insert_tresql))
    }
    def updOrIns = s"|_upsert($upd, $ins)"

    def mayBeDef(tresql: String) = mayBeDeferredTresql(ctx, tresql)

    import ctx.saveOptions._
    val pk = table.key.cols
    if (parent == null) if (saveTo.key.isEmpty && pk.isEmpty) null else mayBeDef(upd)
    else if (pk.size == 1 && refToParent == pk.head) mayBeDef(upd) else
      if (saveTo.key.isEmpty && pk.isEmpty) {
        (Option(doDelete).filter(_ == true).map(_ => mayBeDef(delAllChildren)) ++
          Option(doInsert).filter(_ == true)
            .flatMap(_ => Option(mayBeDef(ins)))).mkString(", ")
      } else {
        (Option(doDelete).filter(_ == true)
          .map(_ => mayBeDef(if(!doUpdate) delAllChildren else delMissingChildren)) ++
          ((doInsert, doUpdate) match {
            case (true, true) => Option(mayBeDef(updOrIns))
            case (true, false) => Option(mayBeDef(ins))
            case (false, true) => Option(mayBeDef(upd))
            case (false, false) => None
          })).mkString(", ")
      }
  } else null

  private def save_tresql_internal(
    ctx: SaveContext,
    table_save_tresql: SaveData => String,
    children_save_tresql: (
      String, //children property name
      View,
      List[ParentRef], //parent chain
      SaveOptions,
      Boolean, // optional flag
      Boolean, // forInsert flag
      Boolean, // forUpdate flag
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
      def hasOptionalFields(view: View) = view.properties.exists(_.optional)
      def autoValue(tbl: metadata.Table, col: String, value: String, autoRef: Boolean): String =
        if (tbl.key.cols.contains(col) && value.startsWith(":")) {
          (if (autoRef) ":" else "") + s"#${tbl.name}$value"
        } else value

      ctx.view.properties.flatMap {
        case OrtMetadata.Property(col, _, _, _, _) if refsAndPk.exists(_.name == col) => Nil
        case OrtMetadata.Property(col, KeyValue(_, TresqlValue(valueTresql), updValOpt), _, forInsert, forUpdate) =>
          List(ColVal(table.colOption(col).map(_.name).orNull,
            valueTresql, forInsert, forUpdate, updValOpt.map(_.tresql)))
        case OrtMetadata.Property(col, KeyValue(_, AutoValue(valueTresql), updValOpt), _, forInsert, forUpdate) =>
          List(ColVal(table.colOption(col).map(_.name).orNull,
            autoValue(table, col, valueTresql, false), forInsert, forUpdate,
            updValOpt.map {
              case AutoValue(v) => autoValue(table, col, v, true)
              case v => v.tresql
            }))
        case OrtMetadata.Property(col, TresqlValue(tresql), _, forInsert, forUpdate) =>
          val (t, c) = col.split("\\.") match {
            case Array(a, b) => (a, b)
            case Array(a) => (table.name, a)
            case a => (a.dropRight(1).mkString("."), a.last)
          }
          if (t == table.name)
            List(ColVal(table.colOption(c).map(_.name).orNull, tresql, forInsert, forUpdate))
          else Nil
        case OrtMetadata.Property(prop, ViewValue(v, so), optional, forInsert, forUpdate) =>
          if (children_save_tresql != null) {
            val isOptional = optional || hasOptionalFields(v)
            val chtresql = children_save_tresql(prop, v,
              ParentRef(table.name, ctx.refToParent) :: ctx.parents, so, isOptional, forInsert, forUpdate)
            val chtresql_alias = Option(prop).map(p => s" '$p'").mkString
            List(ColVal(Option(chtresql).map(_ + chtresql_alias).orNull, null, forInsert, forUpdate))
          } else Nil
        case OrtMetadata.Property(refColName, LookupViewValue(propName, lookupView), optional, forInsert, forUpdate) =>
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
                  case OrtMetadata.Property(col, TresqlValue(v), _, _, _) if key.contains(col) =>
                    List((col, v))
                  case OrtMetadata.Property(col, KeyValue(where, _, _), _, _, _) if key.contains(col) =>
                    List((col, where))
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
                  case OrtMetadata.Property(`pk`, TresqlValue(v), _, _, _) =>
                    if (v.startsWith(":")) v.substring(1) else pk
                  case OrtMetadata.Property(`pk`, KeyValue(_, ov, _), _, _, _) =>
                    val v = ov.tresql
                    if (v.startsWith(":")) v.substring(1) else pk
                }
              }.orNull
            }
            val idPropName = idProp(lookupView, lookupTable)
            val update = save_tresql(null, lookupView, Nil,
              SaveOptions(true, false, true), false, true, true, update_tresql)
            val insert = save_tresql(null, lookupView, Nil,
              SaveOptions(true, false, true), false ,true, true, insert_tresql)
            val lookupUpsert = {
              val tr = s"|_upsert($update, $insert)"
              if (hasOptionalFields(lookupView)) s"_deferred_build($tr)"
              else tr
            }
            val lookupIdSel = idSelExpr(lookupView, lookupTable)
            val (lookupTresql, refColTresql) = {
              val tr = s":$refColName = |_lookup_upsert('$propName', ${
                if (idPropName == null) "null" else s"'$idPropName'"}, $lookupUpsert, $lookupIdSel)"
              if (optional)
                (s"_deferred_build(if_defined(:$propName?, $tr))", s"if_defined(:$propName?, :$refColName)")
              else
                (tr, s":$refColName")
            }
            // TODO lookupTresql will be executed always regardless of forInsert, forUpdate settings
            List(lookupTresql, ColVal(refColName, refColTresql, forInsert, forUpdate))
          }).getOrElse(Nil)
        case OrtMetadata.Property(col, AutoValue(tresql), _, forInsert, forUpdate) =>
          List(ColVal(table.colOption(col).map(_.name).orNull,
            autoValue(table, col, tresql, false), forInsert, forUpdate))
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
    md.tableOption(headTable.table).flatMap { tableDef =>
      val linkedTresqls =
        for {
          linkedTable <- ctx.view.saveTo.tail
          linkedTableDef <- md.tableOption(linkedTable.table)
        } yield
          tresql_string(linkedTableDef, ctx.view.alias,
            refsPkAndKey(ctx, linkedTableDef, linkedTable.refs, linkedTable.key),
            Nil, true) //no children, set upsert flag for linked table

      Option(tresql_string(tableDef, ctx.view.alias, refsPkAndKey(ctx, tableDef, Set(), headTable.key),
        linkedTresqls.filter(_ != null), false))
    }.orNull
  }

  private def refsPkAndKey(
    ctx: SaveContext,
    tbl: metadata.Table,
    linkedRefs: Set[String],
    key: Seq[String]
  ): (Set[IdOrRefVal], Seq[(KeyPart, String)]) = {
    def idRefId(idRef: String, id: String) = s"_id_ref_id($idRef, $id)"

    def idExps: Set[IdOrRefVal] = {
      val t = tbl.name
      val colNames = ctx.view.properties.map(_.col).toSet
      val excludeCols = (linkedRefs + ctx.refToParent) ++ key.toSet.filter(colNames(_))
      val pk = tbl.key.cols.filterNot(excludeCols(_))
      val useBindVarName = pk.size > 1

      def exps(n: String, idVal: String, idRefVal: String): Set[IdOrRefVal] =
        Set(IdVal(n, idVal), IdRefVal(n, idRefVal))

      def idExp(t: String, useBindVarTresql: Boolean, pk: String): Set[IdOrRefVal] =
        ctx.view.properties.collectFirst {
          case OrtMetadata.Property(`pk`, TresqlValue(tresql), _, _, _)
            if tresql.startsWith(":") && (tresql.substring(1) != pk || useBindVarTresql) =>
            exps(pk, s"#$t$tresql", s":#$t$tresql")
          case OrtMetadata.Property(`pk`, KeyValue(_, insVal, updVal), _, _, _) =>
            val insTr = insVal.tresql
            val (idVal, idRefVal1) =
              if (insTr.startsWith(":")) (s"#$t$insTr", s":#$t$insTr")
              else (insTr, insTr)
            val idRefVal = updVal.map {
                case v if v.tresql.startsWith(":") => s":#$t${v.tresql}"
                case v => v.tresql
              }
              .getOrElse(idRefVal1)
            exps(pk, idVal, idRefVal)
          case OrtMetadata.Property(`pk`, _: LookupViewValue, _, _, _) => Set[IdOrRefVal]() // lookup statement should handle value
        }.getOrElse(exps(pk, s"#$t:$pk", s":#$t:$pk"))

      pk.flatMap(idExp(t, useBindVarName, _)).toSet
    }

    val parent = ctx.parents.headOption.map(_.table).orNull
    val headTable = ctx.view.saveTo.head
    val ref_to_parent_and_pk: Set[IdOrRefVal] = {
      //ref table (set fk and pk)
      (if (tbl.name == ctx.table.name && ctx.refToParent != null) {
        val pks = tbl.key.cols
        val refStr =
          if (pks.size == 1 && pks.contains(ctx.refToParent)) idRefId(parent, tbl.name)
          else s":#$parent"
        Set(IdParentVal(ctx.refToParent, refStr))
      } else Set()) ++ idExps
    }
    val refsPk = ref_to_parent_and_pk ++ //set linked refs
      (if (tbl.name == headTable.table)
        Set()
      else //filter out pk of the linked table in case it matches refToParent
        linkedRefs
          .filterNot(tbl.name == ctx.table.name && _ == ctx.refToParent)
          .map(IdParentVal(_, idRefId(headTable.table, tbl.name)))
        )
    (refsPk,
      key.map(k =>
        refsPk.collectFirst { case x if x.name == k && x.name == ctx.refToParent =>
            RefKeyCol(x.name) -> x.value
          }
          .getOrElse(
            KeyCol(k) -> ctx.view.properties
              .collectFirst {
                case OrtMetadata.Property(`k`, TresqlValue(v), _, _, _) => v
                case OrtMetadata.Property(`k`, KeyValue(whereTresql, _, _), _, _, _) => whereTresql
              }
              .getOrElse(s":$k")
          )
      )
    )
  }
}

object OrtMetadata {
  sealed trait OrtValue
  sealed trait OrtSimpleValue extends OrtValue { def tresql: String }

  /** Column value
   *
   * @param tresql        tresql statement
   * */
  case class TresqlValue(tresql: String) extends OrtSimpleValue

  /** Column value
   *  If tresql value is bind variable and column name of containing {{{Property}}} corresponds to
   *  current table's primary key column id or id ref expressions will appear in ort's generated
   *  insert or update statement.
   *  Otherwise behaves like {{{TresqlValue}}}
   * @param tresql tresql statement
   * */
  case class AutoValue(tresql: String) extends OrtSimpleValue

  /** Column value
   * @param view          child view
   * @param saveOptions   saveable options [+-=]
   * */
  case class ViewValue(view: View, saveOptions: SaveOptions) extends OrtValue

  /** Column value
   *
   * @param propName      property name (from environment)
   * @param view          child view
   * */
  case class LookupViewValue(propName: String, view: View) extends OrtValue

  /** Column value
   * @param whereTresql   key find tresql
   * @param valueTresql   key value tresql
   * */
  case class KeyValue(whereTresql: String,
                      valueTresql: OrtSimpleValue,
                      updValueTresql: Option[OrtSimpleValue] = None) extends OrtValue

  /** Saveable column.
   *
   * @param col           Goes to dml column clause or child tresql alias
   * @param value         Goes to dml values clause or column clause as child tresql
   * @param optional      Indicates whether column value may be absent
   *                      NOTE: if value is {{{TresqlValue}}} optional flag is ignored. Optionality must be provided
   *                      in tresql expression
   * @param forInsert     Column value is to be included into insert statement if true
   * @param forUpdate     Column value is to be included into update statement if true
   * */
  case class Property(col: String, value: OrtValue, optional: Boolean, forInsert: Boolean, forUpdate: Boolean)

  /** Saveable view.
   * @param saveTo        destination tables. If first table has {{{refs}}} parameter
   *                      it indicates reference field to parent.
   * @param filters       horizontal authentication filters
   * @param alias         table alias in DML statement
   * @param properties    saveable fields
   * @param db            database name (can be null)
   * */
  case class View(saveTo: Seq[SaveTo],
                  filters: Option[Filters],
                  alias: String,
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
      (View(saveTo(tables, tresqlMetadata(db)), filters, alias, Nil, db), SaveOptions(i, u, d))
    }
    def resolver_tresql(property: String, resolverExp: String) = {
      import ast._
      val OrtMetadata.Patterns.resolverProp(prop) = property
      val OrtMetadata.Patterns.resolverExp(col, exp) = resolverExp
      val parser = new QueryParser(resources, resources.cache)
      OrtMetadata.Property(col, TresqlValue(
        parser.transformer {
          case Ident(List("_")) => Variable(prop, Nil, opt = false)
        } (parser.parseExp(if (exp startsWith "(" ) exp else s"($exp)")).tresql
      ), false, true, true)
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
                OrtMetadata.Property(prop, LookupViewValue(prop, ortMetadata(lookupTable, m)._1), false, true, true)
            }.getOrElse {
              val (v, so) = ortMetadata(prop, m)
              OrtMetadata.Property(prop, ViewValue(v, so), false, true, true)
            }
          case v: String if prop.indexOf("->") != -1 => resolver_tresql(prop, v)
          case _ => OrtMetadata.Property(prop, TresqlValue(s":$prop"), false, true, true)
        }
      }.toList
    (view.copy(properties = props), saveOptions)
  }
}

object ORT extends ORT
