package org

package object tresql extends CoreTypes {

  /**
   *  tresql string interpolator.
   *  NOTE: If variable reference in tresql string ends with ?
   *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
   *  passed to Query.
   */

  //whitebox context is important (not blackbox) since return value of tresql function
  //must return type actually returned from Macro.impl i.e not CompiledResult[RowLike]
  import scala.reflect.macros.whitebox.Context
  import scala.language.experimental.macros
  /**
    Compiles tresql statement and returns compiled result.
    For tresql select definition returned result of type see example:
    {{{
    //class reflecting result row
    class Dept extends CompiledRow {
      var deptno: java.lang.Integer = _
      var dname: java.lang.String = _
      var emps: org.tresql.CompiledResult[Emp] = _
      override def apply(idx: Int) = idx match {
        case 0 => deptno
        case 1 => dname
        case 2 => emps
      }
      override def columnCount = 3
      override val columns = Vector(
        org.tresql.Column(-1, "deptno", null),
        org.tresql.Column(-1, "dname", null),
        org.tresql.Column(-1, "emps", null)
      )
    }
    //RowConverter definition
    object Dept extends RowConverter[Dept] {
      def apply(row: RowLike): Dept = {
        val obj = new Dept
        obj.deptno = row.typed[java.lang.Integer](0)
        obj.dname = row.typed[java.lang.String](1)
        obj.emps = row.typed[org.tresql.CompiledResult[Emp]](2)
        obj
      }
    }
    class Emp extends CompiledRow {
      var empno: java.lang.Integer = _
      var ename: java.lang.String = _
      var hiredate: java.sql.Date = _
      override def apply(idx: Int) = idx match {
        case 0 => empno
        case 1 => ename
        case 2 => hiredate
      }
      override def columnCount = 3
      override val columns = Vector(
        org.tresql.Column(-1, "empno", null),
        org.tresql.Column(-1, "ename", null),
        org.tresql.Column(-1, "hiredate", null)
      )
    }
    object Emp extends RowConverter[Emp] {
      def apply(row: RowLike): Emp = {
        val obj = new Emp
        obj.empno = row.typed[java.lang.Integer](0)
        obj.ename = row.typed[java.lang.String](1)
        obj.hiredate = row.typed[java.sql.Date](2)
        obj
      }
    }
  }}}
  */
  implicit class Tresql(val sc: StringContext) extends AnyVal {
    def tresql(params: Any*)(implicit resources: Resources): Any = macro Macros.impl
  }

  private object Macros {
    import scala.language.reflectiveCalls //supress warnings that class Ctx is defined in function resultClassTree and later returned
    import scala.language.existentials //supress warnings that class Ctx is defined in function resultClassTree and later returned
    import QueryCompiler._
    def impl(c: Context)(params: c.Expr[Any]*)(resources: c.Expr[Resources]): c.Expr[Result[RowLike]] = {
      import c.universe._
      import CoreTypes.RowConverter
      case class Ctx(
        //class name which extends RowLike
        className: String,
        tree: List[c.Tree], //class (field) & converter definition.
        depth: Int, //select descendancy idx (i.e. QueryBuilder.queryDepth)
        colIdx: Int, //column idx - used in converter
        childIdx: Int, //child select index - used to get converter from env
        convRegister: List[c.Tree], //Map of converters for Env in form of (Int, Int) -> RowConveter[_]
        colNames: Set[String], //field names (stored in order to avoid duplicates)
        colType: Tree, //filled by ColDef, may be used in result converter
        resultConverter: Option[(String, c.Tree)] //function in form (functionName -> CompiledResult[_] -> T)
      )
      def resultClassTree(exp: parsing.Exp): Ctx = {
        def uniqueName(prefix: String, names: Set[String]) = if (names(prefix)) {
          prefix + Stream.from(1).filterNot(i => names(prefix + i)).head
        } else prefix
        def typeNameFromManifest(m: Manifest[_]) = {
          //FIXME bizzare way of getting qualified type name, did not find another way...
          val q"typeOf[$colType]" = c.parse(s"typeOf[${m.toString}]")
          colType
        }
        lazy val generator: QueryCompiler.Traverser[Ctx] = traverser(ctx => {
          case f: FunDef[_] => ctx //for sql_concat function
            .copy(convRegister =
              q"((${ctx.depth}, ${ctx.childIdx}), identity[RowLike] _)" :: ctx.convRegister)
          //inserts updates deletes
          case dml: DMLDefBase => ctx.copy(className = "DMLResult")
          case pd: PrimitiveDef[_] =>
            val name = c.freshName("tresqlResultConv")
            val funName = TermName(name)
            val typeName = typeNameFromManifest(pd.typ)
            val conv =
              q"""
                 def $funName(res: org.tresql.Result[_]): $typeName = {
                    res.head[$typeName]
                 }
               """
            ctx.copy(convRegister =
              q"((${ctx.depth}, ${ctx.childIdx}), identity[RowLike] _)" :: ctx.convRegister,
              resultConverter = Some(name, conv))
          //queries are compiled to CompiledResult[_ <: RowLike], arrays are compiled to tuples.
          case sd: RowDefBase =>
            val className = c.freshName("Tresql")
            val resultConv = Option(sd).collect {
              case _: ArrayDef => (c.freshName("tresqlResultConv"), null)
            }
            case class ColsCtx(
              colTrees: List[List[c.Tree]],
              colIdx: Int,
              childIdx: Int,
              convRegister: List[c.Tree],
              colNames: Set[String],
              colTypes: List[c.Tree],
              resultConvs: List[(String, c.Tree)]
            )
            val colsCtx = sd.cols
              .foldLeft(ColsCtx(Nil, 0, 0, ctx.convRegister, Set(), Nil, Nil)) { case (colCt, col) =>
                val ct = generator(Ctx(
                  className,
                  Nil,
                  ctx.depth,
                  colCt.colIdx,
                  colCt.childIdx,
                  colCt.convRegister,
                  colCt.colNames,
                  null,
                  None
                ))(col)
                ColsCtx(
                  ct.tree :: colCt.colTrees,
                  colCt.colIdx + 1,
                  ct.childIdx,
                  ct.convRegister,
                  ct.colNames,
                  ct.colType :: colCt.colTypes,
                  resultConv
                    .map { _ => ct.resultConverter.getOrElse((null, null)) :: colCt.resultConvs }
                    .getOrElse(Nil)
                )
              }
            val (fieldDefs, fieldConvs, fieldTerms, children) = colsCtx.colTrees
              //first three values are field def, field converter, fieldName (as term) and the rest is children
              .map (l => (l(0), l(1), l(2), l drop 3))
              .foldLeft((List[c.Tree](), List[c.Tree](), List[c.Tree](), List[c.Tree]())) {
                case ((fs, cvs, fts, chs), (f, cv, ft, ch)) =>
                  (f :: fs, cv :: cvs, ft :: fts, chs ++ ch)
              }
            val colsByIdx = fieldTerms.zipWithIndex.map {case (t, i) => cq"$i => $t"}
            val colDefs = fieldTerms.map { cn => q"org.tresql.Column(-1, ${cn.toString}, null)" }
            val typeName = TypeName(className)
            val classDef = q"""
              class $typeName extends org.tresql.CompiledRow {
                ..$fieldDefs
                override def apply(idx: Int) = idx match {
                  case ..$colsByIdx
                }
                override def columnCount = ${colsByIdx.size}
                override val columns = Vector(..$colDefs)
              }
            """
            val converterName = TermName(className)
            val converterDef = q"""
              object $converterName extends org.tresql.CoreTypes.RowConverter[$typeName] {
                def apply(row: org.tresql.RowLike): $typeName = {
                  val obj = new $typeName
                  ..$fieldConvs
                  obj
                }
              }
            """
            val converterRegister = q"((${ctx.depth}, ${ctx.childIdx}), $converterName)"
            def maybeResConv(colTypes: List[c.Tree], membResConvs: List[(String, c.Tree)]) =
              resultConv.map {
                case (n, _) =>
                  val membConvInvocations =
                    (colTypes zip membResConvs).zipWithIndex map { case ((ct, (mn, _)), i) =>
                      if (mn == null)
                        if (ct == null) q"res.typed[CompiledResult[_]]($i)"
                        else q"res.typed[CompiledResult[$ct]]($i)"
                      else
                        q"${TermName(mn)}(res.typed[Result[_]]($i))"
                    }
                  (n, q"""def ${TermName(n)}(res: CompiledResult[_]) = {
                            ..${membResConvs.filter(_._1 != null).map(_._2)}
                            (..$membConvInvocations)
                          }""")
              }
            Ctx(
              className,
              classDef :: converterDef :: children,
              ctx.depth,
              ctx.colIdx,
              ctx.childIdx,
              converterRegister :: colsCtx.convRegister,
              Set(),
              null,
              maybeResConv(colsCtx.colTypes.reverse, colsCtx.resultConvs.reverse)
            )
          case ColDef(colName, ChildDef(sd: RowDefBase), _) =>
            val selDefCtx = generator(Ctx(ctx.className, Nil, ctx.depth + 1,
              ctx.colIdx, ctx.childIdx, ctx.convRegister,
              Set(), null, ctx.resultConverter))(sd)
            val name = uniqueName(colName, ctx.colNames)
            val fieldTerm = q"${TermName(name)}"
            val fieldDef =
              q"""var ${TermName(name)}: org.tresql.CompiledResult[${
                TypeName(selDefCtx.className)}] = _"""
            //user type projection: child type in selDefCtx, parent in ctx
            val fieldConv =
              q"""obj.${TermName(name)} = row.typed[org.tresql.CompiledResult[${
                TypeName(selDefCtx.className)}]](${ctx.colIdx})"""
            ctx.copy(
              tree = fieldDef :: fieldConv :: fieldTerm :: selDefCtx.tree,
              childIdx = ctx.childIdx + 1,
              convRegister = selDefCtx.convRegister,
              colNames = ctx.colNames + name,
              colType = tq"${TypeName(selDefCtx.className)}"
            )
          case ColDef(_, pd: PrimitiveDef[_], _) =>
            val nctx = generator(ctx)(pd)
            nctx.copy(tree = List(q"", q"", q"identity[RowLike] _"))
          case ColDef(colName, _, typ) =>
            val name = uniqueName(colName, ctx.colNames)
            val fieldTerm = q"${TermName(name)}"
            val colType = typeNameFromManifest(typ)
            //first element of l is field def, second field convertion row which will be placed in converter
            val l = List(
              q"var ${TermName(name)}: $colType = _",
              q"obj.${TermName(name)} = row.typed[$colType](${ctx.colIdx})",
              fieldTerm
            )
            ctx.copy(tree = l, colNames = ctx.colNames + name, colType = q"$colType")
        })
        generator(Ctx(null, Nil, 0, 0, 0, Nil, Set(), null, None))(exp)
      }
      val macroSettings = settings(c.settings)
      val verbose = macroSettings.contains("verbose")
      if (verbose) {
        Env.logger = (msg, params, _) => {
          println(msg)
          if (params != null && params.nonEmpty) println(s" Params: $params")
        }
        Env.log("Verbose flag set")
      }
      def info(msg: Any) = if (verbose) c.info(c.enclosingPosition, String.valueOf(msg), false)
      Env.log(s"Macro compiler settings:\n$macroSettings")
      val q"org.tresql.`package`.Tresql(scala.StringContext.apply(..$parts)).tresql(..$pars)($res)" =
        c.macroApplication
      val tresqlString = parts.map { case Literal(Constant(x)) => x } match {
        case l => l.head + l.tail.zipWithIndex.map(t => ":_" + t._2 + t._1).mkString //replace placeholders with variable defs
      }
      val compilerMetadata = metadata(macroSettings)
      implicit val resrc = new Resources {}
        .withMetadata(compilerMetadata.metadata)
        .withMacros(compilerMetadata.macros)
      info(s"Compiling: $tresqlString")
      val compiledExp = try compile(tresqlString) catch {
        case ce: CompilerException => c.abort(c.enclosingPosition, ce.getMessage)
      }
      val resultClassCtx = resultClassTree(compiledExp)
      val resultClassName = resultClassCtx.className match {
        case null => "org.tresql.RowLike" case n => n }
      val q"typeOf[$classType]" = c.parse(s"typeOf[$resultClassName]")
      val (classDefs, convRegister, resultConv) =
        (resultClassCtx.tree, resultClassCtx.convRegister, resultClassCtx.resultConverter)
      info("\n------------Result converter:------------\n")
      info(resultConv) //showCode does not work
      info("\n------------Class:------------\n")
      info(classDefs) //showCode does not work
      info("\n------------Converter register:----------\n")
      info(convRegister) //showCode does not work
      val defsTree = resultConv.map { case (_, c) => c :: classDefs }.getOrElse(classDefs)
      val resTree = q"""
        var optionalVars = Set[Int]()
        val query = new Query {
          override def converters =
            Map[(Int, Int), RowConverter[_ <: RowLike]](..$convRegister)
        }
        def result = query.compiledResult[$classType](
          ${parts.head} + List[String](..${parts.tail})
            .zipWithIndex.map { case (part, idx) =>
              if (part.trim.startsWith("?")) optionalVars += idx
              ":_" + idx + part
            }.mkString, List[Any](..$params)
              .zipWithIndex
              .filterNot { case (param, idx) => param == null && (optionalVars contains idx) }
              .map { case (param, idx) => ("_" + idx) -> param }.toMap
        )($res)
        ${ resultConv
             .map {case (n, _) => q"${TermName(n)}(result)"}
             .getOrElse(q"result")
        }
        """
      val tree =
        q"""
           ..$defsTree
           $resTree
           """
      c.Expr(tree)
    }
    def settings(sett: List[String]) = sett.map { s => s.split("=") match {
      case Array(key) => key.trim -> ""
      case Array(key, value) => key.trim -> value.trim
      case _ => sys.error(s"Setting must be in format <key>=<value> or <key>. Instead found: $s")
    }}.toMap
    def metadata(conf: Map[String, String]) = conf.get("metadataFactoryClass").map { factory =>
      compiling.MetadataCache.create(
        conf,
        Class.forName(factory).getDeclaredConstructor().newInstance()
          .asInstanceOf[compiling.CompilerMetadataFactory]
      )
    }.getOrElse(
      sys.error(s"Tresql interpolator not available. Scala macro compiler setting missing - 'metadataFactoryClass'. " +
        s"Try to set -Xmacro-settings scala compiler option. " +
        s"Example: -Xmacro-settings:metadataFactoryClass=org.tresql.compiling.CompilerJDBCMetadataFactory")
    )
  }

  /** Does not refer to scala compiler macro. Is placed here to be in the package object tresql */
  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new DynamicSelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), new Env(Map[String, Any](), new Resources {}, false), "<not available>", Nil)
  }
}
