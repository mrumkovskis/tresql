package org

package object tresql extends CoreTypes {

  /**
   *  tresql string interpolator.
   *  NOTE: If variable reference in tresql string ends with ?
   *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
   *  passed to Query.
   */

  implicit val resources: Resources = Env

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
    def tresql(params: Any*)(implicit resources: Resources): Result = macro Macros.impl
  }

  object Macros {
    import scala.language.reflectiveCalls //supress warnings that class Ctx is defined in function resultClassTree and later returned
    import scala.language.existentials //supress warnings that class Ctx is defined in function resultClassTree and later returned
    import QueryCompiler._
    def impl(c: Context)(params: c.Expr[Any]*)(resources: c.Expr[Resources]): c.Expr[Result] = {
      import c.universe._
      import CoreTypes.RowConverter
      case class Ctx(
        //class name which extends RowLike, can be type projection (class1#class2) in case of child queries
        //in reverse order - starts with innermost child
        className: List[String],
        tree: List[c.Tree],
        depth: Int, //select descendancy idx (i.e. QueryBuilder.queryDepth)
        colIdx: Int,
        convRegister: List[c.Tree] //Map of converters for Env in form of (Int, Int) -> RowConveter[_]
      )
      def resultClassTree(exp: Exp): Ctx = {
        lazy val generator: PartialFunction[(Ctx, Exp), Ctx] = extractorAndTraverser {
          case (ctx, sd: SelectDef) =>
            val className = c.freshName("Tresql")
            case class ColsCtx(colTrees: List[List[c.Tree]], colIdx: Int, convRegister: List[c.Tree])
            val colsCtx = sd.cols
              .foldLeft(ColsCtx(Nil, 0, ctx.convRegister)) { case (colCt, c) =>
                val ct = generator(Ctx(
                  className :: ctx.className,
                  Nil,
                  ctx.depth,
                  colCt.colIdx,
                  colCt.convRegister
                ) -> c)
                ColsCtx(ct.tree :: colCt.colTrees, colCt.colIdx + 1, ct.convRegister)
              }
            val (fieldDefs, fieldConvs, children) = colsCtx.colTrees
              .map (l => (l.head, l.tail.head, l.tail.tail)) //first two values are field def, field converter, the rest is children
              .foldLeft((List[c.Tree](), List[c.Tree](), List[c.Tree]())) {
                case ((fs, cvs, chs), (f, cv, ch)) => (f :: fs, cv :: cvs, chs ++ ch)
              }
            val typeName = TypeName(className)
            val classDef = q"""
              class $typeName extends org.tresql.CompiledRow {
                ..$fieldDefs
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
            //depth for select starts with 1
            val converterRegister = q"((${ctx.depth + 1} , ${ctx.colIdx}), $converterName)"
            (Ctx(
              className :: ctx.className,
              classDef :: converterDef :: children,
              ctx.depth,
              ctx.colIdx,
              converterRegister :: colsCtx.convRegister
            ), false)
          case (ctx, ColDef(name, ChildDef(sd: SelectDef), typ)) =>
            val selDefCtx = generator(
              Ctx(ctx.className, Nil, ctx.depth + 1, ctx.colIdx, ctx.convRegister) -> sd)
            val fieldDef =
              q"""var ${TermName(name)}: org.tresql.CompiledResult[${
                TypeName(selDefCtx.className.head)}] = _"""
            //user type projection: child type in selDefCtx, parent in ctx
            //FIXME bizzare way of getting type projection, did not find another way...
            val q"typeOf[$childClassType]" = c.parse(s"typeOf[${selDefCtx.className.head}]")
            val fieldConv =
              q"""obj.${TermName(name)} =
                row.typed[org.tresql.CompiledResult[$childClassType]](${ctx.colIdx})"""
            (ctx.copy(
              tree = fieldDef :: fieldConv :: selDefCtx.tree,
              convRegister = selDefCtx.convRegister
            ), false)
          case (ctx, ColDef(name, ChildDef(_), typ)) =>
            // TODO unsupported at the moment
            (ctx, false)
          case (ctx, ColDef(name, _, typ)) =>
            //FIXME bizzare way of getting qualified type name, did not find another way...
            val q"typeOf[$colType]" = c.parse(s"typeOf[${typ.toString}]")
            //first element of l is field def, second field convertion row which will be placed in converter
            val l = List(q"var ${TermName(name)}: $colType = _",
             q"obj.${TermName(name)} = row.typed[$colType](${ctx.colIdx})")
            (ctx.copy(tree = l), false)
        }
        generator((Ctx(Nil, Nil, 0, 0, Nil), exp))
      }
      val macroSettings = settings(c.settings)
      val verbose = macroSettings.contains("verbose")
      def log(msg: Any) = if (verbose) println(msg)
      log(s"Macro compiler settings:\n$macroSettings")
      val q"org.tresql.`package`.Tresql(scala.StringContext.apply(..$parts)).tresql(..$pars)($res)" =
        c.macroApplication
      val tresqlString = parts.map { case Literal(Constant(x)) => x } match {
        case l => l.head + l.tail.zipWithIndex.map(t => ":_" + t._2 + t._1).mkString //replace placeholders with variable defs
      }
      Env.metaData = metadata(macroSettings)
      log(s"Compiling: $tresqlString")
      val compiledExp = compile(tresqlString)
      val resultClassCtx = resultClassTree(compiledExp)
      val (classDefs, convRegister) =
        if (resultClassCtx != null) (resultClassCtx.tree, resultClassCtx.convRegister)
        else (Nil, Nil)
      log("------------Class:------------\n")
      log(classDefs) //showCode does not work
      log("------------Converter register:----------\n")
      log(convRegister) //showCode does not work
      val tree = q"""
        ..$classDefs
        var optionalVars = Set[Int]()
        val query = new Query {
          override def converters =
            Map[(Int, Int), RowConverter[_]](..$convRegister)
        }
        Query(${parts.head} + List[String](..${parts.tail}).zipWithIndex.map { case (part, idx) =>
          if (part.trim.startsWith("?")) optionalVars += idx
          ":_" + idx + part
        }.mkString, List[Any](..$params)
          .zipWithIndex
          .filterNot { case (param, idx) => param == null && (optionalVars contains idx) }
          .map { case (param, idx) => ("_" + idx) -> param }.toMap)($res)"""
      c.Expr(tree)
    }
    def settings(sett: List[String]) = sett.map { _.split("=") match {
      case Array(key) => key.trim -> ""
      case Array(key, value) => key.trim -> value.trim
      case x => sys.error(s"Setting must be in format <key>=<value> or <key>. Instead found: $x")
    }}.toMap
    def metadata(conf: Map[String, String]) = conf.get("metadataFactoryClass").map { factory =>
      Class.forName(factory).newInstance.asInstanceOf[compiling.CompilerMetaData].create(
        conf.getOrElse("driverClass", null),
        conf.getOrElse("url", null),
        conf.getOrElse("user", null),
        conf.getOrElse("password", null),
        conf.getOrElse("dbCreateScript", null)
      )
    }.getOrElse(
      sys.error(s"metadataFactoryClass macro compiler setting missing. Try to set -Xmacro-settings: scala compiler option.")
    )
  }

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
