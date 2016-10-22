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
  implicit class Tresql(val sc: StringContext) extends AnyVal {
    def tresql(params: Any*)(implicit resources: Resources): Result = macro Macros.impl
  }

  object Macros {
    def impl(c: Context)(params: c.Expr[Any]*)(resources: c.Expr[Resources]): c.Expr[Result] = {
      import c.universe._
      val q"org.tresql.`package`.Tresql(scala.StringContext.apply(..$parts)).tresql(..$pars)($res)" =
        c.macroApplication
      val tresqlString = parts.map { case Literal(Constant(x)) => x } match {
        case l => l.head + l.tail.zipWithIndex.map(t => ":_" + t._2 + t._1).mkString //replace placeholders with variable defs
      }
      Env.metaData = metadata(settings(c.settings))
      Env.log(s"Compiling: $tresqlString")
      val compiledExp = QueryCompiler.compile(tresqlString)
      val tree = q"""
        var optionalVars = Set[Int]()
        Query(${parts.head} + List[String](..${parts.tail}).zipWithIndex.map { t =>
          if (t._1.trim.startsWith("?")) optionalVars += t._2
          ":_" + t._2 + t._1
        }.mkString, List[Any](..$params)
          .zipWithIndex
          .filterNot(t => t._1 == null && (optionalVars contains t._2))
          .map(t => ("_" + t._2) -> t._1).toMap)($res)"""
      c.Expr(tree)
    }
    def settings(sett: List[String]) = sett.map { _.split("=") match {
      case Array(key, value) => key.trim -> value.trim
      case x => sys.error(s"Setting must be in format <key>=<value> with non empty keys and values. Instead found: $x")
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

    def selectResultClassTree(exp: QueryCompiler.SelectDef, c: Context) = {
      import QueryCompiler._
      import c.universe._
      lazy val generator: PartialFunction[(c.Tree, Exp), c.Tree] = extractorAndTraverser {
        case (tree, sd: SelectDef) =>
          val typeName = c.freshName("Tresql")
          val fields_convs = sd.cols.zipWithIndex.map { case (c: ColDef[_], idx: Int) =>
            (q"var ${TermName(c.name)}: ${TypeName(c.typ.toString)} = null",
             q"obj.${TermName(c.name)} = row.typed[${TypeName(c.typ.toString)}]($idx)")
          }
          (q"class A", false)
      }
    }
  }

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
