package org.tresql

/**
 *  tresql string interpolator.
 *  NOTE: If variable reference in tresql string ends with ?
 *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
 *  passed to Query.
 */
import scala.quoted.*
import reflect.Selectable.reflectiveSelectable
import org.tresql.*
import org.tresql.CoreTypes.{RowConverter, ResultConverter}
import org.tresql.ast.CompilerAst.*
import org.tresql.ast.CompilerException

import scala.collection.immutable.ListMap
import java.util.Properties

class Record(data: ListMap[String, Any], source: Result[_]) extends RowLike with Selectable:
  def selectDynamic(name: String): Any = data(name)
  def apply(idx: Int): Any = data.slice(idx, idx + 1).head._2
  def apply(name: String): Any = selectDynamic(name)
  def column(idx: Int): Column = columns(idx)
  def columnCount: Int = data.size
  def values: Seq[Any] = data.values.toSeq
  def typed[T:Manifest](name: String): T = apply(name).asInstanceOf[T]
  val columns: Seq[Column] = data.map((n, _) => Column(-1, n, null)).toList
  override def close: Unit = source.close

private sealed trait Ex
private case class ColEx(col: String, typ: String | Ex, idx: Int) extends Ex
private case class QueryEx(cols: List[ColEx], pos: List[Int], isArr: Boolean) extends Ex
private case object DMLEx extends Ex
private case class PrimitiveEx(typ: String) extends Ex

private val SimpleAliasRegex = """"(?U)(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)"""".r

private def tresqlMacro(tresql: quoted.Expr[StringContext])(
  pars: quoted.Expr[Seq[Any]])(resources: quoted.Expr[Resources])(using Quotes) =
  import quotes.reflect.*

  def initCompiler =
    val macroPropertiesResourceName = "tresql-scala-macro.properties"
    val verboseProp = "tresql.scala.macro.verbose"
    val MetadataFactoryProp = "metadata_factory_class"
    def settings: (Map[String, String], Boolean) =
      val p = new Properties()
      val macroPropertiesStream = classOf[Record].getClassLoader.getResourceAsStream(macroPropertiesResourceName)
      if (macroPropertiesStream == null)
        sys.error(s"Macro properties resource not found: $macroPropertiesResourceName")
      p.load(macroPropertiesStream)
      import scala.jdk.CollectionConverters.*
      val (settings, verbose) = (p.asScala.toMap, System.getProperties.containsKey(verboseProp))
      if (verbose) println(s"Scala compiler macro settings:\n$settings")
      (settings, verbose)

    def metadata(conf: Map[String, String], verbose: Boolean) =
      conf.get(MetadataFactoryProp).map { factory =>
        compiling.MetadataCache.create(
          conf.filterNot(_._1 == MetadataFactoryProp),
          Class.forName(factory).getDeclaredConstructor().newInstance()
            .asInstanceOf[compiling.CompilerMetadataFactory],
          verbose
        )
      }.getOrElse(
        sys.error(s"Tresql interpolator not available. Scala macro compiler property missing - " +
          s"'$MetadataFactoryProp'. See if resource $macroPropertiesResourceName is available in classpath.")
      )
    val (macroSettings, verbose) = settings
    if (verbose) report.info(s"Macro compiler settings:\n$macroSettings")
    val compilerMetadata = metadata(macroSettings, verbose)
    ( new QueryCompiler(
      compilerMetadata.metadata, compilerMetadata.extraMetadata,
      new MacroResourcesImpl(compilerMetadata.macros, compilerMetadata.metadata)),
      verbose
    )

  val (compiler, verbose) = initCompiler

  def info(msg: => String) = if (verbose) println(msg)

  val parts = tresql.valueOrAbort.parts.map(StringContext.processEscapes)
  val tresqlString =
    parts.head + parts.tail.zipWithIndex.map {case (s, i) => s":_$i$s" }.mkString
  info(s"Compiling: $tresqlString")

  type ColConv      = quoted.Expr[RowConverter[Any]]
  type ResultConv   = quoted.Expr[(List[Int], ResultConverter[_])]

  sealed trait Res { def typ: TypeRepr }
  sealed trait RowRes extends Res { def convs: List[ResultConv] }
  case class ColRes(name: String, typ: TypeRepr, conv: ColConv, nestedResultConvs: List[ResultConv]) extends Res
  case class QueryRes(typ: TypeRepr, convs: List[ResultConv]) extends RowRes
  case class PrimitiveRes(typ: TypeRepr, conv: quoted.Expr[Any => Any]) extends Res
  case class DMLRes(typ: TypeRepr) extends Res

  def typeRepr(tn: String) = compiler.metadata.to_scala_type(tn) match
    case "Any" => TypeRepr.of[Any]
    case "Unit" => TypeRepr.of[Unit]
    case "Array[Byte]" => TypeRepr.of[Array[Byte]]
    case mf => TypeRepr.typeConstructorOf(Class.forName(mf))

  def resultConv(typeName: String) = '{(result: Any) => result match
    case r: Result[_] =>  r.headValue(${ quoted.Expr(compiler.metadata.to_scala_type(typeName)) })
    case _ => result
  }

  def res(md: Ex): Res = md match
    case q: QueryEx => if q.isArr then arrRes(q) else rowRes(q)
    case c: ColEx => colRes(c)
    case PrimitiveEx(tn) => PrimitiveRes(typeRepr(tn), resultConv(tn))
    case DMLEx => DMLRes(TypeRepr.of[DMLResult])
    case null => QueryRes(TypeRepr.of[Result[RowLike]], Nil)

  def colRes(col: ColEx): ColRes =
    def colConv(i: Int, tn: String) =
      val scalaType = compiler.metadata.to_scala_type(tn)
      '{ (row: RowLike) => row.typed(${ quoted.Expr(i) }, ${ quoted.Expr(scalaType) }) }

    val ColEx(colName, colType, idx) = col
    colType match
      case tn: String => ColRes(colName, typeRepr(tn), colConv(idx, tn), Nil)
      case PrimitiveEx(tn: String) =>
        val conv = '{ ${ colConv(idx, "Any") }.andThen(${ resultConv(tn) }) }
        ColRes(colName, typeRepr(tn), conv, Nil)
      case md: Ex => res(md) match
        case rr: RowRes => ColRes(colName, rr.typ, colConv(idx, rr.typ.typeSymbol.name), rr.convs)
        case DMLRes(typ) => ColRes(colName, typ, colConv(idx, typ.typeSymbol.name), Nil)
        case x => report.errorAndAbort(s"Unexpected type: $x")

  def rowRes(query: QueryEx): QueryRes =
    val (qt, crs) =
      (query.cols.foldLeft((TypeRepr.of[Record], List[ColRes]())):
        case ((rt, rc), col) =>
          val cr = colRes(col)
          (Refinement(rt, cr.name, cr.typ), cr :: rc)) match
        case (rt, rc) =>
          rt.asType match { case '[t] => (TypeRepr.of[Result[t & RowLike]].simplified, rc) }
    val conv: ResultConv = '{
      ( ${ quoted.Expr(query.pos) },
        (result: Result[RowLike]) => new CompiledResult(
          result,
          (row: RowLike) => new Record(ListMap[String, Any](${
            Varargs(crs.reverse.map { cr => '{ ${ quoted.Expr(cr.name) } -> ${ cr.conv } (row) } })
          }: _*), result)
        )
      )
    }
    val convs = crs.foldLeft(List(conv))(_ ::: _.nestedResultConvs)
    QueryRes(qt, convs)

  def arrRes(arr: QueryEx): QueryRes =
    def rowTypeAndConv(cols: List[ColEx]): (TypeRepr, ColConv, List[ResultConv]) =
      val (at, crs) =
        (cols.reverse.foldLeft((TypeRepr.of[EmptyTuple], List[ColRes]())):
          case ((rt, rc), col) =>
            val cr = colRes(col)
            val resType = cr.typ.asType match
              case '[ct] => rt.asType match
                case '[bt] => TypeRepr.of[*:[ct, bt & Tuple]]
            (resType, cr :: rc)) match
          // unwrap single element tuple type
          case (rt, cs@List(_)) => rt.asType match { case '[*:[t, EmptyTuple.type]] => (TypeRepr.of[t], cs) }
          case x => x
      val conv = '{ (row: RowLike) => ${
          if crs.size == 1 then '{ ${ crs.head.conv } (row) }
          else crs.foldLeft[quoted.Expr[Tuple]](quoted.Expr(EmptyTuple)):
            (res, cr) => '{ $res :* ${ cr.conv } (row) }
        }
      }
      (at, conv, crs.foldLeft(List[ResultConv]())(_ ::: _.nestedResultConvs))
    arr match
      case QueryEx(List(ColEx(_, q@QueryEx(_, _, false), _)), _, true) =>
        val (qt, rowConv, nestedConvs) = rowTypeAndConv(q.cols)
        val qconv: ResultConv = '{(${ quoted.Expr(q.pos)}, _.map($rowConv(_)))}
        val conv: ResultConv = '{(${ quoted.Expr(arr.pos) }, _(0))}
        val typ = qt.asType match { case '[t] => TypeRepr.of[Iterator[t]] }
        QueryRes(typ, conv :: qconv :: nestedConvs)
      case _ =>
        val (at, rowConv, nestedConvs) = rowTypeAndConv(arr.cols)
        val resConv = '{( ${quoted.Expr(arr.pos)}, $rowConv )}
        QueryRes(at.simplified, resConv :: nestedConvs)

  def normalizedName(name: String) = if (name.startsWith("\""))
    SimpleAliasRegex.unapplySeq(name).map(_.head).getOrElse(name) else name
  case class Ctx(ex: Ex, path: List[Int], colIdx: Int, childIdx: Int)
  lazy val exGenerator: compiler.Traverser[Ctx] = compiler.traverser(ctx => {
    case _: DMLDefBase => ctx.copy(ex = DMLEx)
    case PrimitiveDef(_, ExprType(tn)) => ctx.copy(PrimitiveEx(tn))
    case rd: RowDefBase =>
      val (_, exs) = rd.cols.foldLeft(ctx -> List[ColEx]()):
        case ((rctx, rcols), col) =>
          val cctx = exGenerator(rctx)(col)
          (rctx.copy(colIdx = rctx.colIdx + 1, childIdx = cctx.childIdx),
            cctx.ex.asInstanceOf[ColEx] :: rcols)
      val cols = exs.reverse
      val ex = QueryEx(cols, ctx.path, rd.isInstanceOf[ArrayDef])
      ctx.copy(ex = ex)
    case ColDef(name, exp, ExprType(tn)) =>
      val (nctx, ex) = exp match
        case c: ChildDef =>
          (ctx.copy(childIdx = ctx.childIdx + 1), exGenerator(ctx)(exp).ex)
        case p: PrimitiveDef =>
          (ctx, exGenerator(ctx)(exp).ex)
        case _ => (ctx, tn)
      nctx.copy(ex = ColEx(normalizedName(name), ex, ctx.colIdx))
    case ChildDef(exp, _) =>
      val nctx = ctx.copy(path = ctx.childIdx :: ctx.path, colIdx = 0, childIdx = 0)
      ctx.copy(ex = exGenerator(nctx)(exp).ex)
  })

  val compiledExp = try compiler.compile(tresqlString) catch
    case ex: CompilerException => report.errorAndAbort(ex.getMessage)
  val exp = exGenerator(Ctx(null, List(0), 0, 0))(compiledExp).ex
  val resMd = res(exp)
  val queryExpr = resMd match
    case res: RowRes => '{
      new Query {
        override private[tresql] def converters =
          Map[List[Int], ResultConverter[_]](${ Varargs(res.convs) }: _*)
      }
    }
    case _ => '{Query}
  val queryResExpr = '{
    var optionalVars = Set[Int]()
    val queryString =
      StringContext.processEscapes(${ quoted.Expr(parts.head) }) +
        List[String](${ Varargs(parts.tail.map(quoted.Expr(_))) }: _*)
          .map(StringContext.processEscapes)
          .zipWithIndex.map { case (part, idx) =>
            if part.trim.startsWith("?") then optionalVars += idx
            ":_" + idx + part
          }.mkString
    val queryParams = List[Any]($pars:_*)
      .zipWithIndex
      .filterNot { case (param, idx) => param == null && (optionalVars contains idx) }
      .map { case (param, idx) => ("_" + idx) -> param }.toMap
    $queryExpr.compiledResult(queryString, queryParams)($resources)
  }
  val resExpr =
    resMd match
      case PrimitiveRes(typ, conv) => typ.asType match { case '[t] => '{$conv($queryResExpr).asInstanceOf[t]} }
      case _ => resMd.typ.asType match { case '[t] => '{$queryResExpr.asInstanceOf[t]} }

  info("------ Generated code ---------")
  info(resExpr.asTerm.show(using Printer.TreeShortCode))
  info("-------------------------------")
  resExpr

extension(inline sc: StringContext)
  transparent inline def tresql(pars: Any*)(using resources: Resources) =
    ${tresqlMacro('sc)('pars)('resources)}

export CoreTypes.*

given Conversion[java.sql.ResultSet, Result[RowLike]] with
  def apply(jdbcResult: java.sql.ResultSet): Result[RowLike] =
    val md = jdbcResult.getMetaData
    new DynamicSelectResult(jdbcResult,
      Vector((1 to md.getColumnCount map { i => Column(i, md.getColumnLabel(i), null) }): _*),
      new Env(Map[String, Any](), Resources(), false), "<not available>", Nil
    )
