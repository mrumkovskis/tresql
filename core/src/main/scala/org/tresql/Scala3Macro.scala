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
import org.tresql.CoreTypes.RowConverter
import org.tresql.ast.CompilerAst.*
import org.tresql.ast.CompilerException

import scala.collection.immutable.ListMap
import java.util.Properties

class Record(data: ListMap[String, Any]) extends CompiledRow with Selectable:
  def selectDynamic(name: String): Any = data(name)
  def apply(idx: Int): Any = data.slice(idx, idx + 1).head._2
  def columnCount: Int = data.size
  val columns: Seq[Column] = data.map((n, _) => Column(-1, n, null)).toList

private def tresqlMacro(tresql: quoted.Expr[StringContext])(
  pars: quoted.Expr[Seq[Any]])(resources: quoted.Expr[Resources])(using Quotes) =
  import quotes.reflect.*

  def initCompiler =
    val macroPropertiesResourceName = "/tresql-scala-macro.properties"
    val verboseProp = "tresql.scala.macro.verbose"
    val MetadataFactoryProp = "metadata_factory_class"
    def settings: (Map[String, String], Boolean) =
      val p = new Properties()
      val macroPropertiesStream = classOf[Record].getResourceAsStream(macroPropertiesResourceName)
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

  case class ExPos(depth: Int, idx: Int)
  sealed trait Ex
  case class ColEx(col: String, typ: String | Ex, idx: Int) extends Ex
  case class QueryEx(cols: List[ColEx], pos: List[ExPos], isArr: Boolean) extends Ex
  case object DMLEx extends Ex
  case class PrimitiveEx(typ: String) extends Ex

  type ColConv      = quoted.Expr[RowConverter[Any]]
  type RowConv      = quoted.Expr[((Int, Int), RowConverter[RowLike])]
  type ResultConv   = quoted.Expr[Result[_] => Any]

  sealed trait Res { def typ: TypeRepr }
  sealed trait RowRes extends Res { def convs: List[RowConv] }
  case class ColRes(name: String, typ: TypeRepr, conv: ColConv, nestedRowConvs: List[RowConv]) extends Res
  case class QueryRes(typ: TypeRepr, convs: List[RowConv]) extends RowRes
  case class ArrRes(typ: TypeRepr, convs: List[RowConv], arrConv: ColConv) extends RowRes
  case class PrimitiveRes(primitiveTyp: TypeRepr, conv: ResultConv,
                          typ: TypeRepr = TypeRepr.of[Result[RowLike]]) extends Res
  case class DMLRes(typ: TypeRepr) extends Res

  def typeRepr(className: String) = className match
    case "Any" => TypeRepr.of[Any]
    case "Boolean" => TypeRepr.of[Boolean]
    case "Unit" => TypeRepr.of[Unit]
    case cn => TypeRepr.typeConstructorOf(Class.forName(cn))
  def resultConv(typeName: String) =
    '{ (result: Result[_]) => result.headValue(${ quoted.Expr(typeName) }) }

  def res(md: Ex): Res = md match
    case q: QueryEx => if q.isArr then arrRes(q) else rowRes(q)
    case c: ColEx => colRes(c)
    case PrimitiveEx(tn) => PrimitiveRes(typeRepr(tn), resultConv(tn))
    case DMLEx => DMLRes(TypeRepr.of[DMLResult])
    case null => QueryRes(TypeRepr.of[RowLike], Nil)

  def colRes(col: ColEx): ColRes =
    def conv(i: Int, tn: String) =
      '{ (row: RowLike) => row.typed(${ quoted.Expr(i) }, ${ quoted.Expr(tn) }) }

    val ColEx(colName, colType, idx) = col
    colType match
      case tn: String =>
        ColRes(colName, typeRepr(tn), conv(idx, tn), Nil)
      case PrimitiveEx(tn: String) =>
        val colConv = '{
          ${ conv(idx, "org.tresql.Result") }.asInstanceOf[RowConverter[Result[_]]]
            .andThen(${ resultConv(tn) })
        }
        ColRes(colName, typeRepr(tn), colConv, Nil)
      case md => res(md.asInstanceOf[Ex]) match
        case QueryRes(typ, convs) =>
          val ct = typ.asType match
            case '[t] => TypeRepr.of[Result[t & RowLike]]
          ColRes(colName, ct, conv(idx, ct.typeSymbol.name), convs)
        case ArrRes(typ, convs, colConv) =>
          val c =
            '{
              ${conv(idx, typ.typeSymbol.name)}.asInstanceOf[RowConverter[RowLike]]
                .andThen($colConv)
            }
          ColRes(colName, typ, c, convs)
        case DMLRes(typ) =>
          ColRes(colName, typ, conv(idx, typ.typeSymbol.name), Nil)
        case x => report.errorAndAbort(s"Unexpected type: $x")

  def rowRes(query: QueryEx): QueryRes =
    val (qt, crs) =
      query.cols.foldLeft((TypeRepr.of[Record], List[ColRes]())):
        case ((rt, rc), col) =>
          val cr = colRes(col)
          val resType = cr.typ.asType match
            case '[t] => Refinement(rt, cr.name, TypeRepr.of[t])
          (resType, cr :: rc)
    val ExPos(depth, idx) = query.pos.head
    val conv: RowConv = '{
      ( (${ quoted.Expr(depth) }, ${ quoted.Expr(idx) }),
        (row: RowLike) => new Record(ListMap[String, Any](
          ${
            Varargs(crs.reverse.map { cr => '{ ${ quoted.Expr(cr.name) } -> ${ cr.conv } (row) } })
          }: _*)
        )
      )
    }
    val convs = crs.foldLeft(List(conv))(_ ::: _.nestedRowConvs)
    QueryRes(qt, convs)

  def arrRes(arr: QueryEx): ArrRes =
    val (at, crs) =
      arr.cols.reverse.foldLeft((TypeRepr.of[EmptyTuple], List[ColRes]())):
        case ((rt, rc), col) =>
          val cr = colRes(col)
          val resType = cr.typ.asType match
            case '[t] => AppliedType(TypeRepr.of[*:[_, _]], List(TypeRepr.of[t], rt))
          (resType, cr :: rc)
    val ExPos(depth, idx) = arr.pos.head
    val conv: RowConv = '{
      ( (${ quoted.Expr(depth) }, ${ quoted.Expr(idx) }),
        identity[RowLike] _
      )
    }
    val convs = crs.foldLeft(List(conv))(_ ::: _.nestedRowConvs)
    val colConv = '{
      (row: RowLike) =>
        ${
          crs.foldLeft[quoted.Expr[Tuple]](quoted.Expr(EmptyTuple)):
            (res, cr) => '{ $res :* ${ cr.conv } (row) }
        }
    }
    ArrRes(at, convs, colConv)

  case class Ctx(ex: Ex, path: List[ExPos], colIdx: Int)
  lazy val exGenerator: compiler.Traverser[Ctx] = compiler.traverser(ctx => {
    case _: DMLDefBase => ctx.copy(ex = DMLEx)
    case PrimitiveDef(_, ExprType(tn)) => ctx.copy(PrimitiveEx(tn))
    case rd: RowDefBase =>
      val np = ExPos(ctx.path.head.depth, -1)
      val nctx = ctx.copy(path = np :: ctx.path)
      val (_, exs) = rd.cols.zipWithIndex.foldLeft(nctx -> List[ColEx]()):
        case ((rctx, rcols), (col, idx)) =>
          val c = exGenerator(rctx.copy(colIdx = idx))(col)
          (c, c.ex.asInstanceOf[ColEx] :: rcols)
      val cols = exs.reverse
      val ex = QueryEx(cols, ctx.path, rd.isInstanceOf[ArrayDef])
      ctx.copy(ex = ex)
    case ColDef(name, exp, ExprType(tn)) =>
      val (nctx, ex) = exp match
        case c: ChildDef =>
          val ExPos(depth, idx) = ctx.path.head
          val path = ExPos(depth, idx + 1) :: ctx.path.tail
          val cctx = ctx.copy(path = path)
          val ex = exGenerator(cctx)(c).ex
          (cctx, ex)
        case PrimitiveDef(_, ExprType(tn)) => (ctx, PrimitiveEx(tn))
        case _ => (ctx, tn)
      nctx.copy(ex = ColEx(name, ex, ctx.colIdx))
    case ChildDef(exp, _) =>
      val ExPos(depth, idx) = ctx.path.head
      val path = ExPos(depth + 1, idx) :: ctx.path.tail
      val nctx = ctx.copy(path = path)
      nctx.copy(ex = exGenerator(nctx)(exp).ex)
  })

  val compiledExp = try compiler.compile(tresqlString) catch
    case ex: CompilerException => report.errorAndAbort(ex.getMessage)
  val exp = exGenerator(Ctx(null, List(ExPos(0, 0)), -1))(compiledExp).ex
  val resMd = res(exp)
  val queryExpr = resMd match
    case row: RowRes => '{
      new Query {
        override def converters =
          Map[(Int, Int), RowConverter[RowLike]](${ Varargs(row.convs) }: _*)
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
    $queryExpr.compiledResult[RowLike](queryString, queryParams)($resources)
  }
  val resExpr =
    resMd match
      case PrimitiveRes(primitiveTyp, conv, _) => primitiveTyp.asType match
        case '[typ] => '{$conv($queryResExpr).asInstanceOf[typ]}
      case ArrRes(arrTyp, _, arrConv) => arrTyp.asType match
        case '[typ] => '{$arrConv($queryResExpr).asInstanceOf[typ]}
      case _ =>
        resMd.typ.asType match
          case '[typ] =>
            '{$queryResExpr.asInstanceOf[Result[typ & RowLike]]}

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
      new Env(Map[String, Any](), new Resources {}, false), "<not available>", Nil
    )
