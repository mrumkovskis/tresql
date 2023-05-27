package org.tresql.resources

import org.tresql.ast.CompilerAst.ExprType
import org.tresql.{Expr, QueryBuilder}
import org.tresql.metadata.{FixedReturnType, Par, ParameterReturnType, Procedure, ReturnType, TypeMapper}
import org.tresql.ast.{Cast, Exp, Fun, Ident, Obj}
import org.tresql.parsing.QueryParsers

import java.io.InputStream
import java.lang.reflect.{Method, ParameterizedType}
import scala.io.BufferedSource
import scala.reflect.ManifestFactory
import scala.util.Try
import scala.util.matching.Regex

case class FunctionSignatures(signatures: Map[String, List[Procedure]]) {
  def merge(fs: FunctionSignatures): FunctionSignatures = {
    FunctionSignatures(
      ResourceMerger.mergeResources(signatures, fs.signatures)
        .map { case (n, l) => (n, l.toList) }
    )
  }
}
object FunctionSignatures {
  def empty = FunctionSignatures(Map())
}

class FunctionSignaturesLoader(typeMapper: TypeMapper) extends ResourceLoader {
  protected val ResourceFile        = "/tresql-function-signatures.txt"
  protected val DefaultResourceFile = "/tresql-default-function-signatures.txt"

  private val ParTypeDefRegex = """(\w*)(\*?)""".r
  private val RetTypeDefRegex = """(\$?)(\w+)""".r

  protected val qp: QueryParsers = new QueryParsers {
    override val reserved: Set[String] = Set()
  }

  private val primitiveClassAnyValMap: Map[Class[_], Manifest[_]] =
    Map(
      java.lang.Byte.TYPE -> ManifestFactory.Byte,
      java.lang.Short.TYPE  -> ManifestFactory.Short,
      java.lang.Character.TYPE  -> ManifestFactory.Char,
      java.lang.Integer.TYPE -> ManifestFactory.Int,
      java.lang.Long.TYPE -> ManifestFactory.Long,
      java.lang.Float.TYPE -> ManifestFactory.Float,
      java.lang.Double.TYPE -> ManifestFactory.Double,
      java.lang.Boolean.TYPE -> ManifestFactory.Boolean,
      java.lang.Void.TYPE -> ManifestFactory.Unit
    )

  protected def tryParseSignature(signature: String) = {
    var repeatedPars = false
    def parseParType(t: String) = {
      val ParTypeDefRegex(pt, isRepeated) = t
      (pt, isRepeated.nonEmpty)
    }
    def parseRetType(t: String) = {
      val RetTypeDefRegex(ref, pt) = t
      (ref.nonEmpty, pt)
    }
    def createPar[T](pn: String, pt: ExprType) = Par(pn, null, -1, -1, null, pt)
    def parse_params(params: List[Exp]) =
      params.map {
        case Obj(Ident(List(pn)), null, null, null, false) => createPar(pn, ExprType.Any)
        case Cast(Obj(Ident(List(pn)), null, null, null, false), type_) =>
          val (pt, isRepeated) = parseParType(type_)
          if (isRepeated) repeatedPars = true
          createPar(
            pn,
            if (pt.isEmpty || typeMapper == null) ExprType.Any
            else ExprType(typeMapper.xsd_scala_type_map(pt).toString)
          )
        case x => sys.error(s"Invalid function '$signature' paramater - '${x.tresql}'. " +
          s"Expected identifier or identifier with cast.")
      }
    def createFun(fn: String, pars: List[Par], rt: ReturnType, repPars: Boolean) =
      Procedure(fn, null, -1, pars, -1, null, rt, repPars)
    Try(qp.parseExp(signature))
      .map {
        case Fun(name, parameters, _, None, None) =>
          createFun(name, parse_params(parameters), FixedReturnType(ExprType.Any), repeatedPars)
        case Cast(Fun(name, parameters, _, None, None), type_) =>
          val pars = parse_params(parameters)
          val (isRef, pt) = parseRetType(type_)
          val rt =
            if (isRef) {
              val idx = pars.indexWhere(_.name == pt)
              assert(idx != -1, s"Invalid function $name return type '$type_'. Parameter $pt does not exist.")
              ParameterReturnType(idx)
            }
            else FixedReturnType(
              if (typeMapper!= null) ExprType(typeMapper.xsd_scala_type_map(pt).toString)
              else ExprType.Any
            )
          createFun(name, pars, rt, repeatedPars)
        case _ => sys.error(s"function signature must be function, instead found - '$signature'")
      }
  }

  protected def parseErr(signatureDef: String) =
    sys.error(s"Error in function signature definition '$signatureDef'. " +
      s"Format - <funname>(<param_name>[::type], ...)")

  def parseSignature(signatureDef: String): Procedure =
    tryParseSignature(signatureDef).get

  protected def trySignatureDef(signatureDef: String): Try[Boolean] = {
    Try(qp.parseExp(signatureDef))
      .map {
        case _: Fun | Cast(_: Fun, _) => true
        case _ => false /* maybe comment */
      }
      .recover { case _ => parseErr(signatureDef) }
  }

  def isSignatureDef(signatureDef: String): Boolean = {
    trySignatureDef(signatureDef).get
  }

  def parseSignature(m: Method): Procedure = {
    var repeatedPars = false
    // parameter types and return types are Any since this is considered
    // macro parsing where arguments are expressions. if macro function needs
    // parameter or return types during tresql compilation, put it into
    // function signatures
    val pars: List[Par] = m.getGenericParameterTypes.map {
      case par: ParameterizedType =>
        //consider parameterized type as a Seq[T] of repeated args
        //isVarArgs method of java reflection api does not work on scala repeated args
        repeatedPars = true
        par.getActualTypeArguments match {
          case scala.Array(_) => ExprType.Any
          case _ => sys.error(s"Multiple type parameters not supported! Method: $m, parameter: $par")
        }
      case _ => ExprType.Any
    }.zipWithIndex.map { case (et, i) =>
      Par(s"_$i", null, -1, -1, null, et)
    }.toList.drop(1) // drop builder or parser argument
    val returnType = m.getGenericReturnType match {
      case par: ParameterizedType => sys.error(s"Parametrized return type not supported! Method: $m, parameter: $par")
      case _: Class[_] => FixedReturnType(ExprType.Any)
      case x =>
        val idx  = pars.indexWhere(_.scalaType.toString == x.toString)
        if (idx == -1) FixedReturnType(ExprType.Any) else ParameterReturnType(idx)
    }
    Procedure(m.getName, null, -1, pars, -1, null, returnType, repeatedPars)
  }

  def loadFunctionSignatures(signatures: Seq[String]): FunctionSignatures = {
    val fs = signatures
      .collect { case s if isSignatureDef(s) => parseSignature(s) }
      .toList
      .groupBy(_.name)
    FunctionSignatures(fs)
  }

  def loadFunctionSignaturesFromClass(clazz: Class[_]): FunctionSignatures = {
    if (clazz == null) FunctionSignatures.empty
    else {
      val signatures =
        clazz.getMethods
          .collect { case m if m.getParameterCount > 0 => parseSignature(m) } // must have at least one par - parser or builder
          .toList
          .groupBy(_.name)
      FunctionSignatures(signatures)
    }
  }
}

trait TresqlMacro[A, B] {
  def invoke(env: A, params: IndexedSeq[_]): B
  def signature: Procedure
}

case class TresqlMacros(parserMacros: Map[String, Seq[TresqlMacro[QueryParsers, Exp]]],
                        builderMacros: Map[String, Seq[TresqlMacro[QueryBuilder, Expr]]],
                        builderDeferredMacros: Map[String, Seq[TresqlMacro[QueryBuilder, Expr]]]) {
  def merge(tresqlMacros: TresqlMacros): TresqlMacros = {
    import ResourceMerger._
    TresqlMacros(parserMacros = mergeResources(parserMacros, tresqlMacros.parserMacros),
      builderMacros = mergeResources(builderMacros, tresqlMacros.builderMacros),
      builderDeferredMacros = mergeResources(builderDeferredMacros, tresqlMacros.builderDeferredMacros))
  }
}
object TresqlMacros {
  def empty = TresqlMacros(Map(), Map(), Map())
}

class MacrosLoader(typeMapper: TypeMapper) extends FunctionSignaturesLoader(typeMapper) {
  override protected val ResourceFile        = "/tresql-macros.txt"
  override protected val DefaultResourceFile = "/tresql-default-macros.txt"

  private case class MacroBody(parts: Seq[MacroBodyPart], suffix: String)
  private case class MacroBodyPart(prefix: String, parIdx: Int)
  private class TresqlResourcesMacro(val signature: Procedure, body: MacroBody)
    extends TresqlMacro[QueryParsers, Exp] {
    private val sigParCount = signature.pars.size
    override def invoke(env: QueryParsers, params: IndexedSeq[_]): Exp = {
      val parCount = params.size
      def mayBeLiftToArr(idx: Int) = {
        if (signature.hasRepeatedPar && idx == sigParCount - 1 && sigParCount < parCount)
          params.slice(idx, parCount).map(_.asInstanceOf[Exp].tresql).mkString("[", ",", "]")
        else params(idx).asInstanceOf[Exp].tresql
      }
      val res =
        body.parts.foldLeft(new StringBuilder()) { (res, bp) =>
          res.append(bp.prefix).append(mayBeLiftToArr(bp.parIdx))
        }
        .append(body.suffix)
        .toString()
      env.parseExp(res)
    }
    override def toString() = s"Signature - $signature, body - $body"
  }
  private case class TresqlScalaMacro[A, B](signature: Procedure, method: Method, invocationTarget: Any)
    extends TresqlMacro[A, B] {
    override def invoke(env: A, params: IndexedSeq[_]): B = {
      val p = method.getParameterTypes
      try {
          val _args =
            (if (signature.hasRepeatedPar && params.nonEmpty && p.last.isAssignableFrom(classOf[Seq[_]])) {
              val idx = signature.pars.size - 1
              params.slice(0, idx)
                .:+(params.slice(idx, params.length))
            } else {
              params
            }).+:(env).asInstanceOf[Seq[Object]] //must cast for scala verion 2.12
          method.invoke(invocationTarget, _args: _*).asInstanceOf[B]
      } catch {
        case e: Exception =>
          def msg(e: Throwable): List[String] = {
            if (e == null) Nil
            else s"""${e.getClass}${if (e.getMessage != null) ": " + e.getMessage else ""}""" :: msg(e.getCause)
          }
          throw new RuntimeException(s"Error invoking macro function - ${signature.name} (${msg(e).mkString(" ")})", e)
      }
    }
  }

  override protected def parseErr(macroDef: String) =
    sys.error(s"Error in macro definition '$macroDef'. " +
      s"Macro def format - <macro function signature> = <macro body>")

  def parseMacro(macroDef: String): TresqlMacro[QueryParsers, Exp] = {
    def parseBody(body: String, params: Seq[String]) = {
      val pattern = params.sortBy(- _.length).map("\\$" + _).mkString("|")
      val regex = new Regex(pattern)
      if (pattern.nonEmpty) {
        val (suffixIdx, bodyPartsReversed) =
          regex.findAllMatchIn(body).foldLeft(0 -> List[MacroBodyPart]()) { case ((suffIdx, res), m) =>
            val par = m.matched.substring(1)
            val idx = params.indexWhere(_ == par)
            assert(idx != -1, sys.error(s"Unknown parameter reference '$par' in macro: ($body)"))
            m.end -> (MacroBodyPart(body.substring(suffIdx, m.start), idx) :: res)
          }
        MacroBody(bodyPartsReversed.reverse, body.substring(suffixIdx))
      }
      else MacroBody(Nil, body)
    }
    macroDef.split("=", 2) match {
      case Array(signature, body) =>
        tryParseSignature(signature)
          .map { sign =>
            new TresqlResourcesMacro(sign, parseBody(body, sign.pars.map(_.name)))
          }.get
      case _ => parseErr(macroDef)
    }
  }

  def isMacroDef(macroDef: String): Boolean = {
    val idx = macroDef.indexOf("=")
    if (idx == -1) isSignatureDef(macroDef) // maybe comment
    else {
      trySignatureDef(macroDef.substring(0, idx))
        .recoverWith { case _ => trySignatureDef(macroDef) /* maybe comment containing '=' sign */ }
        .get
    }
  }

  def loadTresqlMacros(macros: Seq[String]): TresqlMacros = {
    val parserMacros =
      macros.collect {
        case m if isMacroDef(m) => parseMacro(m)
      }.foldLeft(Map[String, Seq[TresqlMacro[QueryParsers, Exp]]]()) { (res, m) =>
        val n = m.signature.name
        res.get(n)
          .map { ml =>
            res + (n -> (ml :+ m))
          }.getOrElse(res + (n -> Seq(m)))
      }
    TresqlMacros(parserMacros = parserMacros, builderMacros = Map(), builderDeferredMacros = Map())
  }

  def loadTresqlScalaMacros(obj: Any): TresqlMacros = {
    def macroMethods(mobj: Any): TresqlMacros = mobj match {
      case null => TresqlMacros.empty
      case Some(o) => macroMethods(o)
      case None => macroMethods(null)
      case x =>
        def isMacro(m: java.lang.reflect.Method) =
          m.getParameterTypes.nonEmpty && (isParserMacro(m) || isBuilderMacro(m))
        def isParserMacro(m: java.lang.reflect.Method) =
          classOf[QueryParsers].isAssignableFrom(m.getParameterTypes()(0)) &&
            classOf[Exp].isAssignableFrom(m.getReturnType)
        def isBuilderMacro(m: java.lang.reflect.Method) =
          classOf[QueryBuilder].isAssignableFrom(m.getParameterTypes()(0)) &&
            classOf[Expr].isAssignableFrom(m.getReturnType)
        def hasAllExpPars(m: java.lang.reflect.Method) =
          m.getParameterTypes.size > 1 && m.getParameterTypes.tail.forall(p => classOf[Exp].isAssignableFrom(p))
        val macros = x.getClass.getMethods.collect {
          case m if isMacro(m) => m
        }.foldLeft(TresqlMacros.empty) { (res, m) =>
          def app[A, B](map: Map[String, Seq[TresqlMacro[A, B]]]) = {
            val sign = parseSignature(m)
            val macr = TresqlScalaMacro[A, B](sign, m, mobj)
            val n = sign.name
            map.get(n)
              .map { ml =>
                map + (n -> (ml :+ macr))
              }.getOrElse(map + (n -> Seq(macr)))
          }
          if (isBuilderMacro(m))
            if (hasAllExpPars(m)) res.copy(builderDeferredMacros = app(res.builderDeferredMacros))
            else res.copy(builderMacros = app(res.builderMacros))
          else res.copy(parserMacros = app(res.parserMacros))
        }
        if (macros.builderMacros.isEmpty && macros.parserMacros.isEmpty)
          sys.error(s"No macro methods found in object $mobj. " +
            s"If you do not want to use macros pass null as a parameter")
        macros
    }
    macroMethods(obj)
  }

  override def loadFunctionSignatures(macro_def: Seq[String]): FunctionSignatures = {
    val fs = macro_def
      .collect { case s if isMacroDef(s) => parseSignature(s.substring(0, s.indexOf("="))) }
      .toList
      .groupBy(_.name)
    FunctionSignatures(fs)
  }
}

trait ResourceLoader {
  private val SeparatorPattern = """\R+(?=[^\s]|$)"""
  private val IncludePattern   = """include\s+(.+)""".r
  protected def ResourceFile: String
  protected def DefaultResourceFile: String
  protected def getResourceAsStream(r: String): InputStream = getClass.getResourceAsStream(r)

  def load(res: String): Option[Seq[String]] = {
    def l(r: String)(loaded: Set[String]): Option[Seq[String]] = {
      if (loaded(r)) None
      else {
        val in = getResourceAsStream(r)
        if (in == null) {
          if (loaded.isEmpty) None
          else sys.error(s"Resource not found: $r (referenced from ${loaded mkString " or "})")
        }
        else {
          val res =
            new BufferedSource(in).mkString.split(SeparatorPattern).toIndexedSeq
              .flatMap { token =>
                if (IncludePattern.pattern.matcher(token).matches) {
                  val IncludePattern(nr) = token
                  l(nr)(loaded + r).getOrElse(Nil)
                } else {
                  List(token)
                }
              }
          Option(res)
        }
      }
    }
    l(res)(Set())
  }

  def load(): Seq[String] = {
    (load(ResourceFile) orElse load(DefaultResourceFile)).getOrElse(Nil)
  }
}

private object ResourceMerger {
  def mergeResources[A](m1: Map[String, Seq[A]], m2: Map[String, Seq[A]]): Map[String, Seq[A]] = {
    m1.foldLeft(m2) { case (res, (n, ml1)) =>
      res.get(n).map { ml2 =>
        res + (n -> (ml2 ++: ml1)) // put right resources in front of left
      }.getOrElse(res + (n -> ml1))
    }
  }
}
