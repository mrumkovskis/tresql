package org.tresql.compiling

import org.tresql.{MetaData, Env}
import org.tresql.metadata.{Procedure, Par}

import scala.reflect.{Manifest, ManifestFactory}
import java.lang.reflect._

trait CompilerFunctions extends org.tresql.MetaData {

  def compilerFunctions: Class[_]

  private val procedures: Map[String, Procedure[_]] = compilerFunctions
    .getMethods.map { method => method.getName -> proc_from_meth(method) }
    .toList.asInstanceOf[List[(String, Procedure[_])]].toMap //strange cast is needed???

  private def proc_from_meth(m: Method): Procedure[_] = {
    var repeatedPars = false
    val pars = m.getGenericParameterTypes.map {
      case par: ParameterizedType =>
        //consider parameterized type as a Seq[T] of repeated args
        //isVarArgs method of java reflection api does not work on scala repeated args
        repeatedPars = true
        par.getActualTypeArguments match {
          case scala.Array(c: Class[_]) => ManifestFactory.classType(c)
          case scala.Array(x) => ManifestFactory.singleType(x)
          case x => sys.error(s"Multiple type parameters not supported! Method: $m, parameter: $par")
        }
      case c: Class[_] => ManifestFactory.classType(c)
      case x => ManifestFactory.singleType(x)
    }.zipWithIndex.map { case (m, i) =>
      Par[Nothing](s"_${i + 1}", null, -1, -1, null, m)
    }.toList
    val returnType = m.getGenericReturnType match {
      case par: ParameterizedType => sys.error(s"Parametrized return type not supported! Method: $m, parameter: $par")
      case c: Class[_] => ManifestFactory.classType(c)
      case x => ManifestFactory.singleType(x)
    }
    Procedure[Nothing](m.getName, null, -1, pars, -1, null, returnType, repeatedPars)
  }

  override def procedure(name: String): Procedure[_] =
    procedureOption(name).getOrElse(sys.error(s"Procedure not found: $name"))
  override def procedureOption(name: String): Option[Procedure[_]] =
    procedures.get(name) orElse super.procedureOption(name)
}
