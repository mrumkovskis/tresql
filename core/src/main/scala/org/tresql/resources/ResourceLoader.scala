package org.tresql.resources

import scala.collection.immutable.ArraySeq
import scala.io.BufferedSource

object FunctionSignaturesLoader extends ResourceLoader {
  protected val ResourceFile        = "/tresql-function-signatures.txt"
  protected val DefaultResourceFile = "/tresql-default-functions-signatures.txt"
}

object MacrosLoader extends ResourceLoader {
  protected val ResourceFile        = "/tresql-macros.txt"
  protected val DefaultResourceFile = "/tresql-default-macros.txt"
}

trait ResourceLoader {
  private val SeparatorPattern = """\R+(?=[^\s])"""
  private val IncludePattern   = """include\s+(.+)""".r
  protected def ResourceFile: String
  protected def DefaultResourceFile: String

  def load(res: String): Seq[String] = {
    def l(r: String)(loaded: Set[String]): Seq[String] = {
      if (loaded(r)) Nil
      else {
        val in = getClass.getResourceAsStream(r)
        if (in == null) Nil
        else {
          ArraySeq.unsafeWrapArray(new BufferedSource(in).mkString.split(SeparatorPattern))
            .flatMap { token =>
              if (IncludePattern.matches(token)) {
                val IncludePattern(nr) = token
                l(nr)(loaded + r)
              } else {
                List(token)
              }
            }
        }
      }
    }
    l(res)(Set())
  }

  def load(): Seq[String] = {
    val macros = load(ResourceFile)
    if (macros.isEmpty) load(DefaultResourceFile) else macros
  }
}
