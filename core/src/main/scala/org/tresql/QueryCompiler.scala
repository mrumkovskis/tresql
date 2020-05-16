package org.tresql

object QueryCompiler extends compiling.Compiler {
  def compile(exp: String)(implicit resources: Resources): parsing.Exp = compile(parseExp(exp))
}
