package org.tresql

object QueryCompiler extends compiling.Compiler {
  def compile(exp: String): parsing.Exp = compile(parseExp(exp))
}
