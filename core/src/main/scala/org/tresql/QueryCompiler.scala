package org.tresql

object QueryCompiler extends compiling.Compiler {
  def compile(exp: String): Exp = compile(parseExp(exp).asInstanceOf[Exp])
}
