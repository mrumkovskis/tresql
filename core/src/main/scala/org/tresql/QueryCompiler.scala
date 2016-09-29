package org.tresql

object QueryCompiler extends compiling.QueryTyper {
  def compile(exp: String): Exp = compile(parseExp(exp).asInstanceOf[Exp])
}
