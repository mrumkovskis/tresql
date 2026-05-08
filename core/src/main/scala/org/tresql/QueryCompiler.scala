package org.tresql

class QueryCompiler(override val metadata: Metadata,
                    override val extraMetadata: Map[String, Metadata],
                    macros: MacroResources, cache: Cache)
  extends QueryParser(macros, null) with compiling.Compiler {
    def this(
      metadata: Metadata,
      extraMetadata: Map[String, Metadata],
      macros: MacroResources,
    ) = this(metadata, extraMetadata, macros, null)
  def compile(exp: String): ast.Exp = compile(parseExp(exp))
}
