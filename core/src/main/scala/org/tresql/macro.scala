package org.tresql

package object macro_ {
  implicit class TresqlMacroInterpolator(val sc: StringContext) extends AnyVal {
    def macro_(args: Expr*)(implicit b: QueryBuilder): Expr = {
      var usedNames: Set[String] = Set()
      val zipped = args.zipWithIndex.map(kv => s"_arg_${kv._2}_" -> kv._1)
      val params = zipped.toMap
      val expr = sc.standardInterpolator(identity, zipped.map(":" + _._1))
      b.transform(b.buildExpr(expr), {
        case v @ b.VarExpr(name, _, _, _) =>
          if (usedNames(name)) sys.error(
            s"Unable to expand macro because of reserved variable name clash: $name")
          usedNames += name
          val resolved = params.get(name) getOrElse v
          resolved
      })
    }
  }
}
