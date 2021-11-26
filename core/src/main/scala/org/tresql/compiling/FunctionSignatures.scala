package org.tresql.compiling

trait DBAggregateFunctionSignatures {
  //aggregate functions
  def count(col: Any): java.lang.Long
  def max[T](col: T): T
  def min[T](col: T): T
  def sum[T](col: T): T
  def avg[T](col: T): T
}

trait TresqlMacroFunctionSignatures {
  //macros
  def if_defined[T](variable: Any, exp: T): T
  def if_defined_or_else[T](variable: Any, exp1: T, exp2: T): T
  def if_missing[T](variable: Any, exp: T): T
  def if_any_defined(exp: Any*): Any
  def if_all_defined(exp: Any*): Any
  def if_any_missing(exp: Any*): Any
  def if_all_missing(exp: Any*): Any
  def sql_concat(exprs: Any*): Any
  def sql(expr: Any): Any
  def _lookup_edit(objName: String, idName: String, insertExpr: Any, updateExpr: Any): Any
  def _update_or_insert(table: Any, updateExpr: Any, insertExpr: Any): Any
  def _upsert(updateExpr: Any, insertExpr: Any): Any
  def _delete_missing_children(objName: String, key: Any, keyValExprs: Any, deleteExpr: Any): Any
  def _not_delete_keys(key: Any, keyValExprs: Any): Any
  def _id_ref_id(idRef: Any, id: Any): Any
  def _id_by_key(idExpr: Any): Any
  def _update_by_key(table: String, setIdExpr: Any, updateExpr: Any): Any
}

trait BasicDBFunctionSignatures {
  def coalesce[T](pars: T*): T
  def upper(string: String): String
  def lower(string: String): String
  def insert (str1: String, offset: Int, length: Int, str2: String): String
  def to_date(date: String, format: String): java.sql.Date
  def trim(string: String): String
  def exists(cond: Compiler#SelectDefBase): java.lang.Boolean
  def group_concat(what: Any): String
  //postgres group_concat replacement is string_agg
  def string_agg(expr: Any*): Any
  def current_date(): java.sql.Date
  def current_time(): java.sql.Timestamp
  def now(): java.sql.Timestamp
}

trait BasicDialectFunctionSignatures {
  //dialect
  def `case`[T](when: Any, `then`: T, rest: Any*): T
  def nextval(seq: String): java.lang.Long
  def cast(exp: Any, typ: String): Any
}

trait TresqlFunctionSignatures
  extends DBAggregateFunctionSignatures
  with TresqlMacroFunctionSignatures
  with BasicDBFunctionSignatures
  with BasicDialectFunctionSignatures
