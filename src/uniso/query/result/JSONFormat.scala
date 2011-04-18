package scala.util.parsing.json

/**
 * This object defines functions that are used when converting JSONType
 * values into String representations. Mostly this is concerned with
 * proper quoting of strings.
 * 
 * @author Derek Chen-Becker <"java"+@+"chen-becker"+"."+"org">
 */
object JSONFormat {

  /**
   * This function can be used to properly quote Strings
   * for JSON output.
   */
  def quoteString (s : String) : String =
    s.map {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '/'  => "\\/" 
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      /* We'll unicode escape any control characters. These include:
       * 0x0 -> 0x1f  : ASCII Control (C0 Control Codes)
       * 0x7f         : ASCII DELETE
       * 0x80 -> 0x9f : C1 Control Codes
       *
       * Per RFC4627, section 2.5, we're not technically required to
       * encode the C1 codes, but we do to be safe.
       */
      case c if ((c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f')) => "\\u%04x".format(c: Int)
      case c => c
    }.mkString
}
