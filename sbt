#!/bin/sh
SCRIPT_DIR="$( cd -P "$( dirname "$0" )" && pwd )"

JAVA_OPTS=
if [ -r "$SCRIPT_DIR"/setenv ]; then
  . "$SCRIPT_DIR"/setenv
fi
java $JAVA_OPTS -Xmx512M -Xss2M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar $SCRIPT_DIR"/sbt-launcher.jar" "$@"
