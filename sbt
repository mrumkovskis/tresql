SCRIPT_DIR="/home/..."
JAVA_OPTS="-Duniso.query.jdbcname=jdbc/uniso/query -Duniso.query.driverclassname=org.postgresql.Driver -Duniso.query.url=jdbc:postgresql://.../... -Duniso.query.user=... -Duniso.query.password=..."

java $JAVA_OPTS -Xmx512M -Xss2M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar $SCRIPT_DIR"/sbt-launcher.jar" "$@"