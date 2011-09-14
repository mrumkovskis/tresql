SCRIPT_DIR="/home/janis/workspace/Query"
JAVA_OPTS="-Duniso.query.jdbcname=jdbc/uniso/query -Duniso.query.driverclassname=org.postgresql.Driver -Duniso.query.url=jdbc:postgresql://192.168.1.77/db112 -Duniso.query.user=db112 -Duniso.query.password=db112"

java $JAVA_OPTS -Xmx512M -Xss2M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar $SCRIPT_DIR"/sbt-launcher.jar" "$@"