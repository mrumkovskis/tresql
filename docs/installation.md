Compiling and installing
==============================

To configure db connection, compile and run with Jetty application server:  

1. Add your jdbc driver jar to `service/lib`
2. Configure db connection by setting JAVA_OPTS environment variable:  
   `JAVA_OPTS=-Djdbc.drivers=... -Duniso.query.driver=... -Duniso.query.db=... -Duniso.query.user=... -Duniso.query.password=... -Duniso.query.schema=...`   
   For example:   
   `set JAVA_OPTS=-Duniso.query.driver=org.postgresql.Driver  -Duniso.query.db=jdbc:postgresql://192.168.1.1/mydb -Duniso.query.user=dbuser -Duniso.query.password=dbuserpass -Duniso.query.schema=public`  
3. Execute `sbt update ~jetty-run`. This downloads Scala and all necessary packages from maven repositories, compiles source and runs application on Jetty server on default port 8080.  
4. Browse to http://localhost:8080.  

Notes:  

* For get and post examples, browse to http://localhost:8080/get and http://localhost:8080/post.  
* Named query parameters are not supported by examples, but are supported by query server API.  
* For more info on gets and posts, see scaladoc for QueryServer. To create scaladoc, execute `sbt doc`.  
* To create core jar and service war, execute `sbt package`. To just compile, use `sbt compile`
* Compiled and generated files (jars, wars, classes, docs) are found in core/target and service/target.  

TreSQL can also be configured to run on Tomcat or other appserver. Just compile TreSQL using `sbt package` and copy to create web application on Tomcat.   
Database connection can be specified using standard JNDI resource locator. The default resource name should be `jdbc/uniso/query`. In this case, database schema "public" is assumed. Databse connection can also be configured using JAVA_OPTS variable as described above.


