
jdbc-api-4.1.jar is necessary to build td-jdbc using an older version (4.1) of JDBC API.

## How to build

Mac OS X

- Install jdk6
- 
```
$ jar cvf /Library/Java/JavaVirtualMachines/1.6.0_65-b14-462.jdk/Contents/Home/bundle/Classes/classes.jar java/sql javax/sql
$ jar cvf jdbc-api-4.1.jar java javax
$ mvn deploy:deploy-file -Durl=file:///(full path to td-jdbc folder)/mvn-local -Dfile=jdbc-api-4.1.jar -DgroupId=com.treasure_data.third_party -DartifactId=jdbc-api -Dpackaging=jar -Dversion=4.1
```

