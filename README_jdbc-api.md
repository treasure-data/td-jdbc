
jdbc-api-4.1.jar is necessary to build td-jdbc using an older version (4.1) of JDBC API.

## How to build

Mac OS X

- Install jdk6 https://support.apple.com/kb/DL1572?locale=en_US
- 
```
$ jar xvf jar xvf /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/bundle/Classes/classes.jar java/sql javax/sql
$ jar cvf jdbc-api-4.1.jar java javax
$ mvn deploy:deploy-file -Durl=file://(path to td-jdbc folder)/mvn-local -Dfile=jdbc-api.jar -DgroupId=com.treasuredata.thirdparty -DartifactId=jdbc-api -Dpackaging=jar -Dversion=4.1
``
