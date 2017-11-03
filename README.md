# spark Test01

### environment
* Intellij
* JDK 1.8
* spark-2.2.0

### RESTful
* /wordcount
    * description : spark workcount sample application
    * method : POST
    * request JSON schema
```json
{
	"inputFile" : "/home/sparktest/README.txt",
	"outputDirPath" : "/home/sparktest/wordcount"
}
```

* /rdd
    * description : spark rdd transform sample
    * method : GET
    
### POSTMAN collection v2
* file : src/main/resources/postman/spark-Test01.postman_collection.json
