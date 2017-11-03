# spark Test01

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