# Akka-Spark-pipeline

## Description

This pipeline uses Akka and Spark framework is used to build a graph, for further analysis 
and to obtain statistical data. For our purpose, we use GitHub repositories as a dataset.

##Technologies

| Technology     | General using                                                | Specific implementation                    |
| ---------------| -------------------------------------------------------------|--------------------------------------------|
| [Akka Streams] | Compose data transformation flows                            | Retrieve repositories metadata from GitHub |                          
| [Spark GraphX] | Component in Spark for graphs and graph-parallel computation | Build a graph from projects dependencies   |
| [MongoDB]      | Document-oriented database                                   | It is used for storing raw data            |
| [Neo4j]        | Graph database                                               | It is used for storing built graph         |

##Branches

| Branch         | Description                                                     |
| ---------------| ----------------------------------------------------------------|
| [master]       | The version with the latest features. May not work consistently |                          
| [spark-graphX] | Version with Spark GraphX functionality. Not fully completed    |

##Project structure

```
akka-spark-kafka-pipeline
├── models                                         # Contains models related to GitHub
├── modules                                        # Contains Guice bindings
├── repositories                                   # Contains classes to work with a database
│   └── github                                     # GitHub specific repository
├── services                                       # Services to work with different teechnologies such as Spark, Kafka
│   ├── github                                     # 
│   │   ├── client                                 # Contains GitHub client functionality
│   │   └── spark                                  # 
│   │       └── GitHubGraphXService.scala          # Service to create graph from project dependencies using Spark GraphX
│   ├── kafka                                      # 
│   │   └── KafkaService.scala                     # Service for interactions with Kafka
│   └── spark                                      # 
│       └── SparkMongoService.scala                # Contains a connector between Spark and MongoDB
└── utils                                          # Contains application utils
```

##How to start
Before starting the application, you must have MongoDB running on your computer.

Run the application:

`sbt run`

## Contributors

## License

Copyright &copy; 2016, 2017 [SysGears INC]. This source code is licensed under the [MIT] license.
