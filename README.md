# Akka Spark Pipeline

## Description

Akka Spark Pipeline is an example project that lets you find out how frequently a specific technology is used with different technology stacks.

Akka Spark Pipeline uses Akka, Spark GraphX, MongoDB, and Neo4j to handle and analyze thousands of projects published on GitHub (read: big data) to build a graph with relations between various technologies. Each relation shows the number of projects where two related technologies are used. 

It's possible to use the graph for further analysis and to obtain statistical data.

### How it works

This example project uses the GitHub client to grab the data about repositories, in particular, project metadata and the list of project dependencies. This list of dependencies is then stored in MongoDB. 

Once the projects' data is downloaded and stored in the database, Spark gets it and builds a graph that reflects the relationships between technologies. 

The created graph is then stored in the Neo4j graph database. Using an HTTP server, you can query the database with a specific technology to see the list technologies it's predominantly used with.

## Technologies

| Technology     | Description                       | Project use                                |
| -------------- | --------------------------------- | ------------------------------------------ |
| [Akka Streams] | Compose data transformation flows | Retrieve repositories metadata from GitHub |
| [Spark GraphX] | Spark component for graphs and graph-parallel computations | Build a graph from projects dependencies |
| [MongoDB]      | A document-oriented database      | Used to store raw data                     |
| [Neo4j]        | A Graph database                  | Used to store the built graphs             |

## Branches

| Branch         | Description                                                      |
| -------------- | ---------------------------------------------------------------- |
| [master]       | The version with the latest features. May not work consistently  |
| [spark-graphx] | Version with the Spark GraphX functionality. Not fully completed |

## Project structure

```
akka-spark-kafka-pipeline
├── models                                    # Contains models that define the GitHub project entity
├── modules                                   # Contains Guice bindings
├── repositories                              # Contains classes to work with the database layer
│   └── github                                # Contains the repository GitHub project entity
├── services                                  # Services to work with different technologies such as Spark or Kafka
│   ├── github                               
│   │   ├── client                            # Contains GitHub client functionality
│   │   └── spark                              
│   │       └── GitHubGraphXService.scala     # The service to create a graph from project dependencies using Spark GraphX
│   ├── kafka                                  
│   │   └── KafkaService.scala                # The service to interact with Kafka
│   └── spark                                  
│       └── SparkMongoService.scala           # Contains a connector between Spark and MongoDB
└── utils                                     # Contains application utils such as a logger
```

## How to start

Before starting the application, you must have MongoDB running on your computer. Also you must set 
personal GitHub token into either `GitHubOAuthToken` env variable (recommended) or 'services/github/GitHubRequestComposer.scala' class (as default value in `private val token = sys.env.getOrElse("GitHubOAuthToken", "")` string).

Run the application:

```bash
sbt run
```

## Contributors

If you have any suggestions or contributions, please contribute.

## License

Copyright &copy; 2019 [SysGears INC]. This source code is licensed under the [MIT] license.

[akka streams]: https://doc.akka.io/docs/akka/2.5/stream/
[spark graphx]: https://spark.apache.org/graphx/ 
[mongodb]: https://www.mongodb.com/
[neo4j]: https://neo4j.com/
[master]: https://github.com/sysgears/akka-spark-pipeline/tree/master
[spark-graphx]: https://github.com/sysgears/akka-spark-pipeline/tree/spark-graphx