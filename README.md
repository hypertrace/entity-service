# Entity Service
###### org.hypertrace.entity.service

[![CircleCI](https://circleci.com/gh/hypertrace/entity-service.svg?style=svg)](https://circleci.com/gh/hypertrace/entity-service)

Service that provides CRUD operations for differently identified entities of observed applications.

## How do we use Entity service?

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/HT-query-architecture.png) | 
|:--:| 
| *Hypertrace Query Architecture* |

- Core concepts like relationships between entities, their attributes are defined in this layer but this layer is generic and doesn't really care about what's inside each entity. 
- Entity service registers the attribute metadata with AttributeService.
- Entity service is also responsible for scalability of entity persistence and CRUD APIs.

## Building locally
The Entity service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Entity Service, run:

```
./gradlew clean build dockerBuildImages
```

## Docker Image Source:
- [DockerHub > Entity service](https://hub.docker.com/r/hypertrace/entity-service)
