# Entity Service
###### org.hypertrace.entity.service

[![CircleCI](https://circleci.com/gh/hypertrace/entity-service.svg?style=svg)](https://circleci.com/gh/hypertrace/entity-service)

Service that provides CRUD operations for differently identified entities of observed applications.

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/hypertrace-query-arch.png) | 
|:--:| 
| *Hypertrace Query Architecture* |

- A service layer manages a life cycle of the identified entities of observed applications.
- Provides CRUD operations for raw or enriched entities, for its types, and their relations.

## Building locally
The Entity service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Entity Service, run:

```
./gradlew dockerBuildImages
```
## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 


### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose
Change the tag for `entity-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  entity-service:
    image: hypertrace/entity-service:test
    container_name: entity-service
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Entity service](https://hub.docker.com/r/hypertrace/entity-service)
