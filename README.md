# Entity Service

Service that provides CRUD operations for differently identified entities of observed applications.

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/arch/ht-query.png) | 
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

#### With docker-compose
To test your image using the docker-compose setup follow the steps:

- Commit you changes to a branch say `entity-service-test`.
- Go to [hypertrace-service](https://github.com/hypertrace/hypertrace-service) and checkout the above branch in the submodule.
```
cd entity-service && git checkout entity-service-test && cd ..
```
- Change tag for `hypertrace-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-service:
    image: hypertrace/hypertrace-service:test
    container_name: hypertrace-service
    ...
```
- and then run `docker-compose up` to test the setup.


#### With helm setup
Add image repository and tag in values.yaml file [here](https://github.com/hypertrace/hypertrace/blob/main/kubernetes/platform-services/values.yaml) like below and then run `./hypertrace.sh install` again and you can test your image!

```yaml
entity-service:
  image:
    repository: "hypertrace/entity-service"
    tagOverride: "test"
 ```

## Docker Image Source:
- [DockerHub > Entity service](https://hub.docker.com/r/hypertrace/entity-service)
