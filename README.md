# rest-users-connectors

A sample Rest Connector for midPoint implementing the Rest Connector Superclass model

# Installation

1. Clone this repository

```
git clone git@github.com:rakkau/midpoint-rest-connector-sample.git
```

2. Compile the sources and run the application

```
cd midpoint-rest-connector-sample
mvn clean package
```

3. Copy the connector jar to the midpoint folder

```
cp target/midpoint-rest-connector-sample-0.0.X-SNAPSHOT.jar $MIDPOINT_HOME/var/icf-connectors/
```

4. Restart midPoint

5. Create the resource using the connector


# References

* Rest Users Api: https://github.com/rakkau/rest-users-api
* Rest Connector Superclass: https://wiki.evolveum.com/display/midPoint/REST+Connector+Superclass 
* Rest Resource Example: https://github.com/Evolveum/midpoint/tree/master/samples/resources/rest