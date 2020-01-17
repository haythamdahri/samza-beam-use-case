
# Check if server is set. If not - set server optimization
[[ $JAVA_OPTS != *-server* ]] && export JAVA_OPTS="$JAVA_OPTS -server"

# Set container ID system property for use in Log4J
[[ $JAVA_OPTS != *-Dsamza.container.id* && ! -z "$SAMZA_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dsamza.container.id=$SAMZA_CONTAINER_ID"

# Set container name system property for use in Log4J
[[ $JAVA_OPTS != *-Dsamza.container.name* && ! -z "$SAMZA_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dsamza.container.name=samza-container-$SAMZA_CONTAINER_ID"

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

override="{\"task.execute\":\"bin/run-beam-container.sh $@\"}"

exec $(dirname $0)/run-class.sh $1 --runner=org.apache.beam.runners.samza.SamzaRunner --configFactory=org.apache.beam.runners.samza.container.ContainerCfgFactory --configOverride="$override" "${@:2}"