
[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

override="{\"task.execute\":\"bin/run-beam-container.sh $@\"}"

exec $(dirname $0)/run-class.sh $1 --runner=org.apache.beam.runners.samza.SamzaRunner --configOverride="$override" "${@:2}"