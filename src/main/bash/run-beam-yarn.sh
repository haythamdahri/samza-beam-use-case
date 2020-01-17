
home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

export EXECUTION_PLAN_DIR="$base_dir/plan"
mkdir -p $EXECUTION_PLAN_DIR

[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

op=$(if [[ "$@" =~ (--operation=)([^ ]*) ]]; then echo "${BASH_REMATCH[2]}"; else echo "run"; fi)

override="{\"task.execute\":\"bin/run-beam-container.sh $@\"}"

case $op in
  run)
    exec $(dirname $0)/run-class.sh $1 --runner=org.apache.beam.runners.samza.SamzaRunner --configOverride="$override" "${@:2}"
  ;;

  kill)
    exec $(dirname $0)/run-class.sh org.apache.samza.job.JobRunner "$@"
  ;;

  status)
    exec $(dirname $0)/run-class.sh org.apache.samza.job.JobRunner "$@"
  ;;
esac