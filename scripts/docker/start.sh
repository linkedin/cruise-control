#!/bin/bash

if [ -z "${MSK_CLUSTER_ASSUME_ROLE}" ]; then
  echo "IAM Role to assume cannot be empty"
  exit 1
fi

if [ -z "${MSK_CLUSTER_ARN}" ]; then
  echo "You must provide a MSK Cluster ARN"
  exit 1
fi

if [ -z "${MSK_CLUSTER_REGION}" ]; then
  echo "You must provide the AWS region where the MSK Cluster is"
  exit 1
fi

if [ -z "${AUTH_TYPE}" ]; then
  echo "An IAM authentication mechanism must be provided"
  exit 1
fi

if [ -z "${PROMETHEUS_RETENTION_SIZE_GB}" ]; then
  echo "Please specify the prometheus retention size in gb. This cannot be larger than your serviceVolume"
  exit 1
fi

CREDENTIALS=$(aws sts assume-role \
    --role-arn "$MSK_CLUSTER_ASSUME_ROLE" \
    --role-session-name "cruise-control-start" \
    --query "Credentials.[AccessKeyId,SecretAccessKey,SessionToken]" \
    --output text)

ACCESS_KEY=$(echo "$CREDENTIALS" | awk '{print $1}')
SECRET_KEY=$(echo "$CREDENTIALS" | awk '{print $2}')
SESSION_TOKEN=$(echo "$CREDENTIALS" | awk '{print $3}')

bootstrap_brokers=$(AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_SESSION_TOKEN="$SESSION_TOKEN" \
                  aws kafka get-bootstrap-brokers \
                    --cluster-arn "${MSK_CLUSTER_ARN}" \
                    --query 'BootstrapBrokerStringSaslIam' \
                    --output text )


all_brokers=$(AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_SESSION_TOKEN="$SESSION_TOKEN" \
                  aws kafka list-nodes \
                      --cluster-arn "${MSK_CLUSTER_ARN}" \
                      | jq -r '[.NodeInfoList[] | select(.NodeType == "BROKER") | .BrokerNodeInfo.Endpoints[]] | join(",")')


echo "Bootstrap brokers: $bootstrap_brokers"
echo "All brokers: $all_brokers"

# Create targets.json Prometheus file based on output from MSK
IFS=',' read -r -a broker_array <<< "$all_brokers"
targets_jmx=()
targets_node=()

for broker in "${broker_array[@]}"; do
  targets_jmx+=("${broker%:*}:11001")
  targets_node+=("${broker%:*}:11002")
done

json_output=$(cat <<EOF
[
  {
    "labels": {
      "job": "jmx"
    },
    "targets": $(jq --compact-output --null-input '$ARGS.positional' --args -- "${targets_jmx[@]}")
  },
  {
    "labels": {
      "job": "node"
    },
    "targets": $(jq --compact-output --null-input '$ARGS.positional' --args -- "${targets_node[@]}")
  }
]
EOF
)

echo "$json_output" > /app/prometheus/targets.json

cd /app/cruise-control/config

sed -i "s/localhost:9092/${bootstrap_brokers}/g" cruisecontrol.properties
echo "aws.msk.cluster.arn=${MSK_CLUSTER_ARN}" >> cruisecontrol.properties
echo "aws.msk.cluster.region=${MSK_CLUSTER_REGION}" >> cruisecontrol.properties
echo "aws.msk.cluster.assume.role=${MSK_CLUSTER_ASSUME_ROLE}" >> cruisecontrol.properties
# Set a default value for LOG_LEVEL if it is not set or is empty
LOG_LEVEL=${LOG_LEVEL:-INFO}
echo "rootLogger.level=${LOG_LEVEL}" >> log4j.properties

if [ "${AUTH_TYPE}" == "IAM" ]; then
  echo "security.protocol=SASL_SSL" >> cruisecontrol.properties && \
  echo "sasl.mechanism=AWS_MSK_IAM" >> cruisecontrol.properties && \
  echo "sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"${MSK_CLUSTER_ASSUME_ROLE}\";" >> cruisecontrol.properties && \
  echo "sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler" >> cruisecontrol.properties && \
  wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar -P /app/cruise-control/cruise-control/build/dependant-libs
fi

cd .. && \
mkdir logs && \
touch logs/kafka-cruise-control.out
mkdir /opt/micros/serviceVolume/prometheus_data

/app/prometheus/prometheus --storage.tsdb.path=/opt/micros/serviceVolume/prometheus_data --storage.tsdb.retention.size="${PROMETHEUS_RETENTION_SIZE_GB}"GB --config.file=/app/prometheus/prometheus.yml > /app/prometheus/prometheus_output.log 2>&1 &
/app/cruise-control/kafka-cruise-control-start.sh -daemon /app/cruise-control/config/cruisecontrol.properties &
tail -f /app/cruise-control/logs/kafka-cruise-control.out