#!/usr/bin/env bash


ENV=$1

if [ -z "$ENV" ]; then
    echo "ENV not set"
    exit 1
elif [ "$ENV" != "dev" ] && [ "$ENV" != "alpha" ] && [ "$ENV" != "staging" ] && [ "$ENV" != "prod" ] && [ "$ENV" != "perf" ]; then
    echo "$ENV does not match supported environment"
    exit 1
fi

BUCKET_NAME=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=bucket_name "secret/dsde/firecloud/$ENV/avro-import/conf"`
CLOUD_FUNCTION_NAME=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=cloud_function_name "secret/dsde/firecloud/$ENV/avro-import/conf"`
PROJECT=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=project "secret/dsde/firecloud/$ENV/avro-import/conf"`
ORCHESTRATION_SERVICE_ACCOUNT=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=orchestration_service_account "secret/dsde/firecloud/$ENV/avro-import/conf"`
RAWLS_SERVICE_ACCOUNT=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=rawls_service_account "secret/dsde/firecloud/$ENV/avro-import/conf"`
TOPIC_NAME=`docker run -v $HOME:/root --rm broadinstitute/dsde-toolbox vault read -field=topic_name "secret/dsde/firecloud/$ENV/avro-import/conf"`
FUNCTION_DIRECTORY=function
LIFECYCLE_RULE_FILE=lifecycle_rule.json

# create cloud function
gcloud functions deploy $CLOUD_FUNCTION_NAME \
--runtime=python37 \
--region=us-central1 \
--entry-point=avro_to_rawls \
--memory=2048MB \
--source=$FUNCTION_DIRECTORY \
--timeout=540s \
--trigger-http \
--project=$PROJECT \
--set-env-vars=ENV=$ENV

# Remove all users
gcloud beta functions remove-iam-policy-binding $CLOUD_FUNCTION_NAME --member "allUsers" --role "roles/cloudfunctions.invoker" --project $PROJECT

# Add service account
gcloud beta functions add-iam-policy-binding $CLOUD_FUNCTION_NAME \
--member serviceAccount:$ORCHESTRATION_SERVICE_ACCOUNT \
--role "roles/cloudfunctions.admin" \
--project $PROJECT


# Create bucket
gsutil mb -p $PROJECT -b on $BUCKET_NAME

# Add the rawls service account to the bucket
gsutil iam ch serviceAccount:$RAWLS_SERVICE_ACCOUNT:admin $BUCKET_NAME


# Set lifecycle rule on the bucket
gsutil lifecycle set $LIFECYCLE_RULE_FILE $BUCKET_NAME


# Create pubsub topic
gcloud pubsub topics create $TOPIC_NAME --project $PROJECT


# Create notification on the pubsub topic
gsutil notification create -f json -e OBJECT_FINALIZE -t $TOPIC_NAME $BUCKET_NAME

