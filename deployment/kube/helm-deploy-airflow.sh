#!/bin/bash
if [[ "$1" == "production" ]]; then
    NAMESPACE="airflow"
    HELM_VALUES="./kube/chart/airflow-production/values.yaml"
    AIRFLOW_NAME="airflow"
elif [[ "$1" == "local" ]]; then
    NAMESPACE="airflow"
    HELM_VALUES="./kube/chart/airflow-local/values.yaml"
    AIRFLOW_NAME="airflow"
elif [[ "$1" == "staging" ]]; then
    NAMESPACE="airflow"
    HELM_VALUES="./kube/chart/airflow-staging/values.yaml"
    AIRFLOW_NAME="airflow"
elif [[ "$1" == "staging-stg" ]]; then
    NAMESPACE="airflow-stg"
    HELM_VALUES="./kube/chart/airflow-staging/values.yaml"
    AIRFLOW_NAME="airflow"
else
    echo "Usage: $0 <local|staging|staging-stg|production> "
    exit 1
fi

echo "helm upgrade --install $AIRFLOW_NAME airflow-stable/airflow --version 8.8.0 --namespace $NAMESPACE --values $HELM_VALUES"
helm upgrade --install $AIRFLOW_NAME airflow-stable/airflow --version 8.8.0 --namespace $NAMESPACE --values $HELM_VALUES
