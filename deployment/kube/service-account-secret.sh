#!/bin/bash
if [[ "$1" == "production" ]]; then
    NAMESPACE="airflow"
elif [[ "$1" == "local" ]]; then
    NAMESPACE="airflow"
elif [[ "$1" == "staging-stg" ]]; then
    NAMESPACE="airflow-stg"
else
    echo "Usage: $0 <developmet|staging|production> "
    exit 1
fi

echo "kubectl -n $NAMESPACE delete secret airflow-serviceaccount"
kubectl -n $NAMESPACE delete secret airflow-serviceaccount
echo "kubectl -n $NAMESPACE create secret generic airflow-serviceaccount --from-file=service-account.json=./config/service-account.json"
kubectl create -n $NAMESPACE secret generic airflow-serviceaccount --from-file=service-account.json=./config/service-account.json
