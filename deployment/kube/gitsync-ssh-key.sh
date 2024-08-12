#!/bin/bash
if [[ "$1" == "production" ]]; then
    NAMESPACE="airflow"
elif [[ "$1" == "local" ]]; then
    NAMESPACE="airflow"
elif [[ "$1" == "staging-stg" ]]; then
    NAMESPACE="airflow-stg"
else
    echo "Usage: $0 <local|staging|staging-stg|production> "
    exit 1
fi

echo "kubectl -n $NAMESPACE delete secret airflow-ssh-git-secret-rsa"
kubectl -n $NAMESPACE delete secret airflow-ssh-git-secret-rsa
echo "kubectl -n $NAMESPACE create secret generic airflow-ssh-git-secret-rsa --from-file=id_rsa=./config/ssh-key"
kubectl create -n $NAMESPACE secret generic airflow-ssh-git-secret-rsa --from-file=id_rsa=./config/ssh-key
