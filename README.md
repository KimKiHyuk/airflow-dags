# Airflow settings repository

## requirements
* k8s
* helm
* bitnami


## commands
> install k8s

https://docs.microsoft.com/ko-kr/azure/aks/kubernetes-walkthrough
> install airflow on k8s
```
helm install cluster -f ./airflow-dags/airflow.yaml bitnami/airflow
```

> upgrade airflow on k8s
```
helm upgrade cluster -f ./airflow-dags/airflow.yaml bitnami/airflow
```

> port-foward airflow web ui 
```
kubectl port-forward --namespace default svc/cluster-airflow 8080:8080
```