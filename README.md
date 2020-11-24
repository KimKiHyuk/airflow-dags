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

## How to throw custom ENV (like aws_access_token)
1. Modify [it](https://github.com/KimKiHyuk/airflow-dags/blob/4aa042d2c1b47be9683427987ca935c291f6ca5a/airflow.yaml#L323)
2. Use it in airflow
```
>>> import os
>>> print(os.envrion['AIRFLOW__AWS__SECRET'])
hallym
```


## scripts
* run_emr.py - add .net spark step to emr remotely 
