## KubernetesOpenShiftCreateJava

## Make sure you have access to the Rancher K3S cluster
```
michaelwilliams@Michaels-MBP ~ % rdctl shell sudo cat /etc/rancher/k3s/k3s.yaml > ~/k3s.yaml

michaelwilliams@Michaels-MBP ~ % export KUBECONFIG=~/k3s.yaml                               
kubectl get nodes

NAME                   STATUS   ROLES                  AGE     VERSION
lima-rancher-desktop   Ready    control-plane,master   4m44s   v1.32.5+k3s1
michaelwilliams@Michaels-MBP ~ % echo 'export KUBECONFIG=~/k3s.yaml' >> ~/.zshrc
michaelwilliams@Michaels-MBP ~ % source ~/.zshrc
michaelwilliams@Michaels-MBP ~ % kubectl get nodes
NAME                   STATUS   ROLES                  AGE     VERSION
lima-rancher-desktop   Ready    control-plane,master   6m18s   v1.32.5+k3s1
```
## Extract certificate from Kubeconfig (linux command line)
```
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > openshift-ca.crt
```
## Extract certificate from Kubeconfig (windows cmd prompt)
```
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' > encoded.txt
certutil -decode encoded.txt openshift-ca.crt 
```
## Extract certificate from Kubeconfig (windows power shell)
```
$base64 = oc config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}'
[System.Text.Encoding]::ASCII.GetString([System.Convert]::FromBase64String($base64)) | Out-File -Encoding ascii openshift-ca.crt
```
## Create a test namespace
```
kubectl create namespace test
```
## give default cluster-level service account edit privileges in all namespaces
```
kubectl create clusterrolebinding default-sa-cluster-edit \
  --clusterrole=edit \
  --serviceaccount=default:default
```
## give default cluster-level service account edit privileges in just test namespaces
```
kubectl create rolebinding default-sa-deployer \
  --namespace=test \
  --role=edit \
  --serviceaccount=default:default
```
## Export Bearer token
```
export BEARER_TOKEN=$(kubectl create token default)
```
## Build and Compile code
```
./gradlew clean build
./gradlew shadowJar
```
## Run code
```
java -jar build/libs/KubernetesOpenShiftCreateJava-all.jar
```
