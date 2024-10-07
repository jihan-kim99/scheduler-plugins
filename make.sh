make local-image

VERSION="v$(date +%Y%m%d)-"
docker tag localhost:5000/scheduler-plugins/kube-scheduler:${VERSION} rfvtgbyh11/scheduler
docker tag localhost:5000/scheduler-plugins/controller:${VERSION} rfvtgbyh11/controller

docker push rfvtgbyh11/scheduler
docker push rfvtgbyh11/controller

helm uninstall scheduler-plugins
helm install scheduler-plugins manifests/install/charts/as-a-second-scheduler/