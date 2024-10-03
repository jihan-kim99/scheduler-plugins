
make local-image

VERSION="v$(date +%Y%m%d)-"
docker tag localhost:5000/scheduler-plugins/kube-scheduler:${VERSION} jinnkenny99/scheduler
docker tag localhost:5000/scheduler-plugins/controller:${VERSION} jinnkenny99/controller

docker push jinnkenny99/scheduler
docker push jinnkenny99/controller