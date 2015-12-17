FROM    busybox:latest

ADD     . /nail/srv/configs/
VOLUME  /nail/srv/configs/

ADD     nail-etc/services.yaml /nail/etc/services/services.yaml
VOLUME /nail/etc/services/

Add     nail-etc/uswest2devc.yaml /nail/etc/zookeeper_discovery/generic/uswest2devc.yaml
VOLUME /nail/etc/zookeeper_discovery/generic

Add     nail-etc/ecosystem /nail/etc/ecosystem
VOLUME /nail/etc/

Add     yelp /nail/etc/services/database_pii/yelp
VOLUME /nail/etc/services/database_pii


Add     nail-etc/teams.yaml /nail/etc/services/data_pipeline/teams.yaml
VOLUME /nail/etc/services/data_pipeline


Add     nail-etc/datapipe.yaml /nail/etc/kafka_discovery/datapipe.yaml
VOLUME /nail/etc/kafka_discovery

# safer_docker expects a command
CMD     echo
