Devops, Drafter & Docker
========================

Successful drafter builds get built into docker images on dockerhub (hub.docker.com) at `swirrl/drafter`.

Running the container
---------------------

Suggested commands:

1. Pull the image

`docker pull swirrl/drafter:build_<build-number>`

2. Make a data container for the database

`docker run -d -v /data/drafter-database --name drafter-data ubuntu true`

3. Make a data container for the logs

`docker run -d -v /drafter/logs --name drafter-logs ubuntu true`

4. Run drafter itself

Depending on the server topology, you might want to do one of the following:

* link the drafter container to a client container
* expose a port to the host server (which you could proxy-to via nginx)
* use an ambassador container to link over the network

e.g. (exposing a port to the host server)
`docker run -d -p 127.0.0.1:<port-to-expose-on-host>:3001 --volumes-from drafter-data --volumes-from drafter-logs --name drafter swirrl/drafter:build_xxx`

(check it's running with `docker ps`)

To backup the data
------------------
`docker run --volumes-from drafter-data -v <folder-on-the-host-to-backup-to>:/data-backup ubuntu tar cvf /data-backup/backup.tar /data/drafter-database`

Explanation: this

* creates a new container based on `ubuntu` image,
* mounts the data volume onto it
* mounts a folder of your choice from the host system into `/data-backup` inside the container
* inside the container, it tars contents of `/var/lib/drafter-database` (the mounted volume from `drafter-data`) into `/data-backup` volume (which will appear on the mounted host dir).

To backup the logs
------------------

Similarly to the data:

`docker run --volumes-from drafter-logs -v <folder-on-the-host-to-backup-to>:/logs-backup ubuntu tar cvf /logs-backup/backup.tar /drafter/logs`

To peek inside a running drafter docker container
-------------------------------------------------

This will get you a bash shell inside the running drafter docker container

`docker exec -i -t drafter bash`
