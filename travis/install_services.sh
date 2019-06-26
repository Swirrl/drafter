#!/bin/bash

# start ssh agent and add key so clojure tools can clone git repos
ssh-agent
ssh-add

# install service dependencies
clojure -M:omni install-dependencies

# start services
./.omni_cache/install/stardog/install/dev-start.sh
./.omni_cache/install/mongodb/install/dev-start.sh
