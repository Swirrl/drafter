#!/bin/bash

set -o errexit

# # start ssh agent and add key so clojure tools can clone git repos
# ssh-agent
# ssh-add

# install service dependencies
clojure -M:omni install-dependencies --dependencies dependencies-mongo-auth.edn

echo "Done omni install dependencies"

# start services
./.omni_cache/install/stardog/install/dev-start.sh
if [[ -d ./.omni_cache/install/mongodb ]]; then
    ./.omni_cache/install/mongodb/install/dev-start.sh
fi
