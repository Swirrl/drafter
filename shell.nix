let
  pkgs     = import <nixpkgs> { };
  unstable = import (fetchTarball https://nixos.org/channels/nixos-unstable/nixexprs.tar.xz) { };
  # drafter  = import drafter/default.nix {
  #   pkgs      = unstable;
  #   stardogDBs = {
  #     drafter = {
  #       name = "drafter-test-db";
  #       tbox = "http://publishmydata.com/graphs/reasoning-tbox";
  #       reasoning = "SL";
  #     };
  #     drafterClient = {
  #       name = "drafter-client-test";
  #       tbox = "http://publishmydata.com/graphs/reasoning-tbox";
  #       reasoning = "SL";
  #     };
  #   };
  # };
in pkgs.stdenv.mkDerivation {

  name = "drafter";

  buildInputs = [
    ( unstable.clojure.override {
        jdk11 = unstable.openjdk;
    })
  ];

  SPARQL_QUERY_ENDPOINT = "http://localhost:5820/drafter-test-db/query";
  SPARQL_UPDATE_ENDPOINT = "http://localhost:5820/drafter-test-db/update";
  DRAFTER_SRC = "../drafter";
  DRAFTER_JWS_SIGNING_KEY = "random-garbage";
  DRAFTER_ENDPOINT = "http://localhost:3001";
  AUTH0_DOMAIN    = "https://dev-kkt-m758.eu.auth0.com";
  AUTH0_AUD       = "https://pmd";
  AUTH0_CLIENT_ID = "7klE25HUY333vTEx7rM1dmsnO6vHkaSG";
  AUTH0_CLIENT_SECRET = "QYoWNwf11dzWNh6XYd3jH8-j5j8r36UKuoFgrPakE_aw_Gy_EwWSppvqSULRICY4";


  # cp ${drafter.stardog.cfg}/* $STARDOG_HOME/
  shellHook = ''
    export STARDOG_HOME=$(mktemp -d)
    export MONGO_HOME=$(mktemp -d)
    export TRAVIS_BRANCH=$(git rev-parse --abbrev-ref HEAD | sed -e 's=/=_=g')
    export TRAVIS_BUILD_NUMBER=$(git rev-parse --short HEAD)
    trap "stop-drafter; clean-drafter" EXIT
  '';
}
