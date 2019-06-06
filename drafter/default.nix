{ pkgs, source ? ./., tbox ? "*", reasoning ? "NONE" }:
let stdenv = pkgs.stdenv;
in stdenv.mkDerivation rec {

  name = "drafter";
  src = source;

  stardog = import ../../../stardog {
    pkgs      = pkgs;
    stdenv    = pkgs.stdenv;
    dbname    = "drafter-test-db";
    tbox      = tbox;
    reasoning = reasoning;
  };

  mongo = import ../../../shell.nix/mongo.default.nix {
    pkgs   = pkgs;
    stdenv = pkgs.stdenv;
    dbname = "drafter-user";
  };

  buildInputs = [
    pkgs.clojure
    pkgs.leiningen
    pkgs.openjdk
    stardog
    mongo
  ];

  startScript = ''
    cd \$DRAFTER_SRC
    start-stardog
    start-mongo
    lein run &
    export DRAFTER_PID=\$!
  '';

  killScript = ''
    if [ -n \"\$DRAFTER_PID\" ]; then
      kill \$(ps -o pid= --ppid \$DRAFTER_PID)
      echo Killed drafter
    fi
    stop-mongo
    stop-stardog
  '';

  cleanScript = ''
    clean-mongo
    clean-stardog
  '';

  installScripts = ''
    ${stardog.installScripts}
    ${mongo.installScripts}
    echo "${startScript}" > $out/bin/start-drafter
    chmod +x $out/bin/start-drafter
    echo "${killScript}" > $out/bin/stop-drafter
    chmod +x $out/bin/stop-drafter
    echo "${cleanScript}" > $out/bin/clean-drafter
    chmod +x $out/bin/clean-drafter
  '';

  configurePhase = ''
    # export LEIN_HOME=$(mktemp -d)
    # export M2_HOME=$(mktemp -d)
    # export _JAVA_OPTIONS=-Duser.home=$M2_HOME
    # export CLASSPATH=
    mkdir -p $out/bin
    # lein deps
  '';

  buildPhase = ''
    # lein uberjar
  '';

  installPhase = ''
    ${installScripts}
  '';

  postInstall = ''
    # rm -r $LEIN_HOME
    # rm -r $M2_HOME
  '';

  meta = with stdenv.lib; {
    description = "drafter";
    homepage = https://github.com/swirrl/drafter/;
    platforms = [ "x86_64-linux" "i686-linux" ];
    maintainers = with maintainers; [ andrewmcveigh ];
  };
}
