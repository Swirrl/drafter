DRAFTER_DIR="$(dirname $0)/.."
DEBIAN_DIR="$DRAFTER_DIR/debian"
DEBIAN_DRAFTER_DIR="$DEBIAN_DIR/drafter"
DRAFTER_JAR_FILE="$DEBIAN_DRAFTER_DIR/opt/drafter/drafter.jar"

if [ -f $DRAFTER_JAR_FILE ]
then
  # file exists
  echo 'drafter.jar exists in debian directory'
else
  # build uberjar and copy to debian dir
  lein uberjar && find "$DRAFTER_DIR/target/uberjar" -type f -name drafter-*-standalone.jar -exec cp -p {}  $DRAFTER_JAR_FILE \;
fi

docker run --name deb -v "$(pwd)/$DEBIAN_DIR:/artifacts" build-deb
