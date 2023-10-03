[[ -z "$STARGATE_VERSION" ]] && { export STARGATE_VERSION="0.0.0.$(date '+%Y%m%d')$VERSION_OVERRIDE"; }
[[ -z "$PYTHON_INDEX_URL" ]] && { export PYTHON_INDEX_URL="https://pypi.apple.com/simple"; }
[[ -z "$PYTHON_EXTRA_INDEX_URL" ]] && { export PYTHON_EXTRA_INDEX_URL="https://pypi.apple.com/simple"; }
[[ -z "$PYTHON_INDEX_REFRESH_WAIT_TIME" ]] && { export PYTHON_INDEX_REFRESH_WAIT_TIME=5; }

cd stargate-python-executor
rm -fR .build
mkdir .build
rm -fR .dist
mkdir .dist

cd .build

cp ../requirements.txt .
if ! [ -x "$(command -v gsed)" ]; then
  sed -i "s+stargate-python-runner+stargate-python-runner==$STARGATE_VERSION+g" requirements.txt
else
  gsed -i "s+stargate-python-runner+stargate-python-runner==$STARGATE_VERSION+g" requirements.txt
fi

mkdir pkg
cd pkg
sleep $PYTHON_INDEX_REFRESH_WAIT_TIME
python -m pip download --no-cache -i $PYTHON_INDEX_URL --extra-index-url $PYTHON_EXTRA_INDEX_URL -r ../requirements.txt
if [ -z "$(ls -A stargate_python_runner*)" ]; then
  echo "Stargate Python Runner not present!!"
  exit
fi
cd ../
cp ../init.sh .
zip -r ../.dist/stargate-python-executor.zip .
cd ../.dist
cp stargate-python-executor.zip stargate-python-executor-$STARGATE_VERSION.zip
cd ../