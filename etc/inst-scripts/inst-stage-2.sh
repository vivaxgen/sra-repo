# stage-2

INST_SCRIPTS_DIR="${ENVS_DIR}/sra-repo/etc/inst-scripts"

echo ">>> Link resource files"
${VVGBIN}/link-resource-files.sh ${ENVS_DIR}/sra-repo/etc/bashrc.d

# if pyproject.toml already exists, skip this step
if [ -f "pyproject.toml" ]; then
    echo "pyproject.toml already exists, skipping creation"
else

VERSION=$(date +%y%m%d)
cat > ${VVG_BASEDIR}/pyproject.toml <<EOL
[project]
name = "${PIXI_ENVNAME}-venv"
version = "${VERSION}"
requires-python = "==${PYVER}.*"
dependencies = []
EOL

fi


if [[ -z ${VVG_MANIFEST_FILE:-} ]]; then
  echo -e "\e[32m>>> No manifest file provided, installing dependencies with inst-deps.sh\e[0m"
  source ${INST_SCRIPTS_DIR}/inst-deps.sh
fi


echo -e "\e[32m>>> Preparing directories\e[0m"
mkdir -p ${VVG_BASEDIR}/store
mkdir -p ${VVG_BASEDIR}/store/.lock
touch ${VVG_BASEDIR}/store/.sra-repo-db
mkdir -p ${VVG_BASEDIR}/tmp
mkdir -p ${VVG_BASEDIR}/cache
