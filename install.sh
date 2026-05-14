#!/usr/bin/bash

# installation script for G6PD pipeline

# optional variable:
# - VVG_BASEDIR
# - VVG_EXCLUDE
# - VVG_SRAREPO_REPOURL

set -eu

# run the base.sh
# Detect the shell from which the script was called
parent=$(ps -o comm $PPID |tail -1)
parent=${parent#-}  # remove the leading dash that login shells have
case "$parent" in
  # shells supported by `micromamba shell init`
  bash|fish|xonsh|zsh)
    shell=$parent
    ;;
  *)
    # use the login shell (basename of $SHELL) as a fallback
    shell=${SHELL##*/}
    ;;
esac

# Parsing arguments
if [ -t 0 ] && [ -z "${VVG_BASEDIR:-}" ]; then
  printf "Pipeline base directory? [./SRA] "
  read VVG_BASEDIR
fi

# default value
VVG_BASEDIR="${VVG_BASEDIR:-./SRA}"

PIXI_ENVNAME="${PIXI_ENVNAME:-SRA}"
PYVER=3.14
VVG_EXCLUDE="snakemake"
source <(curl -L https://raw.githubusercontent.com/vivaxgen/vvg-box/main/install.sh)

echo ">> Cloning vivaxGEN SRA-Repo"
git clone --depth 1 ${VVG_SRAREPO_REPOURL:-https://github.com/vivaxgen/sra-repo.git} ${ENVS_DIR}/sra-repo

source ${ENVS_DIR}/sra-repo/etc/inst-scripts/inst-stage-2.sh

echo "sra-repo" >> ${ETC_DIR}/installed-repo.txt

echo ""
echo "vivaxGEN SRA-Repo has been successfully installed. Read the docs for usage."
echo "Execute the activation file to start using it:"
echo ""
echo "   " `readlink -e ${VVG_BASEDIR}/bin/activate`
echo ""
echo "or source the activation file (eg. inside a script):"
echo "    source" `readlink -e ${VVG_BASEDIR}/bin/activate`
echo ""

# EOF