
echo -e "\e[32m>>>> Setting pixi default channels to conda-forge and bioconda\e[0m"
pixi config set default-channels '["conda-forge", "bioconda"]' --global
pixi config set default-channels '["conda-forge", "bioconda"]'

echo -e "\e[32m>>>> Installing latest htslib tools\e[0m"
# samtools is needed to convert CRAM/BAM to FASTQ files
pixi global install "samtools>=1.18"

echo -e "\e[32m>>>> Installing NCBI SRA Toolkit\e[0m"
pixi global install sra-tools

echo -e "\e[32m>>>> Installing uv\e[0m"
pixi add uv

(
    cd ${VVG_BASEDIR}
    uv add --editable envs/sra-repo
)

# EOF
