
sra-repo
========

SRA repository management system


Overview
--------

sra-repo is a set of command line tools to manage a centralized, local repository containing
published FASTQ files downloaded from SRA databases (NCBI Entrez or EBI ENA).

Some features:

* perform parallel downloads from SRA databases (NCBI Entrez or EBI ENA)

* automatically set all fastq files to read-only to avoid accidental modification of the files

* create symbolic links for the necessary fastq files from central location to target directory



Example
-------

Several usage example:

Fetching FASTQ files
~~~~~~~~~~~~~~~~~~~~

Fetching SRAs from NCBI Entrez database using 3 parallel downloader workers (tasks)::

    $ sra-repo.py fetch --entrez --ntasks 3 ERR

Fetching SRAs from EBI ENA with SRA IDs taken from a file containing each ID per line::

    $ sra-repo.py fetch --ena --ntasks 10 --idfile my_sraids.txt

Fetching SRAs with SRA IDs taken from a column named ENA of a tab-delimited file with proper
headers (ie. a sample file)::

    $ sra-repo.py fetch --entrez --ntasks 20 --samplefile my_samplefile.tsv:ENA

Linking FASTQ files
~~~~~~~~~~~~~~~~~~~

Linking FASTQ files to a target directory is usually necessary before any analysis can be performed.

Installation
------------

sra-repo is written with python3 with the following additional modules used:

* pycurl

* rich


sra-repo requires several external software to be installed:

* sratools

* bcftools

* GNU parallel

