
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



Examples
--------

Several usage examples:

Fetching FASTQ files
~~~~~~~~~~~~~~~~~~~~

Fetching SRAs from public database (by default, sra-repo.py will try EBI ENA first, and then NCBI Entrez) using 3 parallel downloader workers (tasks)::

    $ sra-repo.py fetch --ntasks 3 ERR175543 ERR175544

Fetching SRAs with SRA IDs taken from a file containing each ID per line::

    $ sra-repo.py fetch --ntasks 10 --sraidfile my_sraids.txt

Fetching SRAs with SRA IDs taken from a column named ENA of a tab-delimited file with proper
headers (ie. a sample file)::

    $ sra-repo.py fetch --ntasks 20 --samplefile my_samplefile.tsv:ENA

Checking FASTQ files
~~~~~~~~~~~~~~~~~~~~

To check the existance of certain SRA IDs in the database::

    $ sra-repo.py check ERR175543 ERR175544

or::

    $ sra-repo.py check --sraidfile my_sraids.txt

or::

    $ sra-repo.py check --samplefile my_samplefile.tsv:ENA

To also validate the FASTQ files, use --validate argument::

    $ sra-repo.py check --validate ERR175543 ERR175544

Finding information about FASTQ files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Other commands available in sra-repo are ``info`` and ``path``, which will provide
information on the FASTQ files and the actual path where the FASTQ files were stored,
respectively::

    $ sra-repo.py info
    $ sra-repo.py path

Both commands also can accept a SRA ID file or a sample file, using --sraidfile or --samplefile argument.

Linking FASTQ files
~~~~~~~~~~~~~~~~~~~

Linking FASTQ files to a target directory is usually necessary before any analysis be performed, as it will ease dealing with file path etc.

To create links for several SRA IDs directly::

    $ mkdir test
    $ sra-repo.py link --outdir test ERR175543 ERR175544
    $ ls test

A text file or tab-delimited file can also be used::

    $ sra-repo.py link --outdir test --sraidfile my_sraids.txt

or::

    $ sra-repo.py link --outdir test --samplefile my_samplefile.tsv:Sample,ENA

Please note that when using samplefile, the column names for Sample identifier and SRA ids are required.

When using a sample file, sra-repo can provide a manifest file, a two-column tab-delimited file
with SAMPLE and FASTQ header, providing the sample code and its associated FASTQ files
separated by comma for paired files, and semi-colon for different SRA for the same sample::

    $ sra-repo.py link --outdir test --o my-manifest.tsv --samplefile my_samplefile.tsv:Sample,ENA

Installation
------------

sra-repo is written in Python (the development is with Python 3.11) with the following additional modules used:

* pycurl

* requests

* rich


Python can be installed either using Conda, or using the operating system software manager
(eg. dnf for rpm-based Linux system or apt for deb-based Linux system), or download directly
from https://python.org. To install the above modules, once Python3 has been installed, do the following::

    $ pip3 install pycurl rich requests

sra-repo also requires several external software to be installed:

* NCBI SRA-Toolkit (can be obtained from https://github.com/ncbi/sra-tools)

* bcftools (can use Conda or download/install manually from https://htslib.org)

* GNU parallel (comes in almost all Linux distributions)

[to be continued]
