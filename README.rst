
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

sra-repo has bash tab-complete feature to make typing faster and typo-error free.
Single-tapping TAB key will complete the argument automatically while double-tapping
on TAB key will provide the available arguments. Do note that this feature only available
under bash shell. Try::

    $ sra-repo.py [TAB][TAB]
    $ sra-repo.py fe[TAB]

Several usage examples:

Fetching FASTQ files
~~~~~~~~~~~~~~~~~~~~

Fetching SRAs from public database (by default, sra-repo.py will try EBI ENA first, and then NCBI Entrez) using 3 parallel downloader workers (tasks)::

    $ sra-repo.py fetch --ntasks 3 ERR175543 ERR175544

Fetching SRAs with SRA IDs taken from a file containing each ID per line::

    $ sra-repo.py fetch --ntasks 10 --idfile my_sraids.txt

Fetching SRAs with SRA IDs taken from a column named ENA of a tab-delimited file with proper
headers (ie. a sample file)::

    $ sra-repo.py fetch --ntasks 20 --samplefile my_samplefile.tsv:ENA

Checking FASTQ files
~~~~~~~~~~~~~~~~~~~~

To check the existance of certain SRA IDs in the database::

    $ sra-repo.py check ERR175543 ERR175544

or::

    $ sra-repo.py check --idfile my_sraids.txt

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

    $ sra-repo.py link --outdir test --idfile my_sraids.txt

or::

    $ sra-repo.py link --outdir test --samplefile my_samplefile.tsv:Sample,ENA

Please note that when using samplefile, the column names for Sample identifier and SRA ids are required.

When using a sample file, sra-repo can provide a manifest file, a two-column tab-delimited file
with SAMPLE and FASTQ header, providing the sample code and its associated FASTQ files
separated by comma for paired files, and semi-colon for different SRA for the same sample::

    $ sra-repo.py link --outdir test --o my-manifest.tsv --samplefile my_samplefile.tsv:Sample,ENA

Installation
------------

The first step is to decide the main root directory where sra-repo and its repository system
will be stored. For example, with main root directory of ``/shared/SRA``, the following
directory structure would be recommended::

    /shared/SRA
    /shared/SRA/bin [for activate.sh script]
    /shared/SRA/opt [for manual installation of the requirements if without Conda ]
    /shared/SRA/opt/env [for sra-repo installation]
    /shared/SRA/store [for the main repository of all FASTQ files]
    /shared/SRA/tmp [for temporary space during downloads and format convertion]
    /shared/SRA/cache [for samtools-fastq caching system converting CRAM to FASTQ]

To prepare the above directory structures and also install sra-repo, the following commands
can be used::

    $ export MAIN_ROOT=/share/SRA
    $ mkdir $MAIN_ROOT/bin $MAIN_ROOT/opt $MAIN_ROOT/opt/env $MAIN_ROOT/store $MAIN_ROOT/tmp $MAIN_ROOT/cache
    $ git clone https://github.com/vivaxgen/sra-repo.git $MAIN_ROOT/opt/env/

sra-repo is written in Python (the development is with Python 3.11) with the following additional modules used:

* pycurl

* requests

* rich

* argcomplete


Python can be installed either using Conda, or using the operating system software manager
(eg. dnf for rpm-based Linux system or apt for deb-based Linux system), or download directly
from https://python.org. Once Python3 has been installed, install the required modules by 
doing the following::

    $ pip3 install pycurl rich requests argcomplete

sra-repo also requires several external software to be installed:

* NCBI SRA-Toolkit (can use Conda or be obtained from https://github.com/ncbi/sra-tools)

* bcftools (can use Conda or download/install manually from https://htslib.org)

* GNU parallel (comes in almost all Linux distributions)

If all requirements are going to be manually installed (ie. not using Conda), all requirements
can be installed in $MAIN_ROOT/opt where MAIN_ROOT is the main root directory of sra-repo repository (eg. /shared/SRA with the above example).

[to be continued]
