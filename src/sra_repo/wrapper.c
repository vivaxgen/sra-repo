#include <stdio.h>
#include <unistd.h>

int main(int argc, char ** argv)
{
        /* Reset uid/gid */
        setregid(getegid(), getegid());
        /* setreuid(geteuid(), geteuid()); */

	/* need to clean up LD_LIBRARY_PATH and other enviroment things here
  	   to minimize security problem */

        /* Attempt to execute script */
        execv("./script_wrapped", argv);

        /* Reach here if execv failed */
        perror("execv");
        return 1;
}

/* EOF */
