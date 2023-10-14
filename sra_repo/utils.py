
import sys


def cerr(a_str):
    print(a_str, file=sys.stderr)


def cout(a_str):
    print(a_str, file=sys.stdout)


def cexit(a_str, err_code=1):
    cerr(a_str)
    sys.exit(err_code)


def check_gzip_file(path, prefix_cmds=[]):
    import subprocess
    ok = subprocess.call(prefix_cmds + ['gzip', '-t', path])
    if ok == 0:
        return True
    return False


def md5sum_file(path, prefix_cmds=[]):
    import subprocess
    output = subprocess.check_output(prefix_cmds + ['md5sum', path])
    return output.split()[0].decode('UTF-8')


def byte_conversion(size):
    r = 1024 * 1024
    if size < r:
        return f'{size/r:3.2f} MB'
    r *= 1024
    if size < r:
        return f'{size/r:3.2f} GB'
    r *= 1024
    return f'{size/r:6.2f} TB'


# EOF
