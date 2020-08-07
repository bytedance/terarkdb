#!/usr/bin/python

from os import walk
import subprocess
import sys

SKIP_LIST = ['db_bloom_filter_test', 'transaction_test', 'fault_injection_test', 'perf_context_test', 'obsolete_files_test']
TEST_LIST = []

def run_test(name):
    cmd = "./build/%s" % name
    print "run unit test: %s" % cmd
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    while process.poll() is None:     
        l = process.stdout.readline()
        print l
    print process.stdout.read()
    return process.returncode

if __name__ == '__main__':
    for (dirpath, dirnames, filenames) in walk("build"):
        TEST_LIST = [x for x in filenames if x not in SKIP_LIST and x.endswith("test")]
        break

    # run all tests
    for testname in TEST_LIST:
        code = run_test(testname)
        if code != 0:
            print 'unit test failed: %s, code = %d' % (testname, code)
            sys.exit(code)
