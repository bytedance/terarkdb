#!/usr/bin/python

from os import walk
import subprocess
import sys

SKIP_LIST = []
TEST_LIST = [
"options_settable_test",
"option_change_migration_test",
"document_db_test",
"options_util_test",
"cleanable_test",
"heap_test",
"object_registry_test",
"c_test",
"compaction_iterator_test",
"listener_test",
"event_logger_test",
"lazy_buffer_test",
"db_table_properties_test",
"thread_list_test",
"trace_analyzer_test",
"merger_test"
        ]

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
    #for (dirpath, dirnames, filenames) in walk("build"):
    #    TEST_LIST = [x for x in filenames if x not in SKIP_LIST and x.endswith("_test")]
    #    break

    # run all tests
    if len(sys.argv) > 1 and sys.argv[1] != None:
        failed = []
        for testname in TEST_LIST:
            code = run_test(testname)
            if code != 0:
                failed.append(testname)
        "\n".join(failed)
        f = open(sys.argv[1], "a+w")
        f.writelines(failed) 
        f.close()
    else:
        for testname in TEST_LIST:
            code = run_test(testname)
            if code != 0:
                print 'unit test failed: %s, code = %d' % (testname, code)
                sys.exit(code)
