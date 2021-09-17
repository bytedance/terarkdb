#!/usr/bin/python
#
# Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
# This source code is licensed under Apache 2.0 License.
#
# Usage:
#       ./bench.py $PWD/data 20 10 24,2000 readrandomwriterandom,10
#
#       ./bench.py $PWD/data 20 10 24,2000 readrandomwriterandom,90
#
import time
import io
import os
import subprocess
import sys
import requests
from datetime import datetime

# default values
KV_SIZE = "24, 500"
KSIZE = 24  # key size form KV_SIZE
VSIZE = 500 # value size from KV_SIZE
GB_PER_THREAD = 20
TOTAL_MEM_IN_GB = 64
THREADS = 16
DB_DIR = ""
BENCH_TYPE = "fillseq"
BENCH_ARGS = []
ZBD_PATH = ""

# collected result log
LOG_RESULT_FNAME = "log.txt"
# bench rocksdb output
LOG_BENCH_OUTPUT_FNAME = "output.txt"

BENCH_ENGINES = {'terarkdb':'./output/db_bench'}

def bench(records, key_size, value_size, engine, db_dir, exist_db):
    extra_flags = ''
    if engine == 'terarkdb':
        extra_flags = """
                         --use_terark_table=false
                         --blob_size=128
                      """
    if BENCH_TYPE == 'readrandomwriterandom':
        extra_flags += """
                         --readwritepercent=%s
                       """ % BENCH_ARGS[0]
    if ZBD_PATH != '':
        extra_flags += """
                         --zbd_path=%s
                       """ % ZBD_PATH
    cmd = """
           {db_bench} \
           --benchmarks={bench_type}
	   --use_existing_db={exist_db}
           --sync=1
	   --db={db_dir}
	   --wal_dir={db_dir}
	   --bytes_per_sync=65536
           --wal_bytes_per_sync=65536
           --num={records}
           --threads={threads}
	   --num_levels=6
           --delayed_write_rate=209715200
	   --key_size={key_size}
	   --value_size={value_size}
	   --cache_numshardbits=6
	   --level_compaction_dynamic_level_bytes=true
	   --cache_index_and_filter_blocks=1
	   --pin_l0_filter_and_index_blocks_in_cache=0
	   --benchmark_write_rate_limit=0
	   --hard_rate_limit=3
	   --rate_limit_delay_max_milliseconds=1000000
	   --write_buffer_size=268435456
	   --max_write_buffer_number=6
	   --target_file_size_base=134217728
	   --max_bytes_for_level_base=536870912
	   --verify_checksum=1
	   --delete_obsolete_files_period_micros=62914560
	   --max_bytes_for_level_multiplier=10
	   --statistics=0
	   --stats_per_interval=1
	   --stats_interval_seconds=60
	   --histogram=1
	   --open_files=-1
	   --level0_file_num_compaction_trigger=4
	   --level0_slowdown_writes_trigger=1000
	   --level0_stop_writes_trigger=1000
           --num_high_pri_threads=3
           --num_low_pri_threads=10
           --mmap_read=true
           --compression_type=none
           --memtablerep=skip_list
           {extra_flags}
           """.format(records=records, 
                      key_size=key_size,
                      value_size=value_size, 
                      db_dir=db_dir, 
                      bench_type=BENCH_TYPE,
                      exist_db=exist_db,
                      threads=THREADS,
                      db_bench=BENCH_ENGINES[engine],
                      extra_flags=extra_flags)

    cmd = cmd.replace('\n',' ')
    log = open(LOG_BENCH_OUTPUT_FNAME, 'wb')
    log.write(cmd)
    log.flush()
    process = subprocess.Popen(cmd,
                               stdin=subprocess.PIPE,
                               stderr=log, 
                               stdout=log, 
                               shell=True)
    process.communicate()
    log.flush()
    log.close()
    print 'test finished: %s\n' % LOG_BENCH_OUTPUT_FNAME


def run(engine, db_dir):
    db_size_bytes = int(GB_PER_THREAD) * 1024 * 1024 * 1024
    records = db_size_bytes / (KSIZE + VSIZE)
    bench(records, KSIZE, VSIZE, engine, db_dir, 0)


def gather_result(engine):
    rst = {}
    for bench_type in [BENCH_TYPE]:
        rst[bench_type] = {}
        
        rst[bench_type] = {}

        with open(LOG_BENCH_OUTPUT_FNAME, 'rb') as f:
            lines = f.readlines()
            i = 0
            while i < len(lines):
                # get ops
                s = '%s' % bench_type
                if lines[i].find(s) == 0:
                    rst[bench_type]['ops'] = lines[i].split()[4]

                # get rest of them
                s = 'Microseconds per '
                if lines[i].find(s) >= 0:
                    ops_type = lines[i][17:-2]
                    rst[bench_type][ops_type] = {}
                    rst[bench_type][ops_type]['max'] = lines[i+2].split()[5]
                    rst[bench_type][ops_type]['percentiles'] = lines[i+3][13:-2]
                    i = i + 5
                else:
                    i = i + 1

    # print rst
    output = [('benchmark', 'kv bytes', 'ops', 'operation', 'max lat(us)', 'pct(us)')]
    for bench in rst:
            for t in ['read', 'write']:
                if rst[bench].has_key(t):
                    output.append( (bench, KV_SIZE, rst[bench]['ops'], t, rst[bench][t]['max'], rst[bench][t]['percentiles']) )

    with open(LOG_RESULT_FNAME, 'a') as f:
        for row in output:
            f.write('{0:<25} {1:<15} {2:<15} {3:<15} {4:<15} {5:<100}\n'.format(*row))

if __name__=='__main__':
    if not os.path.isfile(BENCH_ENGINES['terarkdb']):
        print 'db_bench not found, please check: %s', BENCH_ENGINES
        sys.exit()

    if len(sys.argv) != 6:
        print 'usage: ./bench.py [DB_DIR] [GB_PER_THREAD] [THREADS] [KV_SIZE] [BENCH_STR]\n'
        print '\t\tKV_SIZE: 24,500 means key size is 24, value size is 500'
        print '\t\tBENCH_STR: fillseq or readrandomwriterandom,90, the later one means 90% reads'
        sys.exit()

    DB_DIR = sys.argv[1]
    GB_PER_THREAD = sys.argv[2]
    THREADS = sys.argv[3]
    KV_SIZE = sys.argv[4]
    KSIZE, VSIZE = [int(i) for i in KV_SIZE.split(',')]

    BENCH_STR = sys.argv[5].split(",")
    BENCH_TYPE = BENCH_STR[0]
    BENCH_ARGS = BENCH_STR[1:]
    print 'bench_type = %s, bench_args = %s' % (BENCH_TYPE, BENCH_ARGS)

    LOG_RESULT_FNAME = 'rst_%s_gb_%s_thds.txt' % (GB_PER_THREAD, THREADS)

    for engine in ['terarkdb']:
        LOG_BENCH_OUTPUT_FNAME = "output_%s_%s_%s_%s.txt" % (engine, BENCH_TYPE, KSIZE, VSIZE)

        db_dir = '%s_%s' %  (DB_DIR, engine)
        print 'start engine : %s, db_dir = %s' % (engine, db_dir)

        with open(LOG_RESULT_FNAME, 'a') as f:
            f.write('[%s] GB_PER_THREAD: %s, THREADS = %s, KV_SIZE = %s, BENCH_STR = %s, DATA_DIR = %s\n' % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), GB_PER_THREAD, THREADS, KV_SIZE, BENCH_STR, db_dir) )

        run(engine, db_dir)
        gather_result(engine)

        with open(LOG_RESULT_FNAME, 'a') as f:
            f.write('\n\n')
