// SPDX-License-Identifier: GPL-2.0
#include <linux/hrtimer.h>
#include <linux/irqflags.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/percpu.h>
#include <linux/proc_fs.h>
#include <linux/sched/clock.h>
#include <linux/seq_file.h>
#include <linux/sizes.h>
#include <linux/stacktrace.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/vmstat.h>
#include <linux/kallsyms.h>
#include <linux/mmzone.h>
#include <linux/nodemask.h>

struct mem_reserve {
    unsigned long size;
    unsigned long nr;
    struct page *pages[SZ_2M / 8];
};

static struct mem_reserve mem_reserve_data;

static void free_all_mem(void)
{
    int i, size = mem_reserve_data.nr;

    for (i = 0; i < size; i++) {
        __free_pages(mem_reserve_data.pages[i], 10);
        mem_reserve_data.pages[i] = NULL;
    }

    mem_reserve_data.nr = 0;
    mem_reserve_data.size = 0;
}

static int size_show(struct seq_file *m, void *ptr)
{
    seq_printf(m, "%lu MB\n", mem_reserve_data.size);
    return 0;
}

static int size_open(struct inode *inode, struct file *file)
{
    return single_open(file, size_show, inode->i_private);
}

static ssize_t size_write(struct file *file, const char __user *buf,
              size_t count, loff_t *ppos)
{
    u64 bytes, megabytes, per_node_size;
    char *end, str[32];
    int node, i;

    if (count >= sizeof(str))
        return -EINVAL;

    if (copy_from_user(str, buf, count))
        return -EFAULT;
    str[count] = '\0';

    bytes = memparse(strstrip(str), &end);
    if (*end != '\0')
        return -EINVAL;

    if (bytes == 0) {
        free_all_mem();
        return count;
    }

    megabytes = round_up(bytes >> 20, 4);
    per_node_size = megabytes / num_online_nodes();

    for_each_online_node(node) {
        for (i = 0; i < per_node_size / 4; i++) {
            struct page *page;

            page = alloc_pages_node(node, GFP_KERNEL | __GFP_THISNODE, 10);
            if (!page) {
                pr_info("allocate memory fail\n");
                return -ENOMEM;
            }
            mem_reserve_data.pages[mem_reserve_data.nr++] = page;
            if (mem_reserve_data.nr >= ARRAY_SIZE(mem_reserve_data.pages))
                return -ENOMEM;
        }
    }

    mem_reserve_data.size += megabytes;

    return count;
}

static const struct file_operations size_fops = {
    .open        = size_open,
    .read        = seq_read,
    .write        = size_write,
    .llseek        = seq_lseek,
    .release    = single_release,
};

static __init int mem_reserve_init(void)
{
    struct proc_dir_entry *parent_dir;

    parent_dir = proc_mkdir("mem_reserve", NULL);
    if (!parent_dir)
        return -ENOMEM;

    if (!proc_create("size", S_IRUSR | S_IWUSR, parent_dir,
             &size_fops))
        goto remove_parent;

    return 0;
remove_parent:
    proc_remove(parent_dir);
    return -ENOMEM;
}

static __exit void mem_reserve_exit(void)
{
    free_all_mem();
    remove_proc_subtree("mem_reserve", NULL);
}

module_init(mem_reserve_init);
module_exit(mem_reserve_exit);
MODULE_LICENSE("GPL v2");

