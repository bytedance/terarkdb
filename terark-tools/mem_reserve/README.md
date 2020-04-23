### Install
make && make install

### Remove
make remove

### Usage

#### Take Memory
echo 100g > /proc/mem_reserve/size

``` Note that you should free memory before any change ```

#### Free Memory
echo 0 > /proc/mem_reserve/size

### NOTES

- must install under `root` privileges
- `insmod` command must be valid. 
- If `insmod Command not found occur`, please make sure your `/sbin/insmod` is there and install the tool manually:
  - `sudo /sbin/insmod mem_reserve.ko`
