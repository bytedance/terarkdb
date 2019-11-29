## Key Path Analysis Tool

### Features

- Find key in target SST file by Get(Key) and Seek(Key), print all relative information and code path
- Find key in target database dir
  - If there's a manifest file, then use it
  - If there's no manifest file, try all SST files one by one

### Scenario
- When database tells a key cannot be found but you are sure that it should be there.


### Compile
- Compile TerarkDB first and make sure `output/` dir is not empty
- Use `build.sh` to build key_path_analysis tool

### Usage

```
    # list all keys
    env TerarkZipTable_localTempDir=~/ ./key_path_analysis listkeys ~/021649.sst
```