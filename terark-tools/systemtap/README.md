## SystemTap Scripts


### 0. Compile

Compile TerarkDB with `RelWithDebug` or you will have to use arg indicators(e.g. $argN) instead of using variable names(e.g. $nbytes) in your probing scripts.

### 1. Confirm target function is exist

#### user space functions
`sudo stap -L 'process.function("*Sync*")' -x [pid]`

#### kernel space functions
`sudo stap -L 'kernel.function("*sync_file_range*")' -x [pid]`


### 2. Start Probing

`sudo stap -g probe_script.stp -x [pid]`


### 3. Command Line probling

`sudo stap -e 'probe process.function("RangeSync") {printf("%s\n", $$parms$)}' -x [pid]`

the `$$parms$` means to print all local variables inside target function.
