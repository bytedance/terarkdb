
# sample usage:
#
#   sh replace-TerarkConfigString.sh sample-TerarkEnv.sh mysql.cnf
#
< "$1" sed 's/export[ \t][\t]*//' | tr '\r\n' ';' | awk 'BEGIN{getline x < "/dev/stdin"}{gsub("%TerarkConfigString%",x)}1' "$2"

