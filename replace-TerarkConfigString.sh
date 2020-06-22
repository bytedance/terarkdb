
# sample usage:
#
#   sh replace-TerarkConfigString.sh sample-TerarkEnv.sh mysql.cnf
#
< "$1" sed 's/export[ \t][\t]*//' | tr '\r\n' ';' | awk 'BEGIN{getline x < "/dev/stdin"}/%TerarkConfigString%/{gsub("%TerarkConfigString%",x)}1' "$2" > "/tmp/$2.tmp"
mv "/tmp/$2.tmp" "$2"

