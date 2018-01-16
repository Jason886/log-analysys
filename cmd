./log_query_tool_pure.py -t '["2018-01-09 00:00:00","2018-01-15 00:00:00"]' -q '{"est":29}' -o 1 > 0109-0115_est29.txt

./import_est29 -t est29 -f 0109-0115_est29.txt

#/usr/local/opt/mysql55/bin/mysql -h10.0.200.15 -uroot -proot donli
/usr/local/mysql56/bin/mysql -h10.0.200.15 -uroot -proot donli
