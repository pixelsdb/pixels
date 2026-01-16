CUR_DIR=$(pwd)
/home/whz/test/pixels/cpp/build/release/extension/pixels/pixels-cli/pixels-cli <<EOF
LOAD -o /data/9a3-01/clickbench/hits -t /data/9a3-01/clickbench/pixels-e0-fb -s ${CUR_DIR}/clickbench.schema -n 156250 -r \t -e 0 -p true
EOF
#./home/whz/test/pixels/cpp/build/release/extension/pixels/pixels-cli/pixels-cli <<EOF
#LOAD -o /data/9a3-01/clickbench/hits -t /data/9a3-01/clickbench/pixels-e1-fb -s ${CUR_DIR}/clickbench.schema -n 156250 -r \t -e 1 -p true
#EOF
#./home/whz/test/pixels/cpp/build/release/extension/pixels/pixels-cli/pixels-cli <<EOF
#LOAD -o /data/9a3-01/clickbench/hits -t /data/9a3-01/clickbench/pixels-e2-fb -s ${CUR_DIR}/clickbench.schema -n 156250 -r \t -e 2 -p true
#EOF

