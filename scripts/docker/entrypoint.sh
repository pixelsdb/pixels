#!/bin/bash
#
#color define
GREEN='\033[1;32m'
RED='\033[1;31m'
BLUE='\033[1;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'
SUFFIX=" * "


print_result() {
    echo -ne "\033[60G"
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}[  OK  ]${NC}"
    else
        echo -e "${RED}[FAILED]${NC}"
    fi
}

run_service() {
    local name="$1"
    local cmd="$2"
    local log="${3:-/dev/null}"

    echo -n "${SUFFIX}Start $name ... "
    if eval "$cmd" > "$log" 2>&1; then
        print_result 0
    else
        print_result 1
    fi
}

# [1] SSH
service ssh start

# [2] MySQL
service mysql start

# [3] Etcd 
echo -n "${SUFFIX}Start Etcd ... "
sh $HOME/opt/etcd/start-etcd.sh >/tmp/etcd.log 2>&1 &
ETCD_PID=$!

# watting for etcd start
until nc -z 127.0.0.1 2379; do sleep 1; done
print_result 0

# [4] Pixels
run_service "Pixels" "\$PIXELS_HOME/sbin/start-pixels.sh" "/tmp/pixels.log"

# [5] Trino
run_service "Trino" "\$HOME/opt/trino-server/bin/launcher start" "/tmp/trino.log"


echo ""
echo -e "${BLUE}=================================================${NC}"
echo -e "${GREEN}      Pixels is installed successfully!${NC}"
echo -e "${BLUE}=================================================${NC}"
echo ""
echo -e "${YELLOW}To explore Pixels, please try the following steps:${NC}"
echo ""
echo -e "  ${CYAN}# 1. Connect to Trino server:${NC}"
echo -e "  ${CYAN}./bin/trino --server localhost:8080 --catalog pixels${NC}"
echo ""
echo -e "  ${CYAN}# 2. Run SHOW SCHEMAS in trino-cli:${NC}"
echo -e "  ${CYAN}SHOW SCHEMAS;${NC}"
echo ""
echo -e "  ${CYAN}# Expected Output:${NC}"
cat <<EOF
                  Schema
            --------------------
            information_schema
            (1 row)
EOF
echo ""
echo -e "${BLUE}-------------------------------------------------${NC}"
echo -e "  ${YELLOW}More Information:${NC}"
echo -e "  - GitHub:  ${CYAN}https://github.com/pixelsdb/pixels${NC}"
echo -e "  - DeepWiki: ${CYAN}https://deepwiki.com/pixelsdb/pixels${NC}"
echo -e "${BLUE}-------------------------------------------------${NC}"
echo ""

# keep container alive through bash mode or etcd service
if [ -n "$1" ]; then
    exec "$@"
else
    wait $ETCD_PID
fi
