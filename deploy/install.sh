#!/bin/bash
# Installs the query API as a systemd service on Linux. Run as root from the
# directory holding the queryapi binary (queryapi.linux in the client bundle):
#   sudo ./deploy/install.sh            install or upgrade, then (re)start
#   sudo ./deploy/install.sh uninstall  stop and remove (keeps /etc/queryapi)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)

if [ "$(id -u)" -ne 0 ]; then
	echo "run as root (sudo $0 $*)" >&2
	exit 1
fi

if [ "${1:-}" = "uninstall" ]; then
	systemctl disable --now queryapi 2>/dev/null || true
	rm -f /etc/systemd/system/queryapi.service /usr/local/bin/queryapi
	systemctl daemon-reload
	echo "removed queryapi; settings kept in /etc/queryapi"
	exit 0
fi

bin=""
for b in "$root/queryapi.linux" "$root/queryapi"; do
	if [ -x "$b" ] && [ "$(head -c 4 "$b" | tail -c 3)" = "ELF" ]; then
		bin=$b
		break
	fi
done
if [ -z "$bin" ]; then
	echo "no Linux queryapi binary in $root (build with ./build.sh)" >&2
	exit 1
fi

id queryapi >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin queryapi

install -m 0755 "$bin" /usr/local/bin/queryapi
install -d -m 0750 -g queryapi /etc/queryapi
if [ ! -e /etc/queryapi/queryapi.env ]; then
	# Mode 0640: it may hold AEROSPIKE_PASSWORD
	install -m 0640 -g queryapi "$here/queryapi.env" /etc/queryapi/queryapi.env
	echo "wrote /etc/queryapi/queryapi.env; edit -hosts and -namespace there"
fi
install -m 0644 "$here/queryapi.service" /etc/systemd/system/queryapi.service
if [ -e "$root/README.md" ]; then
	install -D -m 0644 "$root/README.md" /usr/local/share/doc/queryapi/README.md
fi

systemctl daemon-reload
systemctl enable queryapi
systemctl restart queryapi
sleep 1
systemctl --no-pager status queryapi | head -5 || true
echo "logs: journalctl -u queryapi -f"
