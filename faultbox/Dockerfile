# Joins a YugabyteDB container's network namespace (docker run --net
# container:yb1 --cap-add NET_ADMIN) to drop packets or add latency there,
# without touching the database image.
FROM alpine:3.22
RUN apk add --no-cache iptables iproute2
