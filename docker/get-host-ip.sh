#!/bin/sh

# TODO: This is Darwin-only; write something for Linux.
NET_IF=$(route get default | grep 'interface:' | awk '{print $2}')
IFADDR=$(ipconfig getifaddr $NET_IF)

echo "$IFADDR"
