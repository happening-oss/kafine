#!/bin/sh

case $(uname) in
	Linux) IFADDR=$(ip route list | awk '/^default/ { print $9 }')
	       ;;
	    *) NET_IF=$(route get default | awk '/interface:/ { print $2 }')
	       IFADDR=$(ipconfig getifaddr "$NET_IF")
	       ;;
esac

echo "$IFADDR"
