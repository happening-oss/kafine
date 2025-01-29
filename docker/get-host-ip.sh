#!/bin/sh

case $(uname) in
	Linux) NET_IF=$(route | awk '/default/ { print $NF }')
	       IFADDR=$(ifconfig "$NET_IF" | awk '$1 == "inet" { print $2 }')
	       ;;
	    *) NET_IF=$(route get default | awk '/interface:/ {print $2}')
	       IFADDR=$(ipconfig getifaddr $NET_IF)
	       ;;
esac

echo "$IFADDR"
