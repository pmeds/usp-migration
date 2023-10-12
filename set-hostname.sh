#!/bin/bash

set -x
#Set hostname
echo "Set hostname"
hostnamectl set-hostname vm1.webmd-usc-poc.net

# DEFAULT IP FOR HOSTNAME
LOCALIP="127.0.0.1"

# Hostname to add/remove.
HOSTNAME="vm1.webmd-usc-poc.net"

IP="$(ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1 | sed -n 1p)"

#Add to /etc/hosts
echo "Add hostname to hosts file"
HOSTS_LINE_LOCAL="$LOCALIP\t$HOSTNAME"
HOSTS_LINE="$IP\t$HOSTNAME"

echo "$HOSTS_LINE_LOCAL" >> /etc/hosts
echo "$HOSTS_LINE" >> /etc/hosts
