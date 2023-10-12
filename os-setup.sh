#!/bin/bash

# Add admin user
echo "Adding admin user"
adduser admin

# Add admin user to sudoer
echo "Adding admin user"
usermod -aG sudo admin

# Create .ssh for admin
echo "Create ssh directory"
mkdir /home/admin/.ssh

# Give permissions to admin user

chmod 700 /home/admin/.ssh

# Copy authorized keys to use the same ssh key as root
echo "copy authorized keys to admin user"
cp /root/.ssh/authorized_keys /home/admin/.ssh/authorized_keys

# Change ownership of directory

chown -R admin:admin /home/admin/.ssh

# Update sshd_config to only allow ssh access
echo "Modify sshd to only allow ssh access. Removing root access from sshd must be done manual and until the setup is complete"
sh update_sshd.sh

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
