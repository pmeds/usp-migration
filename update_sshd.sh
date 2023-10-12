#!/bin/bash

# Remove comment for debugging
#set -x

file="/etc/ssh/sshd_config"
param1="PubkeyAuthentication"
param2="PasswordAuthentication"


  if [ -f ${file} ]
  then
    /usr/bin/cp ${file} ${file}.1
  else
    /usr/bin/echo "File ${file} not found."
    exit 1
  fi

    /usr/bin/sed -i '/^'"${param1}"'/d' ${file}
    /usr/bin/sed -i '/^'"${param2}"'/d' ${file}
    /usr/bin/echo "All lines beginning with '${param1}' and '${param2}' were deleted from ${file}."

  /usr/bin/echo "${param1} yes" >> ${file}
  /usr/bin/echo "'${param1} yes' was added to ${file}."
  /usr/bin/echo "${param2} no" >> ${file}
  /usr/bin/echo "'${param2} no' was added to ${file}"

  # Reload sshd

  #/usr/bin/systemctl reload sshd.service
  #/usr/bin/echo "Run '/usr/bin/systemctl reload sshd.service'...OK"
