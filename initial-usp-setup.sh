#!/bin/bash

# Uncomment line below to enable debugging
# set -x
# Choose release name and architecture (note: 'arm64' only available on 'focal-ports' and 'jammy-ports')
release="jammy"
arch="amd64"

# Set 'stable' or 'beta' repository as source
repo="stable"

# Add the Unified Streaming public key to your keyring
wget https://stable.apt.unified-streaming.com/unifiedstreaming.pub

# Adding the repo key through apt-key does not work as of ubuntu 22.04
# first dearmor the publick key
gpg --output unifiedstreaming.gpg --dearmor unifiedstreaming.pub

# Move the key to /etc/apt/keyrings/

sudo mv unifiedstreaming.gpg /etc/apt/keyrings/

# Add the Unified Streaming repository to your APT sources
echo "Updating sources for repositories"
echo "deb [arch=${arch} signed-by=/etc/apt/keyrings/unifiedstreaming.gpg] https://${repo}.apt.unified-streaming.com ${release} multiverse" |\
  sudo  tee /etc/apt/sources.list.d/unified-streaming.list

# Update your repositories
echo "Updating repositories"
sudo apt update

# downgrade pip if you are 23.xx otherwise you will get an error related to PEP 668

sudo pip install pip==22.3.1 --break-system-packages

# Update APT, and install mp4split, Apache and Unified Streaming webserver module
echo "Installing apache2 and unified-streaming Origin"
sudo apt-get install apache2 libapache2-mod-smooth-streaming

# Check if apache is working if it is not then quit the script
echo "Checking if Apache is running, if not start the servive"

if [ -f /var/run/apache2/apache2.pid ]
then
        echo "Apache2 process is running."
else
        echo "Apache2 process is NOT running."
        echo "Starting the process."
        sudo systemctl start apache2
        if [ -f /var/run/apache2/apache2.pid ]
        then
                echo "Process started successfully."

        else
                echo "Process starting failed, contact admin."
                exit 1
        fi
fi

ev=$(sudo a2query -M)
echo $ev
goodev="worker"
# Only run the mpm changes if apache is running otherwise you will get an error 
if [ -f /var/run/apache2/apache2.pid ]
then
  if [ $ev = $goodev ]
  then
    echo "MPM was already set to worker"
    exit 1
  else
    # Switch from event to worker MPM and enable headers and Unified Streaming module
    a2dismod mpm_event
    a2enmod mpm_worker headers mod_smooth_streaming

    # Disable default virtual host 
    a2dissite 000-default
   

    # Restart Apache
    sudo systemctl restart apache2.service
  fi
else
  echo "Apache 2 is not running"
  exit 1
fi


# Create a virtual host for Unified Origin, with IsmProxyPass configuration for S3
sudo cat << EOF > /etc/apache2/sites-available/origin-usp-poc.conf
UspLicenseKey /etc/usp-license.key

<VirtualHost *:80>
    # Note that the specified ServerName is not a Fully Qualified Domain Name (FQDN)
    # By default AWS dynamically allocates an instance's public hostname at (re)start
    # This makes the public hostname unsuitable as the ServerName of the VirtualHost
    # This is not a problem with one VirtualHost that matches all IPs ('*:80')
    # Because Apache will resolve all requests on port 80 to this VirtualHost
    # This is true whichever 'host' is signaled in the header of a request
    # Thus, a FQDN as ServerName has little importance for this test setup
    # Note that these considerations change if you would switch to HTTPS

    ServerName vm1.webmd-usc-poc.net
    DocumentRoot /var/www/html
    SSLProxyEngine on
    # Allow unconditional access to content hosted via virtual host
    <Directory />
      Require all granted
      Satisfy Any
    </Directory>

    SSLProxyEngine on
    # Activate Origin on virtual host
    <Location />
      UspHandleIsm on
      UspEnableSubreq on
    </Location>
     
    <Location /liveness>
      UspHandleIsm off
      UspEnableSubreq off
    </Location> 

    <Location /delivery/>
	ProxyPass http://0.0.0.0:8081/
	ProxyPassReverse http://0.0.0.0:8081/
    </Location>

    # Enable Origin to handle all requests that it supports
    AddHandler smooth-streaming.extensions .ism .isml .mp4

    # Available loglevels: trace8, ..., trace1, debug, info, notice, warn,
    # error, crit, alert, emerg.
    # It is also possible to configure the loglevel for particular
    # modules, e.g.
    #LogLevel info ssl:warn
    LogLevel debug
    ErrorLog /var/log/apache2/error.log
    CustomLog /var/log/apache2/access.log combined
    LogLevel proxy_http:trace4
    Header always set Access-Control-Allow-Headers "origin, range"
    Header always set Access-Control-Allow-Methods "GET, HEAD, OPTIONS"
    Header always set Access-Control-Allow-Origin "*"
    Header always set Access-Control-Expose-Headers "Server,range"
</VirtualHost>

EOF

# Create a virtual host for Unified Origin, with IsmProxyPass configuration for S3
sudo cat << EOF > /etc/apache2/sites-available/remote-storage.conf

<VirtualHost *:8081>
  ServerName unified-origin-backend
  SSLProxyEngine on
  LogFormat '%h %l %u %t "%r" %>s %b %D "%{Referer}i" "%{User-agent}i" "%{BALANCER_WORKER_NAME}e" ' log_format
  ErrorLog /var/log/apache2/error-proxy.log
  CustomLog /var/log/apache2/access-proxy.log combined
  LogLevel debug proxy_http:trace4
 
  

  <Location "/">
    UspHandleIsm on
    UspEnableSubreq on
    IsmProxyPass http://localhost:8081/load-balancer/
  </Location>

  <Location "/load-balancer/">
    ProxyPass "balancer://load-balancer/"
    ProxyPassReverse "balancer://load-balancer/"
  </Location>

  <Proxy "balancer://load-balancer/">
    ProxySet lbmethod=bybusyness failonstatus=403 failontimeout=On forcerecovery=Off nofailover=Off
    BalancerMember "http://localhost:8081/pmedrano-usp-poc2.us-ord-1.linodeobjects.com" connectiontimeout=5 timeout=5 ttl=600 keepalive=on retry=120 hcmethod=GET hcuri=LB-liveness.html hcinterval=30 hcpasses=1 hcfails=1
    BalancerMember "http://localhost:8081/pmedrano-usp-poc.us-east-1.linodeobjects.com" connectiontimeout=5 timeout=5 ttl=600 keepalive=on retry=120 hcmethod=GET hcuri=LB-liveness.html hcinterval=30 hcpasses=1 hcfails=1
  </Proxy>

  <Location "/pmedrano-usp-poc2.us-ord-1.linodeobjects.com">
   ProxyPass "https://pmedrano-usp-poc2.us-ord-1.linodeobjects.com/delivery"
   ProxyPassReverse "https://pmedrano-usp-poc2.us-ord-1.linodeobjects.com/delivery"
  </Location>

  <Proxy "https://pmedrano-usp-poc2.us-ord-1.linodeobjects.com">
    ProxySet connectiontimeout=5 timeout=5 ttl=300 keepalive=on retry=0 timeout=5 ttl=300
  </Proxy>

  <Location "/pmedrano-usp-poc.us-east-1.linodeobjects.com">
   ProxyPass "https://pmedrano-usp-poc.us-east-1.linodeobjects.com/delivery"
   ProxyPassReverse "https://pmedrano-usp-poc.us-east-1.linodeobjects.com/delivery"
  </Location>

  <Proxy "https://pmedrano-usp-poc.us-east-1.linodeobjects.com">
    ProxySet connectiontimeout=5 timeout=5 ttl=300 keepalive=on retry=0 timeout=5 ttl=300
  </Proxy>

</Virtualhost>

EOF

# Enable the virtual host that was just created and restart Apache
echo "Enabling additional apache proxy and load balancer modules"
sudo a2enmod proxy_http
sudo a2enmod proxy_hcheck
sudo a2enmod proxy_balancer
sudo a2enmod lbmethod_bybusyness


echo "Adding port 8081 to apache2 config"
sudo echo "Listen 8081" >> /etc/apache2/ports.conf
sudo a2ensite remote-storage > /dev/null 2>&1
echo "restarting apache"
sudo systemctl restart apache2.service

exit 0
