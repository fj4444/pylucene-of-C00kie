# !/bin/sh

apt upgrade
apt update
apt install -y nginx

mv ./conf.d/pylucene.conf /etc/nginx/conf.d/

nginx -t
service nginx start
nginx -s reload
service nginx restart
