#!/bin/sh

#nginx setup
sh ./nginx_setup.sh

# loop
echo start_container
python3 main.py 
# python3 server.py | tee ./server.log
# python3 main.py | tee ./server.log
# python3 FSM.py | tee ./server.log
# tail -f /dev/null
