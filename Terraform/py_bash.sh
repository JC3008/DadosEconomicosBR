#!/bin/bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
unzip awscliv2.zip && \
sudo ./aws/install && \
aws configure --profile terraform
# export AWS_PROFILE=terraform
# # python 3 installed with these steps
# apt update && \
# apt install sudo  && \
# sudo apt update  && \
# sudo apt install -y python3 && \
# sudo apt install -y nano && \
# sudo apt install -y git && \
# # # install python3.9
# sudo apt install -y build-essential libssl-dev zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libreadline-dev libffi-dev wget libsqlite3-dev libbz2-dev && \
# wget https://www.python.org/ftp/python/3.9.7/Python-3.9.7.tar.xz && \
# tar -xf Python-3.9.7.tar.xz && \
# cd Python-3.9.7  && \
# ./configure --enable-optimizations
# make -j $(nproc) && \
# sudo make altinstall 
# export PATH="/usr/local/bin/python3:/usr/bin/git:/usr/local/bin/python3.9:$PATH"

# # aws configure
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
# aws configure --profile terraform
# export AWS_PROFILE=terraform

