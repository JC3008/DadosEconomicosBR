# Use the official Ubuntu base image
FROM ubuntu:latest

# Set non-interactive mode during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install necessary packages
RUN apt-get update && \
    apt-get install -y curl unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Terraform
RUN curl -fsSL -o /tmp/terraform.zip https://releases.hashicorp.com/terraform/1.1.0/terraform_1.1.0_linux_amd64.zip && \
    unzip /tmp/terraform.zip -d /usr/local/bin/ && \
    rm /tmp/terraform.zip 
    # apt update  \
    # apt install sudo  \
    # curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    # unzip awscliv2.zip \
    # sudo ./aws/install

# /usr/local/bin/terraform
# Set the working directory
# RUN mkdir ./app

# WORKDIR /app

# Entry point
CMD ["bash"]
