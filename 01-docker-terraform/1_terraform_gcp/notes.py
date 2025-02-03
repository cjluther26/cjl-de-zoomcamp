###############################
####### TERRAFORM NOTES #######
###############################

# See ~/terrademo for notes!




###########################
####### GCP NOTES #########
###########################

# https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=15

# Creating an SSH key for GCP: https://cloud.google.com/compute/docs/connect/create-ssh-keys
# Notes:
    # 1. Go to .ssh directory
    # 2. Run: ssh-keygen -t rsa -f ~/.ssh/{KEY_FILENAME} -C {USERNAME}
    # 3. Copy the public key to the GCP project metadata:
    #      - copy the contents of ~/.ssh/{KEY_FILENAME}.pub (`cat gcp.pub`) into the "SSH Keys" section of the project metadata.
    # 4. Connect to the instance using the private key:
    #     - Get the external IP address of the VM instance from the GCP console.
    #     - Go to your home directory and, using Terminal, type: `ssh -i ~/.ssh/{KEY_FILENAME} {USERNAME}@{EXTERNAL_IP_ADDRESS}`

# Logging into a VM instance:
    # 1. Set project: `gcloud config set project {PROJECT_ID}`
    # 2. Connect to the instance: `gcloud compute ssh {INSTANCE_NAME}`

# Creating a `config` file (which can be used to SSH into a VM instance):
    # 1. Go to .ssh directory
    # 2. Run: touch config
    # 3. Add the following to the config file:
        # Host {INSTANCE_NAME}
        #     HostName {EXTERNAL_IP_ADDRESS}
        #     User {USERNAME}
        #     IdentityFile ~/.ssh/{KEY_FILENAME}
    # 4. Save the config file
    # 5. Go to home directory and connect to the instance: `ssh {INSTANCE_NAME}`


# Configuring a VM instance:
    # 1. Download anaconda: 
    #   - Go to https://www.anaconda.com/download
    #   - Copy the link for the downloader of the version you want (Linux 64-bit)
    #   - In the terminal, run: wget {LINK}
    #   - Run: bash {DOWNLOADER_FILENAME}

    # 2. Install Docker:
    #   - Run: sudo apt-get install docker.io
    #   - Follow instructions here to allow Docker permissions: https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md
    #     - Make sure you logout of the VM instance and log back in to apply the group changes. (`^ + d`, then `ssh {INSTANCE_NAME}`)

    # 3. Install Docker Compose:
    #   - See ~25:00 of video!!! (we altered the PATH in the .bashrc file, too)
    # - cjluther26@de-zoomcamp:~/data-engineering-zoomcamp/01-docker-terraform/2_docker_sql$ docker-compose up -d


    # 4. Install Pgcli
    #   - Open a new terminal window (i.e. one that is not running docker-compose)
    #   - Run: pip install pgcli
    #   - Run: pip uninstall pgcli
    #   - Run: conda install -c conda-forge pgcli

    # 5. Install Terraform:
    #   - Get link: https://releases.hashicorp.com/terraform/1.10.4/terraform_1.10.4_freebsd_amd64.zip
    #   - Go to `bin` directory (where docker-compose is) and run: wget {LINK}
    #   - Run: `sudo apt-get install unzip``
    #   - Run: unzip {DOWNLOADER_FILENAME}



    # Forward port (in VS Code):
    #   - Make sure you are IN THE VS CODE WINDOW WITH REMOTE SSH CONNECTION!

    # Running Jupyter Notebook: `jupyter notebook`
    #   - This gives a link (with port :8888q) to open in the browser (after port forwarding)
    #  I SKIPPED THE JUPYTER NOTEBOOK DATA UPLOAD STUFF




    # SFTP Google Credentials to VM:


    # Close VM instance:
    #   - `sudo shutdown now` (or use the GCP console)

    # Reopen VM instance:
    #   - Use the GCP console to start the instance
    #   - Edit the `config` file in the `.ssh` directory to update the external IP address
    #       - `nano .ssh/config`, save, then `ssh {INSTANCE_NAME}`
    #       - You'll have everything you installed before (but would have to do things like `docker compose up -d` again!)