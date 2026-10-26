terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "eu-west-3" # Paris

  default_tags {
    tags = {
      Project     = "Crypto-Whale-ETL"
      Environment = "Production"
      ManagedBy   = "Terraform"
    }
  }
}

# 1. Génération d'une clé SSH dédiée pour GitHub Actions
resource "tls_private_key" "deploy_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "etl_ec2_key" {
  key_name   = "crypto-etl-deploy-key"
  public_key = tls_private_key.deploy_key.public_key_openssh
}

# 2. Récupération de l'image officielle Ubuntu 22.04 LTS
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# 3. Security Group (Forteresse réseau)
resource "aws_security_group" "etl_sg" {
  name        = "crypto-etl-sg"
  # Apostrophe retirée ici :
  description = "Regles strictes pour ETL (SSH, Metabase, Redpanda Console)"

  # ... le reste des ports reste strictement identique

  # Port 22 : Réservé à l'agent GitHub Actions pour le déploiement
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] 
  }

  # Port 3000 : Accessible au public pour voir le Dashboard Metabase
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Port 8080 : Accessible au public pour voir la Redpanda Console (Monitoring Kafka)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Trafic sortant illimité (Nécessaire pour l'API Binance et télécharger des packages)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# 4. Provisionnement du Serveur EC2
resource "aws_instance" "etl_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t2.micro" # Éligible au Free Tier
  key_name      = aws_key_pair.etl_ec2_key.key_name

  vpc_security_group_ids = [aws_security_group.etl_sg.id]

  # Optimisation du disque dur pour stocker la base de données
  root_block_device {
    volume_size = 15 # Go
    volume_type = "gp3"
  }

  # Script de démarrage d'infrastructure (SWAP + Docker)
  user_data = <<-EOF
              #!/bin/bash
              
              # --- CRÉATION DU SWAP (2 Go) ---
              # Vital pour faire tourner Java (Metabase) et Redpanda sur 1 Go de RAM
              fallocate -l 2G /swapfile
              chmod 600 /swapfile
              mkswap /swapfile
              swapon /swapfile
              echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab

              # --- INSTALLATION DES DÉPENDANCES ---
              apt-get update -y
              apt-get install -y ca-certificates curl gnupg git

              # --- INSTALLATION OFFICIELLE DE DOCKER ---
              install -m 0755 -d /etc/apt/keyrings
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
              chmod a+r /etc/apt/keyrings/docker.gpg
              echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
              
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

              # --- PERMISSIONS ---
              usermod -aG docker ubuntu
              EOF
}