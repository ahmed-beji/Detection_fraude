output "EC2_HOST" {
  value       = aws_instance.etl_server.public_ip
  description = "L'adresse IP publique de votre instance EC2"
}

output "EC2_SSH_KEY" {
  value       = tls_private_key.deploy_key.private_key_pem
  description = "La clé privée SSH brute pour GitHub Actions"
  sensitive   = true
}