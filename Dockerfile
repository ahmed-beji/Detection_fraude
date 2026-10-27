# ==========================================
# 1. L'OS de base : Aligné avec votre CI/CD (Python 3.11)
# ==========================================
FROM python:3.11-slim

# Empêche Python de créer des fichiers .pyc (gain d'espace) 
# et force l'affichage immédiat des logs dans la console Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ==========================================
# 2. On définit notre dossier de travail dans le conteneur
# ==========================================
WORKDIR /app

# ==========================================
# 3. DÉPENDANCES SYSTÈME (Crucial pour Kafka et Postgres)
# ==========================================
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ==========================================
# 4. OPTIMISATION DU CACHE (Dépendances Python)
# ==========================================
# On copie uniquement la définition des paquets en premier
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .
# ==========================================
# 6. SÉCURITÉ : UTILISATEUR NON-ROOT (Standard DevOps)
# ==========================================
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# ==========================================
# 7. LA COMMANDE D'ALLUMAGE (Neutre)
# ==========================================
# On ne met plus de fichier spécifique ici. 
# C'est le docker-compose.yml qui dira à un conteneur de lancer le Producer 
# et à l'autre de lancer le Consumer via la directive "command:".
CMD ["python", "--version"]