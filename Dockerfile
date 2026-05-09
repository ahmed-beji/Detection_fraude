# 1. L'OS de base : Un Linux minimaliste avec Python 3.9
FROM python:3.9-slim

# 2. On définit notre dossier de travail dans le conteneur
WORKDIR /app

# ==========================================
# 3. OPTIMISATION DU CACHE (La signature du Pro)
# ==========================================
# On copie d'abord UNIQUEMENT le pyproject.toml. 
# Pourquoi ? Parce que si on modifie notre code Python plus tard, 
# Docker n'aura pas à retélécharger toutes les librairies Pandas/Requests.
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# ==========================================
# 4. COPIE DU CODE SOURCE
# ==========================================
# On copie notre dossier "src" local vers le dossier "src" du conteneur
COPY src/ src/

# (Note : On ne copie PAS le dossier "data". La donnée ne doit pas être 
# figée dans l'image, elle sera connectée de l'extérieur via un Volume -v)

# ==========================================
# 5. LA COMMANDE D'ALLUMAGE
# ==========================================
# Ce que la machine doit faire quand on appuie sur le bouton "ON"
CMD ["python3", "src/ingestion_api.py"]