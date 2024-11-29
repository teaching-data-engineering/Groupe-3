import secrets
from fastapi import HTTPException, Header

# Liste des tokens valides
VALID_TOKENS = set()  # Dictionnaire pour stocker les tokens valides

def generate_token():
    """
    Génère un token aléatoire sécurisé.
    """
    return secrets.token_hex(16)  # 32 caractères hexadécimaux

def add_token():
    """
    Ajoute un nouveau token valide à la liste des tokens.
    """
    token = generate_token()
    VALID_TOKENS.add(token)  # Ajoute le token généré à la liste des tokens valides
    return token  # Vous pouvez renvoyer le token pour l'utiliser ailleurs

def verify_token(x_token: str = Header(...)):
    """
    Vérifie que le token transmis dans les headers est valide.
    """
    if x_token not in VALID_TOKENS:
        raise HTTPException(
            status_code=403,
            detail="Token invalide ou manquant"
        )