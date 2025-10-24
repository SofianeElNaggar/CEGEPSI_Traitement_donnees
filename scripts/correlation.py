import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# === 1. Charger le CSV ===
# Remplace le nom du fichier par le tien, ex. "mesures.csv"
df = pd.read_csv("../rapports/moyennes_paliers.csv", sep=';')

# === 2. Supprimer la colonne Date ===
if 'Date' in df.columns:
    df = df.drop(columns=['Date'])

# === 3. Convertir toutes les colonnes en numériques (forcer le parsing) ===
# Cela gère les valeurs manquantes ou textes accidentels
df = df.apply(pd.to_numeric, errors='coerce')

# === 4. Calcul de la matrice de corrélation ===
corr = df.corr()

# === 5. Affichage de la heatmap ===
plt.figure(figsize=(12, 10))
sns.heatmap(corr, annot=True, fmt=".1f", cmap='coolwarm', square=True)
plt.title("Matrice de corrélation des paramètres mesurés")
plt.tight_layout()
plt.show()
