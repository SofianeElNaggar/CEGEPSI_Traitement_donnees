import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def normalize_name(s: str) -> str:
    """Normalise un nom de colonne pour comparaison (supprime caractères non alnum, lowercase)."""
    return re.sub(r'\W+', '', str(s)).lower()

def find_column(df: pd.DataFrame, target: str):
    """Retourne le nom de la colonne dans df correspondant au target (tolérant), ou None."""
    tnorm = normalize_name(target)
    for col in df.columns:
        if normalize_name(col) == tnorm:
            return col
    return None

def extraire_date(file_path):
    """Extrait la date depuis la feuille 'Dive info' (ligne 3 colonne 3 d'après l'exemple)."""
    dive_info = pd.read_excel(file_path, sheet_name="Dive info", header=None)
    date_str = dive_info.iloc[3, 2]
    print(date_str)
    return pd.to_datetime(date_str, dayfirst=True)

def extraire_moyennes(file_path):
    """
    Lit 'Sensor data', nettoie en-têtes, rend colonnes uniques, applique le filtrage dynamique
    et retourne un dictionnaire des moyennes (skipna=True).
    """
    try:
        df = pd.read_excel(file_path, sheet_name="Sensor data", header=0)
    except Exception as e:
        print(f"Impossible de lire 'Sensor data' dans {file_path} : {e}")
        return {}

    # La première ligne contient les vrais en-têtes d'après ton format
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Rendre les colonnes uniques (append _1, _2... si besoin)
    cols = pd.Series(df.columns).astype(str)
    dup = cols.duplicated()
    cols = cols.where(~dup, cols + "_" + dup.cumsum().astype(str))
    df.columns = cols

    # Conversion en numérique (les colonnes non-numériques deviendront NaN)
    df = df.apply(pd.to_numeric, errors="coerce")

    # Colonnes à utiliser pour filtrer les lignes invalides
    required_targets = ["BatteryLevel", "DroneLat", "DroneLon", "Depth", "WaterTemp", "PilotLat", "PilotLon"]

    # Construire un masque initial True (on ne supprime rien par défaut)
    mask = pd.Series(True, index=df.index)

    # Pour chaque target, si colonne trouvée => appliquer le test (non-NaN ET != 0)
    found_any = False
    for t in required_targets:
        col = find_column(df, t)
        if col:
            found_any = True
            mask &= df[col].notna() & (df[col] != 0)
        else:
            # colonne absente : on ignore ce critère (ne modifie pas le mask)
            pass

    # Si aucune des colonnes filtres n'a été trouvée, on ne filtre pas (mask reste True)
    df_filtered = df[mask]

    if df_filtered.empty:
        print(f"Aucune donnée valide après filtrage dans {os.path.basename(file_path)}")
        return {}

    # Moyennes en ignorant NaN
    means = df_filtered.mean(skipna=True)

    # convertir en dict pour faciliter la construction du DataFrame global
    return means.to_dict()

def analyser_dossier(dossier):
    """Parcourt tous les fichiers xlsx et construit un DataFrame de moyennes indexé par Date."""
    fichiers = glob.glob(os.path.join(dossier, "*.xlsx"))
    resultats = []

    for f in sorted(fichiers):
        # Ignorer fichiers temporaires Excel (~$)
        if os.path.basename(f).startswith("~$"):
            continue

        try:
            date = extraire_date(f)
        except Exception as e:
            print(f"Impossible d'extraire la date de {f} : {e}")
            continue

        try:
            moyennes = extraire_moyennes(f)
            if moyennes:  # uniquement si on a des résultats valides
                moyennes["Date"] = date
                resultats.append(moyennes)
        except Exception as e:
            print(f"Erreur lors du traitement de {f} : {e}")

    if not resultats:
        raise ValueError("Aucun fichier valide n'a été traité dans le dossier.")

    df_res = pd.DataFrame(resultats).set_index("Date").sort_index()
    return df_res

def tracer_evolution(df_res):
    """Trace un graphique par capteur en ignorant les NaN (points manquants)."""
    for col in df_res.columns:
        series = df_res[col].dropna()
        if series.empty:
            print(f"[Trace] Colonne '{col}' sans données valides -> ignorée.")
            continue

        plt.figure(figsize=(10, 5))
        plt.plot(series.index, series.values, marker="o", linestyle="-")
        plt.title(f"Évolution de {col}")
        plt.xlabel("Date de plongée")
        plt.ylabel(col)
        plt.grid(True)

        # Formatage de l'axe temporel (écarts proportionnels aux distances réelles)
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%m-%Y"))
        plt.gcf().autofmt_xdate()

        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    # ← change ce chemin vers ton dossier de rapports_xlsx
    dossier = "../rapports/rapports_xlsx/Q40"

    df_res = analyser_dossier(dossier)
    print("Tableau des moyennes par date :")
    print(df_res)

    tracer_evolution(df_res)
