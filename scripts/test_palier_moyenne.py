import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ------------ PARAMÈTRES UTILISATEUR ------------
IGNORER_COLONNES = {
    "DroneEasting", "Ts", "Depth", "Altitude", "Heading",
    "PilotLat", "PilotLon", "DroneLat", "DroneLon",
    "DroneNorthing", "BatteryLevel", "Monotonic", "Date",
    "Temperature", "Temperature_2", "Temperature_3", "WaterTemp"
}

#    Liste des mesures à ignorer pour une date donnée
#    Format : { "AAAA-MM-JJ" : {"NomExactColonne1", "NomExactColonne2", ...} }
IGNORER_JOUR_VALEUR = {
    "2025-09-24": {"Dissolved Oxygen Concentration", "Dissolved Oxygen Saturation", "Oxygen Partial Pressure"},
}

PALIERS = {
    "1m":  (0, 2.5),
    "8m":  (6.5, 10),
    "15m": (13, 17)
}
# -------------------------------------------------

def normalize_name(s: str) -> str:
    return re.sub(r'\W+', '', str(s)).lower()

def find_column(df: pd.DataFrame, target: str):
    tnorm = normalize_name(target)
    for col in df.columns:
        if normalize_name(col) == tnorm:
            return col
    return None

def extraire_datetime(file_path):
    """Retourne un datetime complet (date + heure) depuis Dive info (cellule (3,3))."""
    dive_info = pd.read_excel(file_path, sheet_name="Dive info", header=None)
    dt_str = dive_info.iloc[3, 2]
    return pd.to_datetime(dt_str, dayfirst=True, errors="coerce")

def extraire_moyennes_par_palier(file_path):
    """Retourne DataFrame indexé par palier avec moyennes des capteurs."""
    try:
        df = pd.read_excel(file_path, sheet_name="Sensor data", header=0)
    except Exception as e:
        print(f"Impossible de lire 'Sensor data' dans {file_path} : {e}")
        return None

    # Nettoyage des en-têtes
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Colonnes uniques
    cols = pd.Series(df.columns).astype(str)
    dup = cols.duplicated()
    cols = cols.where(~dup, cols + "_" + dup.cumsum().astype(str))
    df.columns = cols

    # Conversion en numérique
    df = df.apply(pd.to_numeric, errors="coerce")

    depth_col = find_column(df, "Depth")
    if not depth_col:
        print(f"[{os.path.basename(file_path)}] Pas de colonne Depth")
        return None

    mask = pd.Series(True, index=df.index)
    df = df[mask]
    if df.empty:
        return None

    result = {}
    for palier, (low, high) in PALIERS.items():
        subset = df[(df[depth_col] >= low) & (df[depth_col] <= high)]
        if subset.empty:
            continue
        result[palier] = subset.mean(skipna=True)

    if not result:
        return None
    return pd.DataFrame(result).T

def analyser_dossier_paliers(dossier):
    fichiers = glob.glob(os.path.join(dossier, "*.xlsx"))
    all_results = []

    for f in sorted(fichiers):
        if os.path.basename(f).startswith("~$"):
            continue
        dt = extraire_datetime(f)
        if pd.isna(dt):
            print(f"[{os.path.basename(f)}] Date/heure introuvable")
            continue

        df_palier = extraire_moyennes_par_palier(f)
        if df_palier is not None:
            df_palier["Date"] = dt.normalize()
            all_results.append(df_palier)

    if not all_results:
        raise ValueError("Aucun fichier valide")

    # Concatène avec MultiIndex (Date, Palier)
    df_all = pd.concat(all_results, keys=[d["Date"].iloc[0] for d in all_results])
    df_all.index.names = ["Date", "Palier"]

    # Moyenne par JOUR et Palier
    df_group = df_all.groupby(level=["Date", "Palier"]).mean()

    # Application des exclusions spécifiques (jour + colonne)
    for date_str, cols in IGNORER_JOUR_VALEUR.items():
        try:
            jour = pd.to_datetime(date_str).normalize()
            for c in cols:
                # Normalisation des noms de colonnes pour robustesse
                matches = [col for col in df_group.columns
                           if normalize_name(col) == normalize_name(c)]
                for m in matches:
                    df_group.loc[(jour, slice(None)), m] = pd.NA
        except Exception as e:
            print(f"Erreur exclusion {date_str}: {e}")

    return df_group

def tracer_evolution_paliers(df_group):
    sensors = [
        c for c in df_group.columns
        if normalize_name(c) not in {normalize_name(x) for x in IGNORER_COLONNES}
    ]

    for col in sensors:
        plt.figure(figsize=(10, 5))
        for palier in df_group.index.levels[1]:
            series = df_group.xs(palier, level="Palier")[col].dropna()
            if series.empty:
                continue
            plt.plot(series.index, series.values, marker="o", linestyle="-", label=palier)

        plt.title(f"Évolution de {col} par palier")
        plt.xlabel("Date")
        plt.ylabel(col)
        plt.grid(True)
        plt.legend(title="Palier")
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%m-%Y"))
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    dossier = "../rapports/rapports_xlsx/Q40"
    df_group = analyser_dossier_paliers(dossier)
    print(df_group)
    tracer_evolution_paliers(df_group)
