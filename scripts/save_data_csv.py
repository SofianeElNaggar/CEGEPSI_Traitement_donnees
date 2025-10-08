import os
import glob
import re
import pandas as pd

IGNORER_COLONNES = {
    "DroneEasting", "Ts", "Depth", "Altitude", "Heading",
    "PilotLat", "PilotLon", "DroneLat", "DroneLon",
    "DroneNorthing", "BatteryLevel", "Monotonic", "Date",
    "Temperature", "Temperature_2", "Temperature_3", "WaterTemp"
}

IGNORER_JOUR_VALEUR = {
    "2025-09-24": {"Dissolved Oxygen Concentration", "Dissolved Oxygen Saturation", "Oxygen Partial Pressure"},
}

PALIERS = {
    "1m":  (0.5, 2),
    "8m":  (6.5, 10),
    "15m": (13, 17)
}

def normalize_name(s: str) -> str:
    return re.sub(r'\W+', '', str(s)).lower()

def find_column(df: pd.DataFrame, target: str):
    tnorm = normalize_name(target)
    for col in df.columns:
        if normalize_name(col) == tnorm:
            return col
    return None

def extraire_datetime(file_path):
    dive_info = pd.read_excel(file_path, sheet_name="Dive info", header=None)
    dt_str = dive_info.iloc[3, 2]
    return pd.to_datetime(dt_str, dayfirst=True, errors="coerce")

def extraire_moyennes_par_palier(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name="Sensor data", header=0)
    except Exception as e:
        print(f"Impossible de lire 'Sensor data' dans {file_path} : {e}")
        return None

    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    cols = pd.Series(df.columns).astype(str)
    dup = cols.duplicated()
    cols = cols.where(~dup, cols + "_" + dup.cumsum().astype(str))
    df.columns = cols

    df = df.apply(pd.to_numeric, errors="coerce")

    depth_col = find_column(df, "Depth")
    if not depth_col:
        print(f"[{os.path.basename(file_path)}] Pas de colonne Depth")
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

    df_all = pd.concat(all_results, keys=[d["Date"].iloc[0] for d in all_results])
    df_all.index.names = ["Date", "Palier"]
    df_group = df_all.groupby(level=["Date", "Palier"]).mean()

    for date_str, cols in IGNORER_JOUR_VALEUR.items():
        try:
            jour = pd.to_datetime(date_str).normalize()
            for c in cols:
                matches = [col for col in df_group.columns if normalize_name(col) == normalize_name(c)]
                for m in matches:
                    df_group.loc[(jour, slice(None)), m] = pd.NA
        except Exception as e:
            print(f"Erreur exclusion {date_str}: {e}")

    # Supprimer les colonnes à ignorer
    ignore_norm = {normalize_name(c) for c in IGNORER_COLONNES}
    df_group = df_group[[c for c in df_group.columns if normalize_name(c) not in ignore_norm]]

    return df_group

def sauvegarder_csv(df_group, output_path="resultats_paliers.csv"):
    df_flat = df_group.unstack(level="Palier")
    df_flat.columns = [f"{col[0]}_{col[1]}" for col in df_flat.columns]
    df_flat.reset_index(inplace=True)
    df_flat.to_csv(output_path, index=False, sep=";", encoding="utf-8")
    print(f"Fichier CSV sauvegardé : {output_path}")

if __name__ == "__main__":
    dossier = "../rapports/rapports_xlsx/Q40"
    df_group = analyser_dossier_paliers(dossier)
    sauvegarder_csv(df_group, "../rapports/moyennes_paliers.csv")
