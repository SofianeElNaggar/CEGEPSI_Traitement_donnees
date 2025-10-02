import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt

# ------------ PARAMÈTRES UTILISATEUR ------------
IGNORER_COLONNES = {
    "DroneEasting", "Ts", "Depth", "Altitude", "Heading",
    "PilotLat", "PilotLon", "DroneLat", "DroneLon",
    "DroneNorthing", "BatteryLevel", "Monotonic", "Date"
}
PALIERS = {
    "1m":  (0, 2),
    "8m":  (6.5, 10),
    "15m": (14, 17)
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
    dive_info = pd.read_excel(file_path, sheet_name="Dive info", header=None)
    dt_str = dive_info.iloc[3, 2]
    return pd.to_datetime(dt_str, dayfirst=True, errors="coerce")

def lire_donnees_continues(file_path):
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
    return df


def collecter_mesures_par_palier(dossier):
    fichiers = glob.glob(os.path.join(dossier, "*.xlsx"))
    paliers_data = {p: [] for p in PALIERS.keys()}

    for f in sorted(fichiers):
        if os.path.basename(f).startswith("~$"):
            continue
        dt = extraire_datetime(f)
        if pd.isna(dt):
            continue
        df = lire_donnees_continues(f)
        if df is None or df.empty:
            continue
        depth_col = find_column(df, "Depth")
        if not depth_col:
            continue

        for palier, (low, high) in PALIERS.items():
            subset = df[(df[depth_col] >= low) & (df[depth_col] <= high)].copy()
            if subset.empty:
                continue
            # ➜ Au lieu de recréer un datetime index, on garde un compteur
            subset.reset_index(drop=True, inplace=True)
            paliers_data[palier].append(subset)

    for palier in paliers_data:
        if paliers_data[palier]:
            # Concaténation continue (l’index reste un compteur croissant)
            paliers_data[palier] = pd.concat(paliers_data[palier], ignore_index=True)
        else:
            paliers_data[palier] = pd.DataFrame()

    return paliers_data


def tracer_mesures_subplots(paliers_data):
    for palier, dfp in paliers_data.items():
        if dfp.empty:
            print(f"[INFO] Aucun point pour le palier {palier}")
            continue

        sensors = [c for c in dfp.columns
                   if normalize_name(c) not in {normalize_name(x) for x in IGNORER_COLONNES}]
        if not sensors:
            continue

        n = len(sensors)
        fig, axes = plt.subplots(n, 1, figsize=(12, 3*n), sharex=True)
        if n == 1:
            axes = [axes]

        fig.suptitle(f"Mesures continues – Palier {palier}", fontsize=14)

        # Palette de couleurs matplotlib
        colors = plt.cm.tab10.colors

        for i, (ax, col) in enumerate(zip(axes, sensors)):
            # Supprimer NaN + valeurs à 0
            series = dfp[col].replace(0, pd.NA).dropna()
            if series.empty:
                continue
            ax.plot(series.index, series.values, color=colors[i % len(colors)], lw=1.5)
            ax.set_ylabel(col)
            ax.grid(True)

        axes[-1].set_xlabel("Échantillons (continu)")
        plt.tight_layout(rect=[0, 0, 1, 0.97])
        plt.show()


if __name__ == "__main__":
    dossier = "../rapports/rapports_xlsx/Q41"
    paliers_data = collecter_mesures_par_palier(dossier)
    tracer_mesures_subplots(paliers_data)

