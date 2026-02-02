import os
import glob
import re
import pandas as pd

# Colonnes à ignorer
IGNORER_COLONNES = {
    "DroneEasting", "Ts", "Depth", "Altitude", "Heading",
    "PilotLat", "PilotLon", "DroneLat", "DroneLon",
    "DroneNorthing", "BatteryLevel", "Monotonic", "Date",
    "WaterTemp", "External Voltage"
}

# Pour certains jours, ignorer certaines valeurs (nom exact des mesures, comparé en normalisé)
IGNORER_JOUR_VALEUR = {
    "2025-09-24": {"Dissolved Oxygen Concentration", "Dissolved Oxygen Saturation", "Oxygen Partial Pressure"},
}

# bornes en m des paliers
PALIERS = {
    "1m":  (0, 2.5),
    "8m":  (6.5, 10),
    "15m": (13, 17)
}

# mapping palier -> valeur numérique demandée pour la colonne Profondeur
PALIERS_VALEUR = {
    "1m": 1,
    "8m": 8,
    "15m": 15
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
    """
    Lit la feuille 'Dive info' et récupère la datetime
    """
    try:
        dive_info = pd.read_excel(file_path, sheet_name="Dive info", header=None)
        dt_str = dive_info.iloc[3, 2]
        return pd.to_datetime(dt_str, dayfirst=True, errors="coerce")
    except Exception as e:
        print(f"Impossible de lire 'Dive info' dans {file_path} : {e}")
        return pd.NaT

def extraire_moyennes_par_palier(file_path):
    """
    Lit la feuille 'Sensor data', nettoie, et calcule la moyenne par palier.
    Retourne un DataFrame indexé par nom de palier (ex: '1m','8m','15m') avec colonnes variables mesurées.
    """
    try:
        df = pd.read_excel(file_path, sheet_name="Sensor data", header=0)
    except Exception as e:
        print(f"Impossible de lire 'Sensor data' dans {file_path} : {e}")
        return None

    # Première ligne contient parfois les noms réels -> replacer colonnes puis supprimer ligne
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Gérer colonnes dupliquées: suffixer en cas de doublons
    cols = pd.Series(df.columns).astype(str)
    dup = cols.duplicated()
    cols = cols.where(~dup, cols + "_" + dup.cumsum().astype(str))
    df.columns = cols

    # Forcer conversion numérique pour permettre moyennes
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
        # moyenne par colonne pour ce palier
        result[palier] = subset.mean(skipna=True)

    if not result:
        return None

    df_palier = pd.DataFrame(result).T  # index = palier
    return df_palier

def _last_temperature_column(cols):
    """
    Retourne le nom de la colonne 'temperature' à garder.
    Reconnaît: Temperature, Temperature_2, Temperature 3, Temperature-10, etc.
    Si aucune colonne temperature trouvée -> retourne None.
    """
    candidates = []
    for c in cols:
        # normaliser seulement pour le matching: enlever caractères non alphanumériques et minuscules
        norm = re.sub(r'\W+', '', str(c)).lower()
        m = re.match(r'^temperature(\d*)$', norm)
        if m:
            idx = int(m.group(1)) if m.group(1) else 0
            candidates.append((idx, c))
    if not candidates:
        return None
    # choisir celui avec l'indice le plus grand (si plusieurs mêmes indices, on prend le dernier apparu)
    candidates.sort(key=lambda x: (x[0],))  # tri stable par idx
    return candidates[-1][1]

def analyser_dossier_paliers(dossier):
    """
    Parcourt les fichiers xlsx du dossier, extrait les moyennes par palier et retourne un DataFrame 'long'
    colonnes: Date, Profondeur, <mesures...>
    """
    fichiers = glob.glob(os.path.join(dossier, "*.xlsx"))
    all_rows = []

    for f in sorted(fichiers):
        if os.path.basename(f).startswith("~$"):
            continue
        dt = extraire_datetime(f)
        if pd.isna(dt):
            print(f"[{os.path.basename(f)}] Date/heure introuvable")
            continue

        df_palier = extraire_moyennes_par_palier(f)
        if df_palier is None:
            continue

        # convertir en format long: index palier -> colonne 'Palier'
        df_palier = df_palier.reset_index().rename(columns={"index": "Palier"})

        # --- Ne garder que la "dernière" colonne Temperature s'il y en a plusieurs ---
        last_temp_col = _last_temperature_column(df_palier.columns)
        if last_temp_col:
            # trouver toutes les colonnes 'temperature' selon la même logique de normalisation
            temp_cols = [c for c in df_palier.columns if re.match(r'^temperature[_\W]*\d*$', re.sub(r'\s+', '', c), flags=re.I)]
            cols_to_drop = [c for c in temp_cols if c != last_temp_col]
            if cols_to_drop:
                df_palier = df_palier.drop(columns=cols_to_drop)
        # --- fin gestion temperature ---

        # ajouter date et profondeur numérique
        df_palier["Date"] = dt.normalize()
        df_palier["Profondeur"] = df_palier["Palier"].map(PALIERS_VALEUR)

        df_palier = df_palier.drop(columns=["Palier"])

        # ajouter lignes à la liste
        all_rows.append(df_palier)

    if not all_rows:
        raise ValueError("Aucun fichier valide")

    # concaténation de toutes les lignes
    df_long = pd.concat(all_rows, ignore_index=True, sort=False)

    # --- si plusieurs mesures le même jour et même palier, faire la moyenne des mesures ---
    # On calcule la moyenne des colonnes numériques; pour les colonnes non numériques on garde la première valeur rencontrée.
    cols_mesures = [c for c in df_long.columns if c not in ("Date", "Profondeur")]
    numeric_cols = df_long[cols_mesures].select_dtypes(include='number').columns.tolist()
    non_numeric_cols = [c for c in cols_mesures if c not in numeric_cols]
    agg_dict = {c: 'mean' for c in numeric_cols}
    for c in non_numeric_cols:
        agg_dict[c] = 'first'
    # groupe et agrège
    df_long = df_long.groupby(["Date", "Profondeur"], as_index=False).agg(agg_dict)
    # --- fin moyenne multiple même jour/palier ---

    # Réordonner colonnes: Date, Profondeur, puis le reste
    other_cols = [c for c in df_long.columns if c not in ("Date", "Profondeur")]
    df_long = df_long[["Date", "Profondeur"] + other_cols]

    # Appliquer suppression de colonnes à IGNORER
    ignore_norm = {normalize_name(c) for c in IGNORER_COLONNES}
    keep_cols = ["Date", "Profondeur"] + [c for c in other_cols if normalize_name(c) not in ignore_norm]
    df_long = df_long[keep_cols]

    # Appliquer IGNORER_JOUR_VALEUR: pour certaines dates, mettre NA pour certaines mesures
    for date_str, cols_to_null in IGNORER_JOUR_VALEUR.items():
        try:
            jour = pd.to_datetime(date_str).normalize()
            mask = df_long["Date"] == jour
            for c in cols_to_null:
                # trouver colonnes correspondantes en normalisant
                matches = [col for col in df_long.columns if normalize_name(col) == normalize_name(c)]
                for m in matches:
                    df_long.loc[mask, m] = pd.NA
        except Exception as e:
            print(f"Erreur exclusion {date_str}: {e}")

    df_long = df_long.sort_values(["Date", "Profondeur"]).reset_index(drop=True)

    return df_long

def sauvegarder_csv(df_long, output_path="resultats_paliers_long.csv"):
    """
    Sauvegarde en CSV ; séparateur ';'.
    """
    df_to_save = df_long.copy()
    # Si Date est Timestamp, formater en date ISO (YYYY-MM-DD)
    if pd.api.types.is_datetime64_any_dtype(df_to_save["Date"]):
        df_to_save["Date"] = df_to_save["Date"].dt.date
    df_to_save.to_csv(output_path, index=False, sep=";", encoding="utf-8")
    print(f"Fichier CSV sauvegardé : {output_path}")

def remplacer_premiere_ligne_csv(csv_path: str, nouvelle_premiere_ligne: str, encoding="utf-8"):
    """
    Remplace la première ligne d'un fichier CSV par `nouvelle_premiere_ligne`.
    Ne tente pas d'ajuster les colonnes — remplace simplement la ligne d'en-tête.
    """
    try:
        with open(csv_path, "r", encoding=encoding) as f:
            lignes = f.readlines()
        if not lignes:
            raise ValueError("Le fichier est vide.")
        # s'assurer que la nouvelle ligne se termine par un saut de ligne
        if not nouvelle_premiere_ligne.endswith("\n"):
            nouvelle_premiere_ligne = nouvelle_premiere_ligne + "\n"
        lignes[0] = nouvelle_premiere_ligne
        with open(csv_path, "w", encoding=encoding) as f:
            f.writelines(lignes)
        print(f"Première ligne du CSV remplacée : {csv_path}")
    except Exception as e:
        print(f"Erreur lors du remplacement de la première ligne de {csv_path} : {e}")

if __name__ == "__main__":
    dossier = "../rapports/rapports_xlsx/Q40"
    df_long = analyser_dossier_paliers(dossier)
    output_csv = "../rapports/moyennes_paliers.csv"
    sauvegarder_csv(df_long, output_csv)

    # Ligne d'en-tête souhaitée (unités entre parenthèses)
    nouvelle_entete = (
        "Date;Profondeur (m);Temperature (°C);Chlorophyll A (RFU);Dissolved Oxygen Concentration (mg/L);"
        "Dissolved Oxygen Saturation (%);Oxygen Partial Pressure (Torr);Actual Conductivity (µS/cm);"
        "Specific Conductivity (µS/cm);Salinity (PSU);Resistivity (Ω·m);Density of Water (g/cm³);"
        "Total Dissolved Solids (ppt);pH;pH mV (mV);ORP (mV);Crude Oil (RFU);Turbidity (NTU)"
    )

    # Remplacer la première ligne du CSV par la nouvelle en-tête
    remplacer_premiere_ligne_csv(output_csv, nouvelle_entete)

