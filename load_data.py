"""
Script final pour charger les données GeoJSON et CSV dans PostgreSQL/PostGIS
Projet: Cartographie des bassins de production du Cameroun
"""

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
from pathlib import Path

# ============================================
# 1. CONFIGURATION DE LA CONNEXION
# ============================================

# À MODIFIER : remplacez 'votre_mot_de_passe' par le vrai mot de passe de l'utilisateur postgres
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'cameroun_production_db',
    'user': 'postgres',
    'password': 'lelouch237' 
}

# Chemins vers les fichiers de données
DATA_DIR = Path('data')
FILES = {
    'regions': DATA_DIR / 'cmr_admin1.geojson',
    'departements': DATA_DIR / 'cmr_admin2.geojson',
    'communes': DATA_DIR / 'cmr_admin3.geojson',
    'productions': DATA_DIR / 'productions.csv'
}

def create_connection_string(config):
    """Créer une chaîne de connexion sécurisée (gère les caractères spéciaux)"""
    password = urllib.parse.quote_plus(config['password'])
    return f"postgresql://{config['user']}:{password}@{config['host']}:{config['port']}/{config['database']}"

# ============================================
# 2. FONCTIONS DE CHARGEMENT
# ============================================

def verify_files():
    """Vérifier que tous les fichiers de données sont présents"""
    print("=== Vérification des fichiers ===")
    all_exist = True
    for name, path in FILES.items():
        exists = path.exists()
        status = "✓" if exists else "✗"
        print(f"{status} {name}: {path}")
        if not exists:
            all_exist = False
    
    if not all_exist:
        raise FileNotFoundError("Certains fichiers sont manquants dans le dossier /data !")
    print()

def test_connection(engine):
    """Tester la connexion et la présence de PostGIS"""
    print("=== Test de connexion à PostgreSQL ===")
    try:
        with engine.connect() as conn:
            # Test version Postgres
            res = conn.execute(text("SELECT version();")).fetchone()
            print(f"✓ Connecté à : {res[0][:50]}...")
            
            # Test extension PostGIS
            res = conn.execute(text("SELECT PostGIS_Version();")).fetchone()
            print(f"✓ PostGIS détecté : {res[0]}")
            return True
    except Exception as e:
        print(f"✗ Erreur de connexion : {e}")
        return False

def process_gdf(gdf, pcode_col, name_col, parent_code_col=None):
    """Nettoyer et formater un GeoDataFrame pour PostGIS"""
    # 1. Forcer le système de coordonnées en WGS84 (EPSG:4326)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    else:
        gdf.to_crs(epsg=4326, inplace=True)

    # 2. Conversion systématique en MultiPolygon pour éviter les conflits de types
    gdf['geometry'] = gdf['geometry'].apply(
        lambda geom: geom if geom.geom_type == 'MultiPolygon' 
        else gpd.GeoSeries([geom]).union_all()
    )

    # 3. Sélection des colonnes et renommage
    cols = [pcode_col, name_col, 'geometry']
    if parent_code_col:
        cols.append(parent_code_col)
    
    gdf_clean = gdf[cols].copy()
    
    # 4. Renommer la colonne géométrie en 'geom' (standard PostGIS)
    gdf_clean = gdf_clean.rename_geometry('geom')
    return gdf_clean

def load_all_data(engine):
    """Charger toutes les tables avec nettoyage préalable des dépendances"""
    try:
        # ÉTAPE PRÉALABLE : Supprimer les tables existantes proprement
        print("--- Nettoyage de la base de données ---")
        with engine.connect() as conn:
            # On supprime dans l'ordre inverse des dépendances ou avec CASCADE
            conn.execute(text("DROP TABLE IF EXISTS productions CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS communes CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS departements CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS regions CASCADE;"))
            conn.commit() # Très important pour valider la suppression
        print("✓ Anciennes tables supprimées.")

        # --- RÉGIONS ---
        print("--- Chargement des régions ---")
        gdf_r = gpd.read_file(FILES['regions'])
        gdf_r_clean = process_gdf(gdf_r, 'adm1_pcode', 'adm1_name1')
        # On utilise 'append' maintenant car on a déjà supprimé la table manuellement au-dessus
        gdf_r_clean.to_postgis('regions', engine, if_exists='append', index=False)
        print(f"✓ {len(gdf_r_clean)} régions chargées.")

        # --- DÉPARTEMENTS ---
        print("--- Chargement des départements ---")
        gdf_d = gpd.read_file(FILES['departements'])
        gdf_d_clean = process_gdf(gdf_d, 'adm2_pcode', 'adm2_name1', 'adm1_pcode')
        gdf_d_clean.to_postgis('departements', engine, if_exists='append', index=False)
        print(f"✓ {len(gdf_d_clean)} départements chargés.")

        # --- COMMUNES ---
        print("--- Chargement des communes ---")
        gdf_c = gpd.read_file(FILES['communes'])
        gdf_c_clean = process_gdf(gdf_c, 'adm3_pcode', 'adm3_name1', 'adm2_pcode')
        gdf_c_clean.to_postgis('communes', engine, if_exists='append', index=False)
        print(f"✓ {len(gdf_c_clean)} communes chargées.")

        # --- PRODUCTIONS (CSV) ---
        print("--- Chargement des données de production ---")
        df_p = pd.read_csv(FILES['productions'], encoding='utf-8')
        if 'adm3_pcode' in df_p.columns:
            df_p['adm3_pcode'] = df_p['adm3_pcode'].astype(str)
        df_p.to_sql('productions', engine, if_exists='append', index=True)
        print(f"✓ {len(df_p)} lignes de production chargées.")
        
        return True
    except Exception as e:
        print(f"✗ Erreur pendant le chargement : {e}")
        return False
# ============================================
# 3. EXECUTION PRINCIPALE
# ============================================

def main():
    print("=" * 60)
    print("IMPORTATION DES DONNÉES CAMEROUN POSTGIS")
    print("=" * 60)
    
    # Vérifier fichiers
    try:
        verify_files()
    except Exception as e:
        print(e)
        return

    # Connexion
    engine = create_engine(create_connection_string(DB_CONFIG))
    if not test_connection(engine):
        print("\nConseil : Assurez-vous d'avoir créé la base de données et activé PostGIS")
        print("SQL : CREATE DATABASE cameroun_production_db; \\c ...; CREATE EXTENSION postgis;")
        return

    # Chargement
    print("-" * 60)
    if load_all_data(engine):
        print("-" * 60)
        print("✓ TOUT EST CHARGÉ AVEC SUCCÈS !")
        print("=" * 60)
    else:
        print("\n✗ Le chargement a échoué.")

if __name__ == '__main__':
    main()