import csv
import argparse

from pydantic import BaseModel
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB

engine = DataRegistryReadWriteDB().get_engine()

class Phenotype(BaseModel):
    name: str
    description: str
    dichotomous: bool
    group: str

ancestries = {
    "ABA": "Aboriginal Australian",
    "AA": "African American or Afro-Caribbean",
    "AF": "African unspecified",
    "SSAF": "Sub-Saharan African",
    "ASUN": "Asian unspecified",
    "CA": "Central Asian",
    "EA": "East Asian",
    "SA": "South Asian",
    "SEA": "South East Asian",
    "EU": "European",
    "GME": "Greater Middle Eastern (Middle Eastern, North African, or Persian)",
    "HS": "Hispanic or Latin American",
    "NAM": "Native American",
    "NR": "Not reported",
    "OC": "Oceanian",
    "OTH": "Other",
    "OAD": "Other admixed ancestry",
    "Mixed": "Mixed ancestry",
    "n/a": "N/A"
}

def read_new_phenotypes_file(file_path):
    phenotypes = []
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            phenotypes.append(Phenotype(name=row['portal_pheno'], description=row['phenotype description'],
                                        dichotomous=row['dichotomous'], group=row['Group']))
    return phenotypes

def read_datasets_file(file_path):
    datasets = {}
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            name = row['dataset']
            if name not in datasets:
                datasets[name] = row
                datasets[name]['phenotypes'] = [row['portal_pheno']]
            else:
                datasets[name]['phenotypes'].append(row['portal_pheno'])
    return datasets

def save_phenotype(name, description, dichotomous, group, prod_run=False):
    table_name = "Phenotypes" if prod_run else "PhenotypesLoader"
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {table_name} (name, description, dichotomous, `group`) 
            VALUES (:name, :description, :dichotomous, :group)
            ON DUPLICATE KEY UPDATE
                description=:description,
                dichotomous=:dichotomous,
                `group`=:group
            """),
            {'name': name, 'description': description, 'dichotomous': dichotomous, 'group': group})
        conn.commit()

def save_phenotypes(phenotypes, prod_run=False):
    for p in phenotypes:
        save_phenotype(p.name, p.description, p.dichotomous, p.group, prod_run)

def get_existing_phenotypes(conn, dataset_name, prod_run=False):
    table_name = "Datasets" if prod_run else "DatasetsLoader"
    result = conn.execute(text(f"""
        SELECT phenotypes 
        FROM {table_name} 
        WHERE name = :dataset_name
    """), {'dataset_name': dataset_name})
    row = result.fetchone()
    if row and row[0]:
        return set(row[0].split(','))
    return set()

def save_datasets(datasets, prod_run=False):
    table_name = "Datasets" if prod_run else "DatasetsLoader"
    with engine.connect() as conn:
        for name, data in datasets.items():
            print(f"Saving {name}")
            params = data.copy()

            existing_phenotypes = get_existing_phenotypes(conn, name, prod_run)
            new_phenotypes = set(data['phenotypes'])
            if new_phenotypes == existing_phenotypes:
                print(f"Skipping {name} as no new phenotypes")
                continue

            merged_phenotypes = existing_phenotypes.union(new_phenotypes)
            added_phenotypes = new_phenotypes - existing_phenotypes
            print(f"Updating {name} with new phenotypes: {added_phenotypes}")


            params['phenotypes'] = ','.join(sorted(merged_phenotypes))
            params['ancestry_name'] = ancestries.get(data['ancestry'], None)
            params['PMID'] = int(data['PMID']) if data['PMID'].isdigit() and int(data['PMID']) > 0 else None
            conn.execute(text(f"""
                INSERT INTO {table_name} (name, description, phenotypes, ancestry, ancestry_name, tech, subjects, pmid, community, added, updated) 
                VALUES (:dataset, :description, :phenotypes, :ancestry, :ancestry_name, :tech, :subjects, :PMID, :community, NOW(), NOW())
                ON DUPLICATE KEY UPDATE 
                    phenotypes=:phenotypes
                    updated=NOW()
                    """), params)
            conn.commit()

def main():
    parser = argparse.ArgumentParser(description='Load phenotypes and datasets from CSV files')
    parser.add_argument('--phenotypes-file', required=True, help='Path to the new phenotypes CSV file')
    parser.add_argument('--datasets-file', required=True, help='Path to the datasets CSV file')
    parser.add_argument('--prod-run', action='store_true', default=False, help='If set, upserts to production tables instead of loader tables')

    args = parser.parse_args()

    phenotypes = read_new_phenotypes_file(args.phenotypes_file)
    save_phenotypes(phenotypes, args.prod_run)

    datasets = read_datasets_file(args.datasets_file)
    save_datasets(datasets, args.prod_run)

if __name__ == "__main__":
    main()
