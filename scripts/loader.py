import csv
import datetime
import uuid

import click

from dataregistry.api import model
from scripts.apiclient import save_study, get_studies, get_datasets, save_dataset, upload_phenotype

data_sets_to_files = {}


def infer_sex(ds_name):
    if ds_name.endswith('Females'):
        return 'female'
    if ds_name.endswith('Males'):
        return 'male'
    return 'mixed'


@click.command()
@click.option('--submitter-name', '-n', type=str, default="Trang Nguyen")
@click.option('--submitter-email', '-e', type=str, default="trang@broadinstitute.org")
@click.option('--csv-file', '-f', type=str, default="/home/dhite/bulk_load/non_EBI.csv")
def load_file(submitter_name, submitter_email, csv_file):
    with open(csv_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            if row['status'] != 'formatted':
                continue
            pmid = row['PMID']
            study = row['Study']
            ds_name = row['dataset']
            saved_study = find_study(study)
            existing_datasets = {ds.name: ds for ds in get_datasets()}
            if ds_name not in existing_datasets:
                ds = model.DataSet(name=ds_name, data_source_type='file',
                                   data_type=row['tech'].lower() if row['tech'] else 'gwas', genome_build='hg19',
                                   ancestry=row['ancestry'] if row['ancestry'] else 'n/a',
                                   data_submitter=submitter_name,
                                   data_submitter_email=submitter_email, sex=infer_sex(ds_name),
                                   global_sample_size=row['subjects'],
                                   status='open' if pmid != '' else 'pre', description=row['description'], pub_id=pmid,
                                   publicly_available=True, study_id=str(saved_study.id).replace('-', ''))
                ds = save_dataset(ds)
            else:
                ds = existing_datasets[ds_name]

            if ds not in data_sets_to_files:
                data_sets_to_files[ds] = []
            data_sets_to_files[ds].append(model.SavedPhenotypeDataSet(id=uuid.uuid4(), created_at=datetime.datetime.now(),
                                                                      phenotype=row['portal_pheno'],
                                                                      dichotomous=True,
                                                                      sample_size=row['subjects'],
                                                                      cases=row['cases'],
                                                                      controls=row['controls'],
                                                                      file_name=row['original_dataset'],
                                                                      s3_path='TBD',
                                                                      file_size=100))
    for ds in data_sets_to_files:
        print(f"Saving {ds.name} with {len(data_sets_to_files[ds])} phenotypes")
        for pd in data_sets_to_files[ds]:
            upload_phenotype(str(ds.id).replace('-', ''), pd.phenotype, pd.dichotomous, pd.sample_size,
                             pd.file_name, "/home/dhite/dr-demo-data/ds2.gz", cases=pd.cases, controls=pd.controls)


def find_study(study):
    existing_studies = {study.name: study for study in get_studies()}
    if study not in existing_studies:
        m_study = model.Study(name=study, institution='Broad Institute')
        saved_study = save_study(m_study)
    else:
        saved_study = existing_studies[study]
    return saved_study


if __name__ == '__main__':
    load_file()
