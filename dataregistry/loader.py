import csv
import uuid
from dataregistry.api import model

with open('/home/dhite/bulk_load/non_EBI.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    datasetIds = {}

    for row in reader:
        print(row)
        pmid = row['PMID']
        model.DataSet(name=row['dataset'], data_source_type='file',
                      data_type=row['tech'].lower() if row['tech'] else 'gwas', genome_build='grch38',
                      ancestry=row['ancestry'] if row['ancestry'] else 'n/a', data_submitter='Drew Hite',
                      data_submitter_email='dhite@broad.org', sex='mixed', global_sample_size=1000,
                      status='open' if pmid != '' else 'pre', description=row['description'], pub_id=pmid,
                      publicly_available=True, study_id='foo')
        # if not datasetIds.get(row['dataset']):
        #     datasetIds[row['dataset']] = uuid.uuid4()

    # for datasetId in datasetIds:
    #     print(f"{datasetId},{datasetIds[datasetId]}")
