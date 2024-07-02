import json
import subprocess

import boto3
import click


def download_file_from_s3(s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    remote_file_name = key.split('/')[-1]
    s3.download_file(bucket, key, remote_file_name)
    return remote_file_name


def upload_file_to_s3(file_name, file_guid):
    s3 = boto3.client('s3')
    bucket = "hermes-qc"
    key = f"images/{file_guid}/" + file_name
    s3.upload_file(file_name, bucket, key, ExtraArgs={'ContentType': 'image/png'})


def convert_to_genepi_map(col_map):
    mapping = {
        'chromosome': 'chr',
        'position': 'bp',
        'reference': 'ea',
        'alt': 'oa',
        'pValue': 'p',
        'beta': 'beta',
        'rsid': 'rsid',
        'stdErr': 'se',
        'eaf': 'eaf',
    }
    return {mapping.get(k, k): v for k, v in col_map.items()}


def run_r_commands(file_path, file_guid, col_map):
    genepi_map = convert_to_genepi_map(col_map)
    col_mapping = ",".join([f"\"{k}\"=\"{v}\"" for k, v in genepi_map.items()])
    r_commands = """
    library('genepi.utils')
    library(data.table)
    gw <- GWAS('{}', ColumnMap(list({})))
    dt <- genepi.utils::as.data.table(gw)
    p <- qq_plot(dt, plot_corrected=TRUE, pval_col='p')
    png("qq_plot.png", width=600, height=600, units="px")
    p
    dev.off()
    setnames(dt, old = "rsid", new = "snp")
    manhattan_data <- manhattan(dt)
    png("manhattan_plot.png", width=600, height=600, units="px")
    manhattan_data
    dev.off()
    """.format(file_path, col_mapping, file_path, file_path)
    result = subprocess.run(["R", "-e", r_commands], check=True)
    if result.returncode != 0:
        print("Error Output:", result.stderr)
        return
    upload_file_to_s3("qq_plot.png", file_guid)
    upload_file_to_s3("manhattan_plot.png", file_guid)


@click.command()
@click.option('--s3_path', '-s', type=str, required=True)
@click.option('--file_guid', '-g', type=str, required=True)
@click.option('--column_map', '-c', type=str, required=True)
def main(s3_path, file_guid, column_map):
    local_file = download_file_from_s3(s3_path)
    run_r_commands(local_file, file_guid, json.loads(column_map))


if __name__ == "__main__":
    main()
