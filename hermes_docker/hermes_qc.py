import subprocess
import uuid

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


def run_r_commands(file_path, file_guid):
    r_commands = """
    library('genepi.utils')
    library(data.table)
    GWAS('{}', c('CHR','BP','OA','EA','EAF','BETA','SE','P','EUR_EAF','SNP'))
    
    p <- qq_plot('{}', plot_corrected=TRUE)
    png("qq_plot.png", width=600, height=600, units="px")
    p
    dev.off()
    dt <- fread('{}')
    setnames(dt, tolower(names(dt)))
    manhattan_data <- manhattan(dt)
    png("manhattan_plot.png", width=600, height=600, units="px")
    manhattan_data
    dev.off()
    """.format(file_path, file_path, file_path)
    result = subprocess.run(["R", "-e", r_commands], check=True)
    if result.returncode != 0:
        print("Error Output:", result.stderr)
        return
    upload_file_to_s3("qq_plot.png", file_guid)
    upload_file_to_s3("manhattan_plot.png", file_guid)


@click.command()
@click.option('--s3_path', '-s', type=str, required=True)
@click.option('--file_guid', '-g', type=str, required=True)
def main(s3_path, file_guid):
    local_file = download_file_from_s3(s3_path)
    run_r_commands(local_file, file_guid)


if __name__ == "__main__":
    main()
