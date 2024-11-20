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


def upload_file_to_s3(file_name, file_guid, extra_args={'ContentType': 'image/png'}):
    s3 = boto3.client('s3')
    bucket = "hermes-qc"
    key = f"images/{file_guid}/" + file_name
    s3.upload_file(file_name, bucket, key, ExtraArgs=extra_args)


def convert_to_genepi_map(col_map):
    mapping = {
        'chromosome': 'chr',
        'position': 'bp',
        'reference': 'ea',
        'alt': 'oa',
        'pValue': 'p',
        'beta': 'beta',
        'rsid': 'rsid',
        'se': 'se',
        'N total': 'n',
        'eaf': 'eaf',
        'zScore': 'info',
    }
    return {mapping[k]: v for k, v in col_map.items() if k in mapping}


def run_r_commands(file_path, file_guid, col_map):
    genepi_map = convert_to_genepi_map(col_map)
    col_mapping = " ".join([f"-{k} {v}" for k, v in genepi_map.items()])
    ref_mapping = "-r_chr \"#CHROM\" -r_bp POS -r_ea REF -r_oa ALT -r_eaf AF -r_id ID -o out"
    r_script_command = (f"Rscript heRmes/scripts/gwas_qc.R -r HRC.r1-1.GRCh37.wgs.mac5.sites.tab.gz -g {file_path} "
                        f"{col_mapping} {ref_mapping} -o .")
    try:
        print("Running command:", r_script_command)
        result = subprocess.run(r_script_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print("QC Script Output:", result.stdout)
        result = subprocess.run(
        [
            "Rscript",
            "-e",
            "rmarkdown::render('heRmes/scripts/gwas_qc.Rmd', params = list(fig_dir = '/usr/src/app'), output_dir = '/usr/src/app')"
        ], check=True, capture_output=True, text=True)
        print("HTML Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Command failed with return code:", e.returncode)
        print("Error Output:", e.stderr)
        print("Standard Output:", e.stdout)
        print("Command that failed:", e.cmd)
        exit(e.returncode)

    if result.returncode != 0:
        print("Command completed with non-zero return code:", result.returncode)
        print("Error Output:", result.stderr)
        print("Standard Output:", result.stdout)
        exit(result.returncode)

    upload_file_to_s3("eaf_plot.png", file_guid)
    upload_file_to_s3("gwas_qc.html", file_guid, extra_args={'ContentType': 'text/html'})
    # upload_file_to_s3("manhattan_plot.png", file_guid)


@click.command()
@click.option('--s3_path', '-s', type=str, required=True)
@click.option('--file_guid', '-g', type=str, required=True)
@click.option('--column_map', '-c', type=str, required=True)
def main(s3_path, file_guid, column_map):
    download_file_from_s3("s3://dig-data-registry-qa/hermes/nick-reference/HRC.r1-1.GRCh37.wgs.mac5.sites.tab.gz")
    local_file = download_file_from_s3(s3_path)
    run_r_commands(local_file, file_guid, json.loads(column_map))


if __name__ == "__main__":
    main()
