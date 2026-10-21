#!/usr/bin/env bash
# One-off preparation of the GRCh38 + dbSNP155 reference bundle used by the
# SGC munge worker. Idempotent: every step is skipped if its output exists.
#
# Usage: prep_reference.sh [OUT_S3] [WORK_DIR]
set -euo pipefail

OUT_S3=${1:-s3://dig-data-registry/sgc/ref/grch38-dbsnp155/}
WORK=${2:-./ref_work}
OUT_S3=${OUT_S3%/}/

REF_VERSION="grch38-ens110+dbsnp155-slim-v1"
FA_URL="https://ftp.ensembl.org/pub/release-110/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
DBSNP_URL="https://ftp.ncbi.nih.gov/snp/archive/b155/VCF/GCF_000001405.39.gz"
REPORT_URL="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.39_GRCh38.p13/GCF_000001405.39_GRCh38.p13_assembly_report.txt"
CONTIGS="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT"

for tool in bgzip tabix samtools bcftools aws curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

# Download helpers. NB: do not use `[[ -s f ]] || { curl && mv; }` inline: set -e is
# suppressed inside && lists on the right of ||, so a failed curl would not abort.
fetch() {
    local url=$1 dest=$2
    if [[ -s $dest ]]; then echo "skip $dest (exists)"; return 0; fi
    echo "download $dest"
    if ! curl -fL -sS --retry 3 -o "$dest.part" "$url"; then
        echo "ERROR: download failed: $url" >&2
        exit 1
    fi
    mv -f "$dest.part" "$dest"
}

mkdir -p "$WORK"
cd "$WORK"

FA=GRCh38.primary.fa.gz
VCF=dbsnp155.grch38.slim.vcf.gz

echo "== step 1: reference FASTA"
# (1) Reference FASTA: Ensembl primary assembly, bgzipped + faidx (.fai/.gzi)
if [[ ! -s $FA || ! -s $FA.fai || ! -s $FA.gzi ]]; then
    fetch "$FA_URL" ensembl.fa.gz
    zcat ensembl.fa.gz | bgzip -@4 > "$FA.part"
    mv -f "$FA.part" "$FA"
    samtools faidx "$FA"
    rm -f ensembl.fa.gz
else
    echo "skip: $FA exists"
fi

echo "== step 2: dbSNP slim VCF"
# (2) dbSNP 155: rename RefSeq accessions to 1..22,X,Y,MT, strip INFO, keep 25 contigs
if [[ ! -s $VCF || ! -s $VCF.tbi ]]; then
    fetch "$DBSNP_URL" GCF_000001405.39.gz
    fetch "$REPORT_URL" assembly_report.txt

    # NCBI publishes the b155 VCF as plain gzip (not BGZF) with no .tbi, so tabix cannot
    # index it. Recompress to BGZF, then index: the b155 header has no ##contig lines and
    # `bcftools annotate --rename-chrs` aborts on such a header unless htslib can fill the
    # contigs from a tabix index.
    if [[ -s GCF_000001405.39.bgz ]]; then
        echo "skip GCF_000001405.39.bgz (exists)"
    else
        echo "== step 2a: recompress GCF_000001405.39.gz to BGZF (~24 GB rewrite, tens of minutes)"
        zcat GCF_000001405.39.gz | bgzip -@ "$(nproc)" -c > GCF_000001405.39.bgz.part
        mv -f GCF_000001405.39.bgz.part GCF_000001405.39.bgz
    fi
    if [[ -s GCF_000001405.39.bgz.tbi ]]; then
        echo "skip GCF_000001405.39.bgz.tbi (exists)"
    else
        echo "== step 2b: index GCF_000001405.39.bgz"
        tabix -p vcf GCF_000001405.39.bgz
    fi

    # Assembly report columns: 1 Sequence-Name, 2 Sequence-Role, 3 Assigned-Molecule,
    # ..., 7 RefSeq-Accn. Assembled chromosomes carry Sequence-Name 1..22,X,Y and MT
    # for the mitochondrion (assigned-molecule MT), so map RefSeq-Accn -> Sequence-Name
    # for role "assembled-molecule" rows only (excludes scaffolds/patches/alts).
    tr -d '\r' < assembly_report.txt \
        | awk -F'\t' '!/^#/ && $2 == "assembled-molecule" && $7 != "na" {print $7 "\t" $1}' > chr_map.txt
    n_map=$(wc -l < chr_map.txt)
    [[ $n_map -eq 25 ]] || { echo "expected 25 chromosome mappings, got $n_map" >&2; exit 1; }
    grep -q -P '\tMT$' chr_map.txt || { echo "no MT mapping in chr_map.txt" >&2; exit 1; }

    bcftools annotate --rename-chrs chr_map.txt -x INFO GCF_000001405.39.bgz -Ou \
        | bcftools view -t "$CONTIGS" -Oz -o "$VCF.part"
    mv -f "$VCF.part" "$VCF"
    tabix -f -p vcf "$VCF"
else
    echo "skip: $VCF exists"
fi

echo "== step 2 checks: $VCF"
# Exactly the 25 renamed contigs, and the first b155 record (rs1570391677 at 1:10001)
# reachable through the index. Run on every invocation, so a stale file also fails.
got=$(tabix -l "$VCF" | sort | paste -sd, -)
want=$(tr ',' '\n' <<< "$CONTIGS" | sort | paste -sd, -)
if [[ $got != "$want" ]]; then
    echo "ERROR: $VCF has contigs [$got]; expected exactly [$want]" >&2
    exit 1
fi
# capture first: `tabix | grep -q` can fail under pipefail when grep exits early (SIGPIPE)
first=$(tabix "$VCF" 1:10001-10001)
if ! grep -q rs1570391677 <<< "$first"; then
    echo "ERROR: rs1570391677 not found at 1:10001 in $VCF" >&2
    exit 1
fi
echo "ok: 25 contigs; rs1570391677 at 1:10001"

echo "== step 3: MANIFEST.json"
# (3) MANIFEST.json
if [[ ! -s MANIFEST.json ]]; then
    cat > MANIFEST.json.part <<JSON
{
  "ref_version": "$REF_VERSION",
  "sources": {
    "fasta": "$FA_URL",
    "dbsnp": "$DBSNP_URL",
    "assembly_report": "$REPORT_URL"
  },
  "md5": {
    "$FA": "$(md5sum "$FA" | cut -d' ' -f1)",
    "$VCF": "$(md5sum "$VCF" | cut -d' ' -f1)",
    "GCF_000001405.39.gz": "$(md5sum GCF_000001405.39.gz | cut -d' ' -f1)",
    "GCF_000001405.39.bgz": "$(md5sum GCF_000001405.39.bgz | cut -d' ' -f1)",
    "assembly_report.txt": "$(md5sum assembly_report.txt | cut -d' ' -f1)"
  },
  "created_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "tools": {
    "bcftools": "$(bcftools --version | head -1)",
    "samtools": "$(samtools --version | head -1)",
    "bgzip": "$(bgzip --version | head -1)",
    "tabix": "$(tabix --version | head -1)"
  }
}
JSON
    mv -f MANIFEST.json.part MANIFEST.json
else
    echo "skip: MANIFEST.json exists"
fi

echo "== step 4: upload to $OUT_S3"
# (4) Upload the six files the worker downloads
for f in "$FA" "$FA.fai" "$FA.gzi" "$VCF" "$VCF.tbi" MANIFEST.json; do
    aws s3 cp "$f" "$OUT_S3$f"
done
echo "done: $OUT_S3"
