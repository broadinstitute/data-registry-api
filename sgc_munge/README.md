# sgc_munge

Munge stage of the SGC GWAS pipeline, run as an AWS Batch Fargate job per GWAS file.

- Reads the uploaded summary statistics and standardises columns using the file's column mapping.
- Validates/normalises alleles against GRCh38 (Ensembl 110) and fills/checks rsIDs from a slim dbSNP 155 VCF.
- Writes a standardised table plus a `munge.json` count summary (and a `ref_mismatch` report when any row
  fails to match the reference) to S3.
- Alleles, Beta and EAF are written exactly as reported; only `SNP_ID` and `RSID` use the reference orientation.

> **Worker images are built manually.** CI and the fabfile do NOT rebuild `sgc-gwas-munge`.
> A change to `sgc_munge/` code is inert until you rebuild and push the image (below).

## 1. One-off: prepare the reference bundle

Needs `bgzip tabix samtools bcftools aws curl` on PATH and ~100 GB of scratch disk. Idempotent: re-running skips finished steps.

```bash
bash sgc_munge/prep_reference.sh s3://dig-data-registry/sgc/ref/grch38-dbsnp155/ ./ref_work
```

dbSNP b155 was released against GRCh38.p13, so the source is `GCF_000001405.39` (the `.40`/p14 file is b156+).
Step 2 is: NCBI's plain-gzip download (no `.tbi` published; header has no `##contig` lines) -> recompress to
BGZF (~24 GB rewrite, tens of minutes) -> tabix index -> `bcftools annotate --rename-chrs` / `view -t`.
Download failures abort the script.
After building the slim VCF the script checks that `tabix -l` lists exactly `1..22 X Y MT` and that
`rs1570391677` is at `1:10001` (also on re-runs, so a stale file fails).

Uploads `GRCh38.primary.fa.gz` (+ `.fai`, `.gzi`), `dbsnp155.grch38.slim.vcf.gz` (+ `.tbi`) and `MANIFEST.json`
(`ref_version` = `grch38-ens110+dbsnp155-slim-v1`, source URLs, md5s, UTC date, tool versions).

## 2. Deploy the stack

Creates the ECR repo, job role, log group and job definition `sgc-gwas-munge-job` (shares the existing `sgc-gwas-qc-plots-queue`).

```bash
aws cloudformation deploy --stack-name sgc-gwas-munge \
  --template-file sgc_munge/sgc_gwas_munge.yml \
  --capabilities CAPABILITY_IAM --region us-east-1
```

## 3. Build and push the image (manual)

Run from the repo root (the build context is the repo root).

```bash
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin 005901288866.dkr.ecr.us-east-1.amazonaws.com
docker build -f sgc_munge/Dockerfile -t sgc-gwas-munge .
docker tag sgc-gwas-munge:latest 005901288866.dkr.ecr.us-east-1.amazonaws.com/sgc-gwas-munge-repo:latest
docker push 005901288866.dkr.ecr.us-east-1.amazonaws.com/sgc-gwas-munge-repo:latest
```

## 4. Run the driver

```bash
# QA
python -m sgc_munge.submit_munge_batch --bucket dig-data-registry-qa --db-name dataregistry_qa
# prod
python -m sgc_munge.submit_munge_batch --bucket dig-data-registry --db-name dataregistry
```

Options:

- `--dry-run` list what would be submitted without submitting.
- `--limit N` submit at most N files.
- `--phenotype P` restrict to one phenotype.
- `--file-id ID` restrict to specific file(s); repeatable.
- `--force` re-munge files that already have output.

**Stale munge:** if a file is re-lifted or restored, its existing munge output is stale. Re-run it with
`--force --file-id <id>`.

## Output layout

```
s3://<bucket>/sgc/munge/{file_id}/standardised.tsv.gz
s3://<bucket>/sgc/munge/{file_id}/munge.json
s3://<bucket>/sgc/munge/{file_id}/ref_mismatch.tsv.gz   # only when non-empty
```

`ref_mismatch.tsv.gz` exists only when at least one row matched neither allele to the reference; the
DB row's `ref_mismatch_s3_key` is NULL otherwise.

Rows in `standardised.tsv.gz` are grouped by contig in the order `1..22, X, Y` and keep the input
order within a contig; `ref_mismatch.tsv.gz` keeps the input order.

A file fails (status FAILED, exit 2, nothing uploaded) when its column mapping is unusable or when, over at
least 1000 pre-scanned SNVs, fewer than 95% have one allele equal to the GRCh38 base ("reference match rate
... suggests the file is not GRCh38"): that is a wrong genome build, not a few bad rows.

## How rsIDs are looked up

The munge reads the input twice, around one bulk dbSNP query, instead of querying dbSNP row by row.
Pass 1 filters, orients and left-normalises every row, stages it on local disk (one file per contig)
and collects the positions to look up (both readings of an ambiguous indel when dbSNP is to break the
tie). Those positions are sorted and de-duplicated, and one `bcftools query --targets-file` streams
the dbSNP VCF once, writing the records found at them to one file per contig. Pass 2 then takes the
contigs one at a time: it loads that contig's records, joins them to its staged rows by position and
alleles (`pick_rsid` in `reference.py`), drops duplicate `SNP_ID`s and writes the output. Only rows on
contigs listed in the dbSNP header are looked up; the others get `RSID` `NA`. The staged rows and
dbSNP records live in `.work/` next to the outputs and are removed when the munge ends (also on
failure). With a dbSNP VCF configured (as the worker always has), a missing `bcftools` fails the file
before the munge reads the input or writes any output; in the worker that check runs after the
references and the input have been downloaded.

## Reading `munge.json`

It records the reference version used, run timing and memory, and row counts. The counts reconcile:

```
n_in == n_out + sum(n_dropped.values())
```

`n_in` is rows read, `n_out` rows written to `standardised.tsv.gz`, `n_dropped` a per-reason breakdown
(e.g. `ref_mismatch`, whose rows are listed in `ref_mismatch.tsv.gz`), `n_with_rsid` rows with an rsID,
and `n_swapped` rows whose effect allele is the reference allele. Nothing is flipped for swapped rows:
`Effect_allele`, `Non-effect_allele`, `Beta` and `Effect_AF` are written exactly as reported; only `SNP_ID`
(`chrom:pos:REF:ALT`) and the rsID lookup use the reference orientation. `Position` is the reported
position as an integer (before indel left-alignment, which only affects `SNP_ID`).

Orientation and indels:

- `orientation_prescan` -- over the first 200,000 parseable SNVs: `n_snv`, `n_oa_is_ref` /
  `n_ea_is_ref` (rows whose non-effect / effect allele is the GRCh38 base), `prefer` (`oa` or `ea`,
  the majority), and `purity` = `min(n_oa_is_ref, n_ea_is_ref) / n_snv` (0 without SNVs) -- the
  minority-orientation fraction, i.e. how mixed the file's coding is (~0 for a REF/ALT-coded file).
- `n_indel_ambiguous` -- written indels whose two alleles both match the reference at their own lengths
  (every deletion, and insertions that copy the following bases), so either could be REF.
  When `purity <= 0.001` (`_TIE_BREAK_MIN_PURITY` in `munge.py`; in practice a perfectly consistent
  file) they are oriented by `prefer` alone.
- `n_indel_tie_dbsnp_resolved` -- when `purity > 0.001`: ambiguous indels where exactly one reading
  (each normalised, then looked up with its own REF/ALT) has a dbSNP allele match, which decided it.
- `n_indel_tie_unresolved` -- when `purity > 0.001`: ambiguous indels where neither or both readings
  matched dbSNP, so `prefer` decided. With `purity > 0.001` these two counters sum to
  `n_indel_ambiguous`; otherwise both are 0.
- `n_indel_norm_exhausted` -- indels that could not be left-aligned within a 1.6 kb window (or ran into
  the contig start); they keep the reported position and alleles in `SNP_ID`.
- `rsid_lookup` -- `n_targets` (unique positions sent to bcftools), `n_hit_positions` (positions
  with a dbSNP record), `bcftools` (version line of the bcftools used; `null` without dbSNP).
- `timings` -- seconds per phase: `pass1_s` (header, orientation pre-scan and pass 1), `sort_s`
  (sorting the targets), `bcftools_s` (the dbSNP query; 0 when there are no targets) and `pass2_s`.
  The same phases are logged to stderr (`[munge] <phase>: ...`) as they finish.

## Development

The tests and the worker need `bcftools` on PATH (or `SGC_BCFTOOLS=/path/to/bcftools`). Without sudo, build it into user space: `cd "$(mktemp -d)" && curl -L https://github.com/samtools/bcftools/releases/download/1.21/bcftools-1.21.tar.bz2 | tar xj && cd bcftools-1.21 && ./configure --prefix="$HOME/.local" && make -j4 && make install`.
