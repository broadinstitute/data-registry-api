# sgc_metal: METAL bottom-line meta-analysis worker

The meta-analysis (MA) stage of the SGC pipeline. For one phenotype/ancestry/sex
selection it takes the munged per-cohort GWAS, runs
[METAL](https://github.com/statgen/METAL) (release tag `2020-05-05`, built from
source in the image), then writes the combined results, Manhattan/QQ plots, a
summary and a top-loci table back to S3 and records the run in the database.
The worker reads phenotype, ancestry, selection and thresholds off the run row,
so it needs only `--run-id` and `--bucket`.

## Why a separate job definition

The existing `sgc-gwas-ma-job` definition points at the `sgc-gwas-ma-repo`
image, which is shared by QA and prod. Swapping that image in place would change
prod behaviour immediately. The METAL worker therefore has its own repo, role,
log group and job definition (`sgc-gwas-metal-job`), and the API host chooses
which one to submit to through the `SGC_MA_JOB_DEFINITION` environment variable.
Jobs still run on the existing `sgc-gwas-qc-plots-queue`; this stack creates no
compute environment or queue.

## Build and push

Worker images are built manually (CI and the fabfile do not rebuild them), so a
code change here is inert until the image is rebuilt and pushed. The build
context is the repo root, and compiling METAL takes a few minutes.

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY=$ACCOUNT.dkr.ecr.us-east-1.amazonaws.com

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $REGISTRY
docker build -f sgc_metal/Dockerfile -t sgc-gwas-metal .
docker tag sgc-gwas-metal:latest $REGISTRY/sgc-gwas-metal-repo:latest
docker push $REGISTRY/sgc-gwas-metal-repo:latest
```

The build checks out the `2020-05-05` tag and prints the METAL commit it got
(`git -C METAL rev-parse HEAD`, currently
`e2253cc3901df8403a331bd725d4d9fe1edfb19f`). The commit is also written to
`/usr/local/share/metal_commit.txt` in the image, and `summary.json`'s `engine`
field includes it, for example
`METAL 2020-05-05 (commit e2253cc3901df8403a331bd725d4d9fe1edfb19f)`. Both base
images are pinned to Debian bookworm.

## Deploy the stack

Deploy the stack (creating the ECR repo) before the first push:

```bash
aws cloudformation deploy --stack-name sgc-gwas-metal \
  --template-file sgc_metal/sgc_gwas_metal.yml \
  --capabilities CAPABILITY_IAM --region us-east-1
```

Job sizing: 4 vCPU, 30 GiB memory (the 4-vCPU Fargate maximum), 100 GiB
ephemeral storage, 6-hour timeout, 2 attempts (a retry overwrites the first
attempt's artifacts under the same `run_id` prefix). The sizing comes from
measurements of METAL 2020-05-05:

- Memory: METAL keeps every distinct marker in memory, about 500-550 bytes each,
  so 30 GiB fits roughly 45M distinct markers.
- Time: the main pass (`ANALYZE HETEROGENEITY`, which reads every input twice)
  takes about 12 µs per input line. The frequency pass, which runs only when some
  cohorts lack EAF, is a plain `ANALYZE` at about 7 µs per input line. A
  22-cohort bucket of 12M-row files is about 55 minutes plus 25 minutes of METAL.
- Disk: the worker deletes each staged input after the last pass that reads it,
  and deletes the raw per-pass tables once the final table is written.

Each run's `summary.json` records the actual cost under `metal_runs`, so the
sizing can be checked against real buckets (see below).

## Cut-over

The API reads its environment from `<checkout>/.env`: the systemd unit starts
`python -m dataregistry.main -e .env serve` from the checkout directory (see
`deploy/dr-api-dev.service` and `deploy/dr-api-prd.service`). For QA that file
is `/home/ec2-user/data-registry-api-qa/.env` and the unit is `dr-api-dev`.

1. Add `SGC_MA_JOB_DEFINITION=sgc-gwas-metal-job` to the `.env` file (QA first)
   and restart the service (`sudo systemctl restart dr-api-dev`). New MA runs
   submitted from the UI then use the METAL worker.

   The same variable also turns on munge gating in candidate selection
   (`sgc_ma.select.munge_required()`). With it set, only files with a
   `SUCCEEDED` munge result are MA candidates, both in the launch preview and in
   `submit_ma_batch`. Without it (the prod default, `sgc-gwas-ma-job`), selection
   is unchanged and the old worker reads the raw files. `SGC_MA_REQUIRE_MUNGE`
   overrides this when set: `1`/`true`/`yes` turns gating on, any other value
   turns it off, whatever the job definition.
2. From the CLI, set it per invocation:

```bash
SGC_MA_JOB_DEFINITION=sgc-gwas-metal-job python -m sgc_ma.submit_ma_batch \
  --phenotype <phenotype> --ancestry <ancestry> --sex <sex> \
  --bucket <bucket> --db-name <db-name>
```

### Thresholds

METAL applies no MAF or INFO filter of its own. The munge stage has already
filtered every input. So the run's `maf_min`/`info_min` (from the launch form,
default 0.005/0.3) must equal the thresholds recorded on each munged input's
`sgc_gwas_munge_results` row. If any munged input differs, the worker fails
straight away, before downloading anything, with a message that names the run's
thresholds and the first mismatching dataset. The fix is to re-munge with
`--force` or to launch with matching thresholds. Note that
`sgc_munge/submit_munge_batch.py` runs the munge worker with its defaults
(MAF >= 0.005, INFO >= 0.3), so in practice that means launching with those
values. The filter caveat in `summary.json` shows the thresholds that were
actually applied.

## Rollback

Remove `SGC_MA_JOB_DEFINITION` from the `.env` file and restart the API service.
Submissions go back to the default `sgc-gwas-ma-job`, and candidate selection
stops requiring munged files (unless `SGC_MA_REQUIRE_MUNGE` is set).

## Output layout

Everything is written under `sgc/ma/{phenotype}/{ancestry}/{run_id}/`:

| File | Contents |
| --- | --- |
| `metal.tbl.gz` | METAL's combined results, every variant (gzip). When only some cohorts report EAF, its four frequency columns come from the frequency pass (see `freq_source`) |
| `metal.tbl.info` | METAL's column descriptions and input file list (see below) |
| `metal.log` | METAL's console log |
| `metal.txt` | The METAL script that was run |
| `metal_freq.tbl.gz`, `metal_freq.tbl.info`, `metal_freq.log`, `metal_freq.txt` | The frequency pass: METAL over only the cohorts that report EAF. Written only when some, but not all, used cohorts report EAF |
| `manhattan.png`, `qq.png` | Plots |
| `summary.json`, `summary.tsv` | Run summary (counts, genomic inflation lambda, per-cohort table) |
| `top_loci.tsv` | Lead variants of the top loci |

If a run fails, `metal.log`, `metal.txt`, `metal_freq.log` and `metal_freq.txt`
are still uploaded when METAL got far enough to write them.

The plots, lambda and top loci use only variants present in at least 2 cohorts;
`metal.tbl.gz` keeps all variants, including single-cohort ones.

`summary.json` also has these fields:

- `freq_source`: where the frequency columns come from. `pass1` means every used
  cohort reports EAF, so they come from the main pass. `pass2_eaf_cohorts` means
  they come from the frequency pass. `none` means no cohort reports EAF, so they
  are `NA`. The matching caveat says the same.
- `metal_runs`: `{"main": {...}, "freq": {...} or null}`, each with
  `metal_elapsed_s` and `metal_peak_rss_mb`. The RSS figure is the peak over every
  child process so far, so `freq` reports the larger of the two passes.
- `per_cohort[].n_markers_processed`: the rows METAL kept from each cohort, taken
  from its `Processed N markers` log line.
- `per_cohort[].n_allele_mismatch`: the rows METAL dropped with
  `Bad alleles for marker ...` because their allele strings differ from the
  marker's first occurrence. The usual cause is an indel reported in another
  form, such as `TA/T` for `AT>A`: munge normalises `SNP_ID` but keeps the
  reported alleles. When the total is above 0, a caveat reports it.

### `top_loci.tsv`

`beta` and `freq1` are the effect and frequency of `alt` (the allele in
`MarkerName`, `chrom:pos:ref:alt`). They are not oriented to METAL's `Allele1`:
METAL sorts each marker's alleles, so `Allele1` is the reference allele for about
half of the SNPs. `orientation` is `alt` when the worker could match `Allele1` to
`ref` or `alt`. For an indel reported in another form, the longer reported allele
is matched to the longer of `ref` and `alt`. When no match is possible, for
example with equal-length alleles that differ, `orientation` is `unresolved` and
`beta`/`freq1` are METAL's, for `allele1`. `allele1`/`allele2` are METAL's alleles
as printed.

METAL prints p-values below the double-precision range (for example
`6.41e-4346`). `pvalue_str` keeps METAL's string and `log10p` is computed exactly
from it. Significance and lead selection use `log10p`. `pvalue` (and the plots
and lambda) clamp such values to the smallest positive double, about `4.9e-324`.

## Reading `metal.tbl.info`

METAL writes `metal.tbl.info` alongside the table. It has two parts:

1. A description of each column in `metal.tbl.gz`, in order: `MarkerName`,
   `Allele1`/`Allele2`, `Freq1`, `FreqSE`, `MinFreq`, `MaxFreq`, `Effect`,
   `StdErr`, `P-value`, `Direction`, the heterogeneity columns (`HetISq`,
   `HetChiSq`, `df`, `HetPVal`), and the custom variables `Ncases` and
   `Ncontrols`. These two are listed only as "custom variable 1/2"; they are the
   per-marker sums of the cohorts' `N_CAS` and `N_CON`.
2. The input files, numbered in `PROCESS` order:

```text
# Input for this meta-analysis was stored in the files:
# --> Input File 1 : work/A_fa.tsv.gz
# --> Input File 2 : work/B_fb.tsv.gz
```

Each file is `work/<dataset>_<file_id>.tsv.gz`, in dataset order. The
`Direction` column has one character per input file, in this order: `+`, `-`,
`0`, or `?` when the marker is absent from that cohort or METAL dropped the row.

The file does not record the scheme, the thresholds or genomic control. Those
are in `metal.txt` (`SCHEME STDERR`, no `GENOMICCONTROL`) and in `summary.json`.
Two of METAL's descriptions are misleading. `Allele1`/`Allele2` are described as
"the first allele ... in the first file where it occurs", but METAL actually
sorts each marker's alleles. And `Effect` and `Freq1` refer to `Allele1`, not to
the cohorts' effect allele. `metal_freq.tbl.info` lists only the cohorts that
report EAF.

## Verifying the image

```bash
docker run --rm --entrypoint metal sgc-gwas-metal < /dev/null | head -5   # METAL banner
docker run --rm --entrypoint cat sgc-gwas-metal /usr/local/share/metal_commit.txt  # METAL commit
docker run --rm --entrypoint python sgc-gwas-metal \
  -c "import sgc_metal.tbl, sgc_metal.script, dataregistry.api.db; print('ok')"
```
