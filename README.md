# Dragen Align PA
Realign CRAM files using Dragen 3.7.8 via ICA.

## Purpose

This workflow performs a realignment of CRAM files to Dragen 3.7.8. This is to be compatible with other large biobanks that are used for population analysis.

## Running the workflow
- Create a custom cohort in Metamist that contains the samples that you wish to realign.
    - Every sample in the cohort must have been aligned with the same initial reference genome. Do not mix samples with different initial reference genomes into the same cohort, as this will cause Dragen to fail. See [Upload a reference to ICA](#upload-a-reference-to-ica) if you need a new reference genome for realignment.
- Create a config file to run the steps you want. See [Setting up a config file](#setting-up-a-config-file) for more information.
- Run the workflow via [analysis runner](#example-analysis_runner-command)

## Cancelling a running pipeline
Due to the disconnect between Hail Batch and ICA, cancelling a running workflow is a little more involved.
1. Cancel the pipeline run in Hail Batch. Note that this _by itself_ will not cancel any running jobs in ICA.
2. Change the value of `cancel_cohort_run` in your config file from `false` to `true`.
3. Submit a new pipeline run with `analysis-runner` with your updated config. This will cancel any in-progress and pending runs in ICA.

## Reconnecting to an ongoing run in ICA
If the main pipeline in Hail Batch fails, this doesn't cause the pipelines in ICA to fail. You can reconnect to these ongoing runs by making the following edits to your config file.
1. Make sure that `cancel_cohort_run` is set to `false`
2. Change the value of `monitor_previous` to `true`
3. Submit a new pipeline run with `analysis-runner`

### Setting up a config file
The config file [dragen_align_pa_defaults.toml](config/dragen_align_pa_defaults.toml) lists all the options that are used for running the realignment workflow.
NOTE: This default config is _not used by default_ and is only provided as a reference for you to write a config file for the cohort that you wish to run.

The following is a list of config values that you might wish to change for your own pipeline run:
- workflow
    - `input_cohorts`: change to your cohort IDs
    - `last_stages`: the default `DownloadDataFromIca` will run the pipeline end-to-end. You can choose any stage from the stages list `[PrepareIcaForDragenAnalysis, UploadDataToIca, ManageDragenPipeline, DownloadCramFromIca, DownloadGvcfFromIca, DownloadDatoFromIca]`
- images
    - ica: set the image tag to the tag you wish to use for the pipeline run. Each image tag should correspond to a code tag in github.
- ica
    - management
        - `cancel_cohort_run`: usually, but not always `false`
        - `monitor_previous`: defaults to `false` but can be set to `true` on new runs, as the behaviour is to start a new run for a sample if an ongoing run is not found
    - data_prep
        - `upload_folder`: where you want to upload data for your cohort (will be created by the pipeline in ICA)
        - `output_folder`: where ICA Dragen pipeline outputs will be written IN ICA (not GCP). Will be created by the pipeline
    - tags
        - `technical_tags`: a list of any technical tags you want to add to the sample (ICA only)
        - `user_tags`: a list of user tags you want to add to the sample (ICA only)
        - `reference_tags`: a list of reference tags you want to add to the sample (ICA only)

### Example analysis_runner command
The pipeline code is installed into the Docker image, and an entrypoint is defined as `dragen_align_pa`. Therefore, the pipeline can be invoked as follows
```commandline
analysis-runner \
--dataset <dataset> \
--access test \
--config config.toml \
--output-dir ica-test \
--description "Description for analysis-runner" \
--image "australia-southeast1-docker.pkg.dev/cpg-common/images-dev/dragen_align_pa:1.0.0-1" \
dragen_align_pa
```
Note that the `--output-dir` flag is required by `analysis-runner`, but is not used in this workflow. No outputs are written to the location, so any placeholder value can be entered here.

### Upload a reference to ICA
The current references in ICA are:
- `Homo_sapiens_assembly38_masked`
- `Homo_sapiens_assembly38`

To upload a new reference from your computer the following steps need to be taken:
- Compress the reference with `bgzip`, ensuring that the file suffix is `.gz`
- Index the reference with `samtools faidx`
- Create a new folder in ICA and upload the reference + index into the folder.
- Use the folder ID as the reference value in your config file (this can be found by navigating into the folder in ICA and clicking `Folder details` on the right of the window, above any data entries).

## Development
Developing the `dragen_align_pa` pipeline differs from the old process in `production_pipelines` and follows a number of new suggested best practices.

### The workflow runner file
[run_workflow.py](src/dragen_align_pa/run_workflow.py) is a much simpler re-implementation of the old production-pipelines `main.py`. It defines only a single pipeline (here `dragen_align_pa`), lists all end stages, and calls the `run_workflow` method.

### The stages file
The stages file [stages.py](src/dragen_align_pa/stages.py) defines all the separate stages that the workflow will run as python classes, with a `@stage` decorator in order to define stage dependencies and what is recorded in Metamist.

Each stage should define two methods: `expected_outputs` and `queue_jobs`. `queue_jobs` must call a single method that returns one of `BashJob, PythonJob` that is used for `self.make_outputs`.

There should be no logic contained within the stage, this should all be handed off to the individual job files.

### Job files
Each job file should implement at minimum tow methods. One should be a private method that is used to initalise a new job for the stage, and the other should be the public method that is called from `stages.py`. This public method must return the created job. PythonJobs should also define a private `_run` method that is called from the public method in the jobs file.
See [upload_data_to_ica.py](src/dragen_align_pa/jobs/upload_data_to_ica.py) for a simple example of a `BashJob` and [prepare_ica_for_analysis](src/dragen_align_pa/jobs/prepare_ica_for_analysis.py) for a simple example of a `PythonJob`.

### Repository Structure

```commandline
src
├── dragen_align_pa
│   ├── __init__.py
│   |── config
│   │   └── dragen_align_pa_defaults.toml
│   ├── jobs
│   │   ├── cancel_ica_pipeline_run.py
│   │   ├── download_ica_pipeline_outputs.py
│   │   ├── download_specific_files_from_ica.py
│   │   ├── manage_dragen_pipeline.py
│   │   ├── monitor_dragen_pipeline.py
│   │   ├── prepare_ica_for_analysis.py
│   │   ├── run_align_genotype_with_dragen.py
│   │   └── upload_data_to_ica.py
│   ├── run_workflow.py
│   ├── stages.py
│   └── utils.py
├── test
└── Dockerfile
```
