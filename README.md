# dragen_align_pa
Realign CRAM files using Dragen 3.7.8 via ICA

## Purpose

When migrating workflows from production-pipelines, this template respository structure can be used to start with a
sensible directory structure, and some suggested conventions for naming and placement of files.

```commandline
src
├── dragen_align_pa
│   ├── __init__.py
│   ├── config_template.toml
│   ├── jobs
│   │   └── LogicForAStage.py
│   ├── main.py
│   ├── stages.py
│   └── utils.py
```

`dragen_align_pa` occurs in a number of places ([pyproject.toml](pyproject.toml), [src](src), and the workflow name in the
template config file). It is intended that you remove this generic placeholder name, and replace it with the name of
your workflow.

`stages.py` contains Stages in the workflow, with the actual logic imported from files in `jobs`.

`stages.py` also links to the Pipeline Naming Conventions document, containing a number of recommendations for naming
Stages and other elements of the workflow.

`config_template.toml` is a template, indicating the settings which are mandatory for the pipeline to run. In
production-pipelines, many of these settings were satisfied by the cpg-workflows or per-workflow default TOML files. If
a pipeline is being migrated from production-pipelines, the previous default config TOML would be a better substitute.
