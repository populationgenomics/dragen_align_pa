from collections.abc import Callable
from functools import partial

import cpg_utils
from cpg_flow.targets import Cohort

from dragen_align_pa.jobs import run_align_genotype_with_dragen
from dragen_align_pa.jobs.ica_pipeline_manager import manage_ica_pipeline_loop


def _submit_new_ica_pipeline(
    sg_name: str,
    cram_ica_fids_path: cpg_utils.Path | None,
    fastq_csv_list_file_path: cpg_utils.Path | None,
    fastq_ids_path: cpg_utils.Path | None,
    individual_fastq_file_list_paths: cpg_utils.Path | None,
    analysis_output_fid_path: cpg_utils.Path,
) -> str:
    ica_pipeline_id: str = run_align_genotype_with_dragen.run(
        cram_ica_fids_path=cram_ica_fids_path,
        fastq_csv_list_file_path=fastq_csv_list_file_path,
        fastq_ids_path=fastq_ids_path,
        analysis_output_fid_path=analysis_output_fid_path,
        individual_fastq_file_list_paths=individual_fastq_file_list_paths,
        sg_name=sg_name,
    )
    return ica_pipeline_id


def run(
    cohort: Cohort,
    outputs: dict[str, cpg_utils.Path],
    cram_ica_fids_path: dict[str, cpg_utils.Path] | None,
    analysis_output_fids_path: dict[str, cpg_utils.Path],
    fastq_csv_list_file_path: cpg_utils.Path | None,
    fastq_ids_path: cpg_utils.Path | None,
    individual_fastq_file_list_paths: dict[str, cpg_utils.Path] | None,
) -> None:
    """
    Calls the generic pipeline manager with settings for the main Dragen pipeline.
    """

    def _create_submit_callable(sg_name: str) -> Callable[[], str]:
        """Creates a zero-argument callable for pipeline submission."""
        return partial(
            _submit_new_ica_pipeline,
            sg_name=sg_name,
            cram_ica_fids_path=cram_ica_fids_path[sg_name] if cram_ica_fids_path else None,
            fastq_csv_list_file_path=fastq_csv_list_file_path,
            fastq_ids_path=fastq_ids_path,
            analysis_output_fid_path=analysis_output_fids_path[sg_name],
            individual_fastq_file_list_paths=individual_fastq_file_list_paths[sg_name]
            if individual_fastq_file_list_paths
            else None,
        )

    pipeline_id_key = '{target_name}_pipeline_id_and_arguid'

    manage_ica_pipeline_loop(
        targets_to_process=cohort.get_sequencing_groups(),
        outputs=outputs,
        pipeline_name='Dragen',
        is_mlr_pipeline=False,
        success_file_key_template='{target_name}_success',
        pipeline_id_file_key_template=pipeline_id_key,
        error_log_key=f'{cohort.name}_errors',
        submit_function_factory=_create_submit_callable,
        allow_retry=True,
        sleep_time_seconds=600,
    )
