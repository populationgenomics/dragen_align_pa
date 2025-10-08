import json
import time

import cpg_utils
from cpg_utils.config import try_get_ar_guid
from icasdk import Configuration
from loguru import logger

from dragen_align_pa.jobs import monitor_dragen_pipeline, run_intake_qc_pipeline
from dragen_align_pa.utils import delete_pipeline_id_file


def manage_md5_pipeline(
    cohort_name: str,
    ica_fastq_ids: list[str],
    outputs: dict[str, cpg_utils.Path],
    api_root: str,
    api_config: Configuration,
    project_id: str,
    md5_outputs_folder_id: str,
) -> cpg_utils.Path:
    # Does a pipeline exist? If not, we need to submit one
    if not (md5_pipeline := outputs['md5sum_pipeline_run']).exists():
        ar_guid: str = try_get_ar_guid()
        # Trigger md5sum pipeline in ICA
        md5_pipeline_id: str = run_intake_qc_pipeline.run_md5_pipeline(
            cohort_name=cohort_name,
            ica_fastq_ids=ica_fastq_ids,
            api_config=api_config,
            project_id=project_id,
            ar_guid=ar_guid,
            md5_outputs_folder_id=md5_outputs_folder_id,
        )
        with md5_pipeline.open('w') as pipeline_fh:
            pipeline_fh.write(json.dumps({'pipeline_id': md5_pipeline_id, 'ar_guid': ar_guid}))

    # What is the status of the pipeline, did it fail and we need to resubmit?
    else:
        with open(md5_pipeline) as md5_fh:
            md5_pipeline_id: str = json.load(md5_fh)['pipeline_id']

        # Check status of pipeline
        md5_status: str = monitor_dragen_pipeline.run(ica_pipeline_id=md5_pipeline_id, api_root=api_root)

        while md5_status not in ['ABORTING', 'ABORTED', 'FAILED', 'FAILEDFINAL', 'SUCCEEDED']:
            if md5_status in ['ABORTING', 'ABORTED'] or md5_status in ['FAILED', 'FAILEDFINAL']:
                logger.info(f'md5 pipeline has status: {md5_status}')
                delete_pipeline_id_file(pipeline_id_file=str(md5_pipeline))
            elif md5_status == 'SUCCEEDED':
                return md5_pipeline
            else:
                time.sleep(300)
                md5_status = monitor_dragen_pipeline.run(ica_pipeline_id=md5_pipeline_id, api_root=api_root)

    return md5_pipeline
