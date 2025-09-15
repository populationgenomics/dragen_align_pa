from cpg_flow.stage import CohortStage, SequencingGroupStage, StageInput, StageOutput
from cpg_flow.targets import Cohort, SequencingGroup


class IntakeQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> None:
        pass

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        pass


class PrepareIcaForAnalysis(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> None:
        pass

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        pass


class RunAndMonitorDragenFastQ(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> None:
        pass

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        pass


class RunAndMonitorDragenMlrFastQ(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> None:
        pass

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        pass


class DownloadDragenResultsFastQ(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> None:
        pass

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        pass


class MultiQcFastQ(SequencingGroupStage):
    def expected_outputs(self, sequencing_group: SequencingGroup) -> None:
        pass

    def queue_jobs(self, sequencing_group: SequencingGroup, inputs: StageInput) -> StageOutput | None:
        pass
