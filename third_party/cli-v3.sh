#!/bin/bash

#### Project parameters
XKEY='<api-key>'
PROJECTID='<project-id>'
OUTPUTFOLDERID='<output-folder-id>'

#### Pipeline variables
PIPELINEID='<pipeline-id>'

#### Input file IDs (replace with actual ICA file/folder IDs)
REFERENCE=fil.xxxxx                     # DRAGEN reference TAR

# -- Choose ONE input mode: CRAM or FASTQ (not both) --
# CRAM mode:
CRAM1=fil.xxxxx                         # CRAM file - sample 1
CRAM2=fil.yyyyy                         # CRAM file - sample 2
CRAM_REF=fol.xxxxx                      # Folder with FA.GZ + FAI (must match the reference used to encode CRAMs)
# FASTQ mode:
FASTQ1=fil.xxxxx                        # SampleA_S1_L001_R1_001.fastq.gz
FASTQ2=fil.yyyyy                        # SampleA_S1_L001_R2_001.fastq.gz
FASTQ3=fil.zzzzz                        # SampleB_S2_L001_R1_001.fastq.gz
FASTQ4=fil.aaaaa                        # SampleB_S2_L001_R2_001.fastq.gz
FASTQ_LIST=fil.bbbbb                    # (optional) pre-built fastq_list.csv

#### Optional input file IDs
QC_COV_BED1=fil.xxxxx                   # QC coverage region BED 1 (e.g. target_regions.bed)
QC_COV_BED2=fil.yyyyy                   # QC coverage region BED 2 (e.g. coding_regions.bed)
QC_COV_BED3=fil.zzzzz                   # QC coverage region BED 3 (e.g. hotspot_regions.bed)
QC_CROSS_CONT=fil.xxxxx                 # QC cross-contamination VCF
ADDITIONAL_FILE1=fil.xxxxx              # e.g. target.bed for WES
ADDITIONAL_FILE2=fil.yyyyy              # e.g. cnv_pon.combined.counts.txt.gz for WES CNV


############################################################################################################
### Enter the project
icav2 --x-api-key $XKEY projects enter $PROJECTID


############################################################################################################
### Example 1: WGS with FASTQ input (2 samples)
############################################################################################################
icav2 --x-api-key $XKEY projectpipelines start nextflow $PIPELINEID \
--user-reference "WGS-FASTQ-2samples" \
--storage-size small \
--input fastqs:${FASTQ1},${FASTQ2},${FASTQ3},${FASTQ4} \
--input ref_tar:$REFERENCE \
--input qc_coverage_region_beds:${QC_COV_BED1},${QC_COV_BED2},${QC_COV_BED3} \
--input qc_cross_cont_vcf:$QC_CROSS_CONT \
--project-id $PROJECTID \
--output-parent-folder $OUTPUTFOLDERID \
--parameters enable_map_align:true \
--parameters output_format:CRAM \
--parameters enable_variant_caller:true \
--parameters enable_sv:true \
--parameters enable_cnv:true \
--parameters dragen_reports:true \
--parameters error_strategy:auto \
--parameters additional_args:"--cnv-enable-self-normalization true --read-trimmers polyg --soft-read-trimmers none"


############################################################################################################
### Example 2: WGS with CRAM input (2 samples)
############################################################################################################
icav2 --x-api-key $XKEY projectpipelines start nextflow $PIPELINEID \
--user-reference "WGS-CRAM-2samples" \
--storage-size small \
--input crams:${CRAM1},${CRAM2} \
--input cram_reference:$CRAM_REF \
--input ref_tar:$REFERENCE \
--input qc_coverage_region_beds:${QC_COV_BED1},${QC_COV_BED2},${QC_COV_BED3} \
--input qc_cross_cont_vcf:$QC_CROSS_CONT \
--project-id $PROJECTID \
--output-parent-folder $OUTPUTFOLDERID \
--parameters enable_map_align:true \
--parameters output_format:CRAM \
--parameters enable_variant_caller:true \
--parameters enable_sv:true \
--parameters enable_cnv:true \
--parameters dragen_reports:true \
--parameters error_strategy:auto \
--parameters additional_args:"--cnv-enable-self-normalization true"


############################################################################################################
### Example 3: WES with FASTQ input + target BED + CNV PON
### Note: --cnv-enable-self-normalization is NOT used for WES (PON is required instead)
############################################################################################################
icav2 --x-api-key $XKEY projectpipelines start nextflow $PIPELINEID \
--user-reference "WES-FASTQ-5samples" \
--storage-size small \
--input fastqs:${FASTQ1},${FASTQ2},${FASTQ3},${FASTQ4} \
--input fastq_list:$FASTQ_LIST \
--input ref_tar:$REFERENCE \
--input qc_coverage_region_beds:${QC_COV_BED1},${QC_COV_BED2} \
--input qc_cross_cont_vcf:$QC_CROSS_CONT \
--input additional_files:${ADDITIONAL_FILE1},${ADDITIONAL_FILE2} \
--project-id $PROJECTID \
--output-parent-folder $OUTPUTFOLDERID \
--parameters enable_map_align:true \
--parameters output_format:CRAM \
--parameters enable_variant_caller:true \
--parameters enable_sv:true \
--parameters enable_cnv:true \
--parameters dragen_reports:true \
--parameters error_strategy:auto \
--parameters additional_args:"--vc-target-bed target.bed --sv-exome true --sv-call-regions-bed target.bed --cnv-target-bed target.bed --cnv-combined-counts cnv_pon.combined.counts.txt.gz"
