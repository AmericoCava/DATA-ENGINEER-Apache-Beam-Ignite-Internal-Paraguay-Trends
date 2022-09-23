#!/usr/bin/env bash
python main_cancel.py \
--fecha 2022-08-17 \
--runner DataflowRunner \
--project dfa-dna-ws0020-la-prd-a1b1 \
--temp_location gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/tmp/ \
--staging_location gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/ \
--region northamerica-northeast1 \
--subnetwork https://www.googleapis.com/compute/v1/projects/dfa-dna-ws0020-la-prd-a1b1/regions/northamerica-northeast1/subnetworks/dfa-dna-ws0020-la-prd-dataflow \
--no_use_public_ips \
--service_account_email ide-sa@dfa-dna-ws0020-la-prd-a1b1.iam.gserviceaccount.com \
--setup_file ./setup.py \
--experiments=shuffle_mode=service
--job_name cancel-$(date '+%Y-%m-%d') \