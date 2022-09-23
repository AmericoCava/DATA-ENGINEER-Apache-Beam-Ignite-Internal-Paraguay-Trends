#!/usr/bin/env bash
python main.py \
--fecha 2022-08-18 \
--runner DataflowRunner \
--project dfa-dna-ws0020-la-prd-a1b1 \
--temp_location gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Trends/tmp/ \
--region northamerica-northeast1 \
--subnetwork https://www.googleapis.com/compute/v1/projects/dfa-dna-ws0020-la-prd-a1b1/regions/northamerica-northeast1/subnetworks/dfa-dna-ws0020-la-prd-dataflow \
--no_use_public_ips \
--service_account_email ide-sa@dfa-dna-ws0020-la-prd-a1b1.iam.gserviceaccount.com \
--setup_file ./setup.py \
--experiments=shuffle_mode=service \
--job_name ignite-portfolio-trends-$(date '+%Y-%m-%d') \
--save_main_session