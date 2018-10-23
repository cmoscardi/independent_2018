.PHONY: radar radar_raw

months = 01 02 03 04 05 06 07 08 09
years = 2017 2018
targets = $(foreach year, $(years), $(foreach month, $(months), $(year)-$(month)))

$(info the targets are $(targets))

$(targets): data/%:
	mkdir -p data/$@

$(targets): data/%/.raw_done: data/%
	touch data/$@/.raw_done

data/09/.raw_done:
	mkdir -p data/09
	touch data/09/.raw_done

data/09/30/NWS_NEXRAD_NXL2DPBL_KOKX_20170930230000_20170930235959.tar: data/09/.raw_done
	for date in `seq -w 01 30`; do mkdir data/09/$$date; gsutil -m cp gs://gcp-public-data-nexrad-l2/2017/09/$$date/KOKX/* ./data/09/$$date/; done

data/09/.extracted_done:
	touch data/09/.extracted_done

data/09/30/KOKX20170930_235509_V06_MDM.ar2v: data/09/.extracted_done
	for date in `seq -w 01 30`; do cat data/09/$$date/*.tar | tar -C data/09/$$date/ -xvf - -i; done

radar_raw: data/09/30/NWS_NEXRAD_NXL2DPBL_KOKX_20170930230000_20170930235959.tar

radar: radar_raw data/09/30/KOKX20170930_235509_V06_MDM.ar2v
