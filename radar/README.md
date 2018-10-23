## Useful links

- https://en.wikipedia.org/wiki/DBZ_(meteorology)
- https://www.timeanddate.com/weather/usa/brooklyn/historic?month=9&year=2017
- https://github.com/adokter/bioRad/blob/master/R/project_as_ppi.R#L98

## Useful one-liners
```
for date in {01..30}; do gsutil -m cp gs://gcp-public-data-nexrad-l2/2017/10/$date/KOKX/* ./$date/; done
for d in `ls`; do cd $d; for fname in `ls NWS_NEXRAD_NXL2DPBL_KOKX_201710[0-9][0-9]0{0..6}*`; do tar -xvf $fname; done; cd ..; done
```
