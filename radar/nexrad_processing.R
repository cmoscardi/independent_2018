# This script iterates through a folder full of NEXRAD tar files, processing the contained ar2v scans to 
# ouput an hdf5 file, esri grid, csv, and jpeg image centered over Manhattan

library(bioRad)
library(ggplot2)
library(sp)

path <- "/Users/.."

# ------ 1. Loop through hourly tar files and extract 10 minute scan files

tar_files <- list.files(path=path, pattern=".tar", full.names = TRUE)

for (file in tar_files){
  # 1) extract ar2v from tar
  untar(file,exdir=path)
}

# ------ 2. Loop through 10 minute ar2v files and ouput hdf5 file, esri grid, csv, and jpeg

ar2v_files <- list.files(path=path, pattern=".ar2v", full.names = TRUE)

for (file in ar2v_files){
  # 1) convert to h5 and save
  h5 <- rsl2odim(file, paste(file,".hdf5",sep=""))
  # 2) read polar volume and extract lowest elevation scan
  test_pvol <- read.pvol(paste(file,".hdf5",sep=""))
  print("pvol read")
  testscan <- test_pvol$scans[[1]]
  # 3) create ppi plot over manhattan
  testppi <- ppi(testscan,cellsize=100,lonlim= c(-74.05,-73.85),latlim = c(40.68,40.85))
  print("ppi created")
  # 4) extract data from ppi
  spatialgrid = testppi$data
  print("grid extracted")
  # 5) save as csv
  write.csv(spatialgrid,paste(file,'.csv'))
  # 6) save as esri raster
  write.asciigrid(spatialgrid,paste(file,'.asc'))
  # 7) map the DBZH with a google maps basemap
  plotfile <- try(map(testppi,map=basemap,param="DBZH",alpha=0.4) + ggtitle(gsub('^.*KOKX\\s*|\\s*.ar2v.*$', '', file)))
  print("plotted")
  # 8) save image 
  try(ggsave(paste(file,".jpeg",sep=""),plotfile))
}

#basemap <- basemap(testppi)
