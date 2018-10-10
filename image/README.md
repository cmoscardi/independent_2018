

### Useful ffmpeg command
(this still needs rotation, ideally)
```
cat `ls *d6*.png | sort -t '_' -k2` | ffmpeg -f image2pipe -i pipe:.png -framerate 24 -vcodec libx264 -crf 24 -vf scale=640:-1 ~/aug04_d6.mp4
```
(consider -vf "transpose=1")

### Bad weather night
04-03-2017

### Sjoin performance notes
Default, no parallelization: 11654 seconds
