

### Useful ffmpeg command
(This doesn't work, need to figure out how to order the input files)
```
ffmpeg -framerate 24 -i '%*_d6_%d.png' -vcodec libx264 -crf 24 -vf scale=640:-1 j25_d6_vid.mp4
```

```
cat `ls *d6*.png | sort -t '_' -k2` | ffmpeg -f image2pipe -i pipe:.png -framerate 24 -vcodec libx264 -crf 24 -vf scale=640:-1 ~/s04_d6_V2.mp4
```

### Bad weather night
04-03-2017

### Sjoin performance notes
Default, no parallelization: 11654 seconds
