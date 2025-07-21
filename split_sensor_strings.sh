#!/bin/bash
source /opt/applications/miniconda3/etc/profile.d/conda.sh
conda activate csv_slicer

for dir in $(ls -d /srv/data/erddap/cmar/nsodp-sensor-strings/) ; do
    dataset_id="$(basename $dir)"
    echo Working on dataset "$dataset_id"
    
    find $dir/data_by_station -type f -name "*_final.csv" -print0 | while read -d $'\0' file
    do
        echo The contents of the directory are "$file"
        file_path="$(dirname $file)"
        python csv_slicer.py "$file" --column timestamp_utc --output $dir/final_output/%Y/ --filename_format "$dataset_id"_%Y-%m-%d.csv --dtype-file /opt/applications/cmar/CMAR-to-CIOOS-data-converter/data_types.yaml --smooth_timestamps
    done

done


conda deactivate
