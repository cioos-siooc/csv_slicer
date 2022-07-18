# CSV Slicer

A collection of scripts for slicing, merging and manipulating CSV files, typically based on date/time as specified by the user.

## csv_slicer.py

For slicing one or more large CSV file into many smaller files based on date/time.  In the process of slicing the files the columns may be re-ordered & renamed, date/time formats and timezones can be adjusted, empty columns can be dropped and the final layout of the resulting files can be split out into subdirectories based on date information.

```console
usage: csv_slicer.py [-h] [-o OUTPUT] [-f FILENAME_FORMAT] [-c COLUMN] [-k]
                     [-m METHOD] [-n NAMES] [-t COLUMN_NAMES] [-d DATA_BEGINS]
                     [-x DROP_COLUMNS] [-z ADJUST_TZ]
                     [--date-format-out DATE_FORMAT_OUT]
                     source_file

positional arguments:
  source_file           Source CSV file to be sliced into smaller files. This
                        argument also accepts wildcards to allow the
                        processing of multiple files in one call. NOTE: All
                        files should have the same CSV structure.

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Destination directory for output files, default: .
  -f FILENAME_FORMAT, --filename_format FILENAME_FORMAT
                        How output file names should be formatted, can and
                        should contain date formatting string. Example:
                        test_data_%Y-%m-%d.csv
  -c COLUMN, --column COLUMN
                        Column name that contains the date/time information to
                        slice the data on. Can accept an integer (zero-based),
                        column name or a column name with what it should be
                        renamed to (old_name:new_name). Default: 0
  -k, --keep-empty      Override default behaviour to drop unnamed columns
                        from final output. NOTE: The process occurs before the
                        column names are assigned. As a result, this setting
                        can complement or interfere with the column names
                        option depending on how it's used.
  -m METHOD, --method METHOD
                        How column should be used to split data by date.
                        Specify in terms for python date formatting string.
                        Example: date:%Y%m%d
  -n NAMES, --names NAMES
                        Row that contains column names, default: 0
  -t COLUMN_NAMES, --column-names COLUMN_NAMES
                        A comma separated list of values that will be assigned
                        to the columns in order. NOTE: This operation occurs
                        AFTER the dropping of "Unnamed" columns. If you have
                        columns, without names, that you want to assign using
                        this option, use the --keep-empty flag to ensure they
                        aren't dropped.
  -d DATA_BEGINS, --data-begins DATA_BEGINS
                        Row number that contains beginning of data (zero-
                        based). If the file is more complex and requires
                        multiple rows to be skipped a comma separated list can
                        be supplied, default: None
  -x DROP_COLUMNS, --drop-columns DROP_COLUMNS
                        List of columns to drop from the output
  -z ADJUST_TZ, --adjust-tz ADJUST_TZ
                        Specifies how date/times should be adjusted, in hours,
                        and what timezone the data should be localized to.
                        Example: 3.5:UTC
  --date-format-out DATE_FORMAT_OUT
                        Specifies how date/times should be formatted in the
                        resulting files. By default, this uses ISO 8601:
                        %Y-%m-%dT%H:%M:%S%z

```

## csv_partition.py

A script to split a CSV file based on the unique values of a specified column.  The source file need not follow a uniform CSV structure, new files will be created based on the unique values of the source column and new sets of column names can be mapped to each unique value.

```console
usage: csv_partition.py [-h] [-c COLUMN] [-f FORMAT] [-l LABELS] [-o OUTPUT]
                        source_file

positional arguments:
  source_file           Source file to be partitioned

optional arguments:
  -h, --help            show this help message and exit
  -c COLUMN, --column COLUMN
                        Column that contains unique values to partition into
                        sepearate files
  -f FORMAT, --format FORMAT
                        A JSON formatted list of columns (zero-based) and how
                        they should be formatted according to python string
                        formatting - applies to ALL rows before partitioning.
                        Example: {"0":{"type":"float","output":"{:1.2f}"}}
                        would format the value in the first column to 2
                        decimal places.
  -l LABELS, --labels LABELS
                        A JSON formatted list of column labels coresponding to
                        1 or more of the values related to the partition
                        column. Example: {"value1":["col_a","col_b", "col_c"],
                        ... ,"valueN":["label_a","label_b"]}
  -o OUTPUT, --output OUTPUT
                        Path to store generated output files
```

## csv_convert_date.py

A script to manipulate date/time information of a CSV file, this can be altering the format of the source file and/or adjusting of timezones.

```console
usage: csv_convert_date.py [-h] [-o OUTPUT] [-f FILENAME_FORMAT] [-n NAMES]
                           [-d DATA_BEGINS] [-t TIMESTAMP] [-c COLUMN]
                           [-i IN_FORMAT] [-w OUT_FORMAT] [-p POSITION]
                           [-x DROP_COLUMNS] [-z ADJUST_TZ]
                           source_file

positional arguments:
  source_file           Source CSV file to be sliced into smaller files. This
                        argument also accepts wildcards to allow the
                        processing of multiple files in one call. NOTE: All
                        files should have the same CSV structure.

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Destination directory for output files, default: .
  -f FILENAME_FORMAT, --filename_format FILENAME_FORMAT
                        How output file names should be formatted, can and
                        should contain date formatting string. Example:
                        test_data_%Y-%m-%d.csv
  -n NAMES, --names NAMES
                        Row that contains column names, default: 0
  -d DATA_BEGINS, --data-begins DATA_BEGINS
                        Row that contains beginning of data, default: None
  -t TIMESTAMP, --timestamp TIMESTAMP
                        Boolean to tell pandas to parse dates (default: true)
                        or a JSON formatted list of columns that can be
                        combined to create a timestamp. NOTE: refer to the
                        pandas read_csv() parse_dates option for further
                        details.
  -c COLUMN, --column COLUMN
                        Name of the new timestamp column
  -i IN_FORMAT, --in-format IN_FORMAT
                        Format of the timestamp column for date/time parsing
  -w OUT_FORMAT, --out-format OUT_FORMAT
                        Output format of the timestamp column after it has
                        been parsed
  -p POSITION, --position POSITION
                        Index position of the newly created timestamp column.
                        Use -1 to append to the end, Default: 0
  -x DROP_COLUMNS, --drop-columns DROP_COLUMNS
                        List of columns to drop from the output
  -z ADJUST_TZ, --adjust-tz ADJUST_TZ
                        Specifies how date/times should be adjusted, in hours,
                        and what timezone the data should be localized to.
                        Example: 3.5:UTC
```

## csv_merge.py

A script for merging multiple CSV files into one file assuming that all files share the same structure and column headers.

```console
usage: csv_merge.py [-h] [-c COLUMN] [-s SORT] [-o OUTPUT]
                    source_files [source_files ...]

positional arguments:
  source_files          Source file to be partitioned, minimum two files, can
                        accept wildcards. Headers from the first file will be
                        assigned to all subsequent files for consistency.

optional arguments:
  -h, --help            show this help message and exit
  -c COLUMN, --column COLUMN
                        Column that contains unique values to merge and filter
                        on
  -s SORT, --sort SORT  Column that contains the value to sort rows on and
                        direction (ASC or DESC). Default: 0|ASC
  -o OUTPUT, --output OUTPUT
                        Path to store generated output files
```
