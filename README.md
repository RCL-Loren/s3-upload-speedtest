# s3-upload-speedtest
Test S3 Upload speeds using both accelerated and non-accelerated endpoints

To see options run
`python3 s3-upload-speed.py -help`

Uses dotenv to protect AWS access keys.  Copy dotenv.tmpl to .env and modify. Do not put .env in repo. Default .gitignore should have .env, if not then add.

Example:
100 Megabyte Test
3 Runs
With both a CSV for run and cumulative CSV with summary results.

`python3 s3-upload-speed.py --bucket some-bucket-west --loglevel=INFO --csvsummaryfilename=5_5MBpsLinkRuns.csv --iter=3 --size=100m --csvfilename=100mx3-ew-5_5ideal.csv`