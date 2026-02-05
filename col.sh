python collect.py \
  --db hn.sqlite \
  --poll-seconds 600 \
  --max-ids-per-poll 300 \
  --target-grace-seconds 3600 \
  --max-target-fetch-per-poll 400
