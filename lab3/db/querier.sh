#!/usr/bin/env bash
function mapId() {
    cut -d$',' -f $1 $2 | sort | uniq | awk -F"," -v OFS=',' '
      {print (NR-1), $0}
    '> $3
}
function mapIdPipe() {
    cut -d$',' -f $1 | sort -S 4G | uniq | awk -F"," -v OFS=',' '
      {print (NR-1), $0}
    '
}
function splitFile() {
    sed 's/<SEP>/,/g' $1
}

function sortFile() {
    LC_ALL='C' sort --parallel=4 -t$',' -k $1 -S $2
}

function ranking() {
    sortFile 1 2G |
        uniq -c | sort -r -S 2G |
        head -n $1 | sort -k 2 -S 2G |
        sed -r 's/^[ \t]*//;s/[ \t]*$//' |
        sed -r 's/\s+/,/g'
}

function sortedBy() {
    sort -k $1 -nr -t$','
}
function sortByNumberColumnDesc() {
    sort -rn -S 3G -k $1 -t$','
}

#prepare data
# mapp tracks
splitFile unique_tracks.txt |
    cut -d$',' -f 2-4 |
    sortFile 1 500M | tee unique_tracks.csv |
    mapIdPipe 1-1 > song_id.csv # with tee empty file is produced - probably because of size...
join -1 1 -2 2 -o 2.1,1.2,1.3 -t$',' --nocheck-order unique_tracks.csv song_id.csv | sortFile 1 2G > mapped_tracks.csv

# map users, timestamps to dates
splitFile triplets_sample_20p.txt | # split on csv
    sortFile 1 4G | tee triplets.csv | # sort on user ID
    mapIdPipe 1-1 > mapped_users.csv  # map user ID
join -t$',' -1 1 -2 2 -o 2.1,1.2,1.3 --nocheck-order triplets.csv mapped_users.csv | # replace user ID with mapped values
    awk -F"," '{OFS=","; $3=(strftime("%Y-%m-%d", $3, 1)); print $0}' | # extract date from timestamp
    sortFile 2 4G > tirplets_by_song_id.csv # sort on song ID
join -1 2 -2 2 -o 1.1,2.1,1.3 -t$',' --nocheck-order tirplets_by_song_id.csv song_id.csv > triplets_mapped.csv
rm triplets.csv
rm tirplets_by_song_id.csv
rm unique_tracks.csv

# map dates
cat triplets_mapped.csv |
    sortFile 3 2G | tee triplets_by_date.csv |
    mapIdPipe 3-3 > dates_id.csv
join -1 3 -2 2 -o 1.1,1.2,2.1 -t$',' triplets_by_date.csv dates_id.csv > triplets.csv
# create columns instead of one field date
sed -i -e 's/-/,/g' dates_id.csv

# create songs ranking by listening with names - required later; higher cost due to high volume
join -1 2 -2 1 -t$',' -o 2.3,2.2,1.1 <(
    cut -d$',' -f 2 triplets.csv |
    sortFile 1 2G |
    uniq -c |
    sort -r -S 4G |
    sed -r 's/^[ \t]*//;s/[ \t]*$//' |
    sed -r 's/\s+/,/g'|
    sortFile 2 2G
    ) mapped_tracks.csv > songs_ranking.csv

# 1
cat songs_ranking.csv | sortByNumberColumnDesc 3 | head -n 10 > result1.csv
# 2
cut -d$',' -f 1-2 triplets.csv | sort -S 3G -u | cut -d$',' -f 1 | ranking 10 > users_ranking.csv
join -1 2 -2 1 -o 2.2,1.1 -t$',' users_ranking.csv <(cat mapped_users.csv | sortFile 1 2G) | sortedBy 2 > result2.csv

# 3
cut -d$',' -f 2-3 songs_ranking.csv |
    sortFile 1 2g |
    awk  -F"," -v OFS=',' '{a[$1] += $2} END{for (i in a) print i, a[i]}' |
    sortByNumberColumnDesc 2 |
    head -n 1 > result3.csv
# 4
cut -d$',' -f 3 triplets.csv | sort -S 2G > listenings_in_month.csv
cat dates_id.csv | sortFile 1 2G > sorted_dates.csv
join -1 1 -2 1 -o 2.3 -t$',' listenings_in_month.csv sorted_dates.csv |
 ranking 12 | awk -F"," -v OFS=',' '{ print $2 "," $1}' > result4.csv
# 5
join -1 1 -2 3 -t$',' <(grep ",Queen," songs_ranking.csv |
    sortByNumberColumnDesc 3 |
    head -n 3 |
    cut -d$',' -f 1 |
    sort) <(grep ",Queen," mapped_tracks.csv | sortFile 3 2G) | cut -d$',' -f 2 > queen_mp_songs.csv

join -1 1 -2 2 -o 2.1 -t$',' <(sort queen_mp_songs.csv) <(cut -d$',' -f 1-2 triplets.csv | sortFile 2 3G | uniq) |
        sortFile 1 2G |
        uniq -c | sort -r -S 2G |
        sed -r 's/^[ \t]*//;s/[ \t]*$//' |
        sed -r 's/\s+/,/g' |
        grep "^3," |
        cut -d$',' -f 2 |
        sort > queen_fans.csv

join -1 1 -2 1 -o 2.2 -t$',' queen_fans.csv <(cat mapped_users.csv | sortFile 1 2G) |
        sort | head -n 10 > result5.csv