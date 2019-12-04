#!/usr/bin/env bash


# remove first line of both files
# SORT_V="$DIR/virtuoso-query-$j-result.txt.sorted"
# SORT_S="$DIR/sage-run-$i-query-$j-result.txt.sorted"
# SORT_S_OPT="$DIR/sage-opt-run-$i-query-$j-result.txt.sorted"
# cat "$DIR/virtuoso-query-$j-result.txt" | sed "s/\"//g" | awk 'NF' | sort > $SORT_V
# cat "$DIR/sage-run-$i-query-$j-result.txt" | awk 'NF' | sort > $SORT_S
# cat "$DIR/sage-opt-run-$i-query-$j-result.txt" | awk 'NF' | sort > $SORT_S_OPT
# d=$(diff -b --changed-group-format='%<%>' --unchanged-group-format="" "$SORT_V" "$SORT_S" | wc -l | awk '{print $1}')
# d2=$(diff -b --changed-group-format='%<%>' --unchanged-group-format="" "$SORT_V" "$SORT_S_OPT" | wc -l | awk '{print $1}')
# # remove temp files
# rm $SORT_V $SORT_S $SORT_S_OPT
# # print the result d1, aka: virtuoso vs sage normal
# if [ $d -eq 0 ]; then
#     echo "$j, 1" >> "$DIR/completeness-sage-run-$i.csv"
# else
#     echo "$j, 0" >> "$DIR/completeness-sage-run-$i.csv"
# fi
#
# # print the result of d2, aka: virtuoso vs sage optimized
# if [ $d2 -eq 0 ]; then
#     echo "$j, 1" >> "$DIR/completeness-sage-opt-run-$i.csv"
# else
#     echo "$j, 0" >> "$DIR/completeness-sage-opt-run-$i.csv"
# fi

VIRTUOSO_REFERENCE_FOLDER=$1
INPUT_FOLDER=$2
OUTPUT_FILE=$3


