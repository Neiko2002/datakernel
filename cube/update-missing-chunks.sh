#!/bin/bash

storage=/path/to/chunks

comm -13 \
<(find ${storage} -name '*.log' -type f -execdir echo '{}' ';' | sort) \
<(mysql -B -N -e "SELECT CONCAT(id,'.log') FROM aggregation_db_chunk" rtb | sort) \
| while read old; do (mysql -B -N -e "UPDATE aggregation_db_chunk SET aggregation_id=concat(aggregation_id, '.bak') WHERE id = ${old/.log/}" rtb); done