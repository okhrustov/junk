#!/bin/bash

lock='/tmp/script.lock'
if [ -f $lock ]; then
exit 1
fi
touch $lock

SRC=/var/log/count/vertica/short
for file in $SRC/*.tsv ; do
    f=${file##*/}
    bin_name=${f%}
    cat $file > /tmp/tmp_file_m_s.tsv
    /opt/vertica/bin/vsql -h 192.168.xxx stats -c "CREATE EXTERNAL TABLE IF NOT EXISTS ext_counter_short (
    date           BIGINT NOT NULL default 0,
    cid            VARCHAR(200) NOT NULL default ' ',
    auid           VARCHAR(200) NOT NULL default ' ',
    bid            VARCHAR(200) NOT NULL default ' ',
    os             VARCHAR(50) NOT NULL default ' ',
    browser        VARCHAR(50) NOT NULL default ' ',
    country        VARCHAR(4) NOT NULL default ' ',
    city           VARCHAR(50) NOT NULL default ' ',
    utm_medium     VARCHAR(100) NOT NULL default ' ',
    utm_source     VARCHAR(100) NOT NULL default ' ',
    domain         VARCHAR(100) NOT NULL default ' ',
    uniq3_cba      INT NULL,
    uniq2_cb       INT NULL,
    uniq2_ca       INT NULL,
    uniq1_c        INT NULL,
    show           BIGINT NULL

) AS COPY FROM '/tmp/tmp_file_m_s.tsv' DELIMITER E'\t' EXCEPTIONS '/tmp/exceptions_m_counter_s.txt' REJECTED DATA '/tmp/rejected_m_counter_s.txt';
MERGE INTO counter_short tgt
USING ext_counter_short tabsrc
ON tabsrc.date = tgt.date
AND tabsrc.cid = tgt.cid
AND tabsrc.auid = tgt.auid
AND tabsrc.bid = tgt.bid
AND tabsrc.os = tgt.os
AND tabsrc.browser = tgt.browser
AND tabsrc.country = tgt.country
AND tabsrc.city = tgt.city
AND tabsrc.utm_medium = tgt.utm_medium
AND tabsrc.utm_source = tgt.utm_source
AND tabsrc.domain = tgt.domain
WHEN MATCHED THEN
UPDATE SET
uniq3_cba = tgt.uniq3_cba + tabsrc.uniq3_cba,
uniq2_cb = tgt.uniq2_cb + tabsrc.uniq2_cb,
uniq2_ca = tgt.uniq2_ca + tabsrc.uniq2_ca,
uniq1_c = tgt.uniq1_c + tabsrc.uniq1_c,
show = tgt.show + tabsrc.show
WHEN NOT MATCHED THEN
INSERT VALUES (tabsrc.date, tabsrc.cid, tabsrc.auid, tabsrc.bid, tabsrc.os, tabsrc.browser, tabsrc.country, tabsrc.city,
       tabsrc.utm_medium, tabsrc.utm_source, tabsrc.domain, tabsrc.uniq3_cba, tabsrc.uniq2_cb, tabsrc.uniq2_ca, tabsrc.uniq1_c, tabsrc.show);

DROP TABLE ext_counter_short;
COMMIT;";
if [ $? -eq 0 ]; then
    cat $file > /var/backup/count_tsv/`basename $file`
    rm -f $file
fi
    rm -f /tmp/tmp_file_m_s.tsv
    rm -f $lock
done
