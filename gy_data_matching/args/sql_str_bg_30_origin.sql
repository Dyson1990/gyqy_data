-- -*- db_type:oracle -*-
SELECT 
    F_QY_JBXX.ENTID
    , SUBSTR(F_QY_JBXX.DISTRICT, 0, {district_code_len}) DISTRICT
    , F_QY_BG.ALTBE
    , F_QY_BG.ALTAF
    , F_QY_BG.ALTDATE 
FROM F_QY_JBXX
LEFT JOIN F_QY_BG
ON F_QY_JBXX.ENTID = F_QY_BG.ENTID
WHERE SUBSTR(F_QY_JBXX.DISTRICT, 0, {district_code_len}) = {district_code}
AND F_QY_BG.ALTITEM = '30'