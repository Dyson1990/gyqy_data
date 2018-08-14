-- 获取F_QY_BG中的企业对应县码的全表
SELECT
    DISTINCT(F_QY_JBXX.DISTRICT)
FROM F_QY_JBXX
INNER JOIN F_QY_BG                                     
ON F_QY_JBXX.ENTID = F_QY_BG.ENTID