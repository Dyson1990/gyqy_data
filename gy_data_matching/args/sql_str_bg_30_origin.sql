SELECT 
    F_QY_JBXX.ENTID                                   --JBXX中的企业ID
    , F_QY_JBXX.DISTRICT                              --JBXX中的省地县码
    , F_QY_BG.ALTBE                                   --BG中的变更前内容
    , F_QY_BG.ALTAF                                   --BG中的变更后内容
    , F_QY_BG.ALTDATE                                 --BG中的变更时间
FROM F_QY_JBXX
LEFT JOIN F_QY_BG                                     --JBXX左连接BG
ON F_QY_JBXX.ENTID = F_QY_BG.ENTID
WHERE ({where_district_str})                          --筛选市码
AND F_QY_BG.ALTITEM = '30'