SELECT
    JBXX0.ENTID
    , JBXX0.ENTNAME
    , JBXX0.OLDNAME_LIST
    , JBXX0.ESDATE
    , JBXX0.DISTRICT
    , JBXX0.NAME R_NAME
FROM CDB_NOV.F_QY_JBXX JBXX0
WHERE ({where_district_str}) --筛选省码或者市码