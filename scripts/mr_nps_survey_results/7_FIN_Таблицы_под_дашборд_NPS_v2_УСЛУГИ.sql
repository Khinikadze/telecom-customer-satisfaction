
------------------------------------------------------------------------------------------
-- ШАГ 1. ФОРМИРОВАНИЕ СПИСКА АБОНЕНТОВ С УСЛУГАМИ ----------------------------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный блок очищает и загружает таблицу `mr_nps_survey_results_service`, 
-- объединяя данные об активных подписках из BIS_SUBS_PACKS с NPS-таблицей. 
-- Определяется принадлежность к ключевым продуктовым группам: ЕВА, МегаСемья, Улучши, Перенос и др.

BEGIN

  --------------------------------------------------------------------------
  -- ШАГ 1.1. ОЧИСТКА ЦЕЛЕВОЙ ТАБЛИЦЫ --------------------------------------
  --------------------------------------------------------------------------
  DELETE FROM fin_ba.mr_nps_survey_results_service
  WHERE 1 = 1;

  --------------------------------------------------------------------------
  -- ШАГ 1.2. ЗАГРУЗКА СТАНДАРТНЫХ УСЛУГ ИЗ СПРАВОЧНИКА `mr_segment_services`
  --------------------------------------------------------------------------
  INSERT INTO mr_nps_survey_results_service
  SELECT /*+ parallel (8) */
         nps.sk_subs_id,
         nps.snap_date,
         bsh.pack_pack_id,
         bsh.start_date,
         bsh.end_date,
         serv.service_group
  FROM PUB_DS.BIS_SUBS_PACKS bsh
  INNER JOIN fin_ba.mr_segment_services serv
         ON bsh.pack_pack_id = serv.pack_id
  INNER JOIN fin_ba.mr_nps_survey_results_tariff nps
         ON bsh.subs_subs_id = nps.subs_subs_id
  WHERE billing_filial_id = 10
    AND bsh.start_date >= TO_DATE('01.01.2022', 'DD.MM.YYYY');

  COMMIT;

  --------------------------------------------------------------------------
  -- ШАГ 1.3. ДОБАВЛЕНИЕ КАТЕГОРИЗАЦИИ ПО ЕВА (EVA_TYPE) ------------------
  --------------------------------------------------------------------------
  -- КОММЕНТАРИЙ: Определяется принадлежность абонента к EVA_PLUS, EVA_MLK, EVA_VOLTE.
  -- Категория вычисляется на основе NAVI_USER и наличия определенного пакета.
  INSERT INTO mr_nps_survey_results_service
  SELECT DISTINCT
         sk_subs_id,
         snap_date,
         pack_pack_id,
         start_date,
         end_date,
         service_group
  FROM (
    SELECT /*+ parallel (8) */
           ba.snap_date,
           ba.sk_subs_id,
           ba.subs_subs_id,
           p.pack_pack_id,
           p.start_date,
           p.end_date,
           p.navi_user,
           -- Определение категории ЕВА
           CASE 
             WHEN p.pack_pack_id IN (504693) THEN 'EVA_PLUS'
             WHEN NVL(SUM(CASE WHEN p.navi_user LIKE '%NOSMS%' THEN 1 END), 0) > 0 
              AND NVL(SUM(CASE WHEN p.navi_user NOT LIKE '%NOSMS%' THEN 1 END), 0) > 0 THEN 'EVA_MLK'
             WHEN NVL(SUM(CASE WHEN p.navi_user LIKE '%NOSMS%' THEN 1 END), 0) = 0 
              AND NVL(SUM(CASE WHEN p.navi_user NOT LIKE '%NOSMS%' THEN 1 END), 0) > 0 THEN 'EVA_MLK'
             WHEN NVL(SUM(CASE WHEN p.navi_user LIKE '%NOSMS%' THEN 1 END), 0) > 0 
              AND NVL(SUM(CASE WHEN p.navi_user NOT LIKE '%NOSMS%' THEN 1 END), 0) = 0 THEN 'EVA_VOLTE'
             ELSE 'NO_EVA'
           END AS service_group,
           -- Флаг, что абонент относится к ЕВА
           CASE 
             WHEN NVL(SUM(CASE WHEN p.pack_pack_id IN (25529,18250,501511,504693,504912) THEN 1 END), 0) > 0 THEN 1
             ELSE 0
           END AS flg_eva
    FROM PUB_DS.BIS_SUBS_PACKS p
    INNER JOIN fin_ba.mr_nps_survey_results_tariff ba
            ON p.subs_subs_id = ba.subs_subs_id
    WHERE p.billing_filial_id = 10
      AND p.start_date >= TO_DATE('01.01.2022', 'DD.MM.YYYY')
      AND p.pack_pack_id IN (25529,18250,501511,504693,504912)
    GROUP BY ba.snap_date, ba.sk_subs_id, ba.subs_subs_id, p.pack_pack_id, start_date, end_date, p.navi_user
  ) eva_services
  WHERE service_group != 'NO_EVA';

  COMMIT;

END;


/*
------------------------------------------------------------------------------------------
--  СОЗДАНИЕ VIEW ДЛЯ ПРИЗНАКОВ ПО УСЛУГАМ -----------------------------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Представление `mr_nps_survey_results_service_view` трансформирует таблицу
-- `mr_nps_survey_results_service` в набор бинарных признаков по каждому продукту,
-- включая актуальные (по end_date > sysdate) и действующие на момент snap_date.

-- DROP VIEW mr_nps_survey_results_service_view;

CREATE VIEW mr_nps_survey_results_service_view AS
SELECT sk_subs_id,
       snap_date,

       -- АКТИВНЫЕ УСЛУГИ НА ДАТУ SNAP_DATE
       MAX(CASE WHEN service_group = 'PRODUCT_PERENOS'        AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_PERENOS,
       MAX(CASE WHEN service_group = 'PRODUCT_MEGASIL_OTHER'  AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_MEGASIL_OTHER,
       MAX(CASE WHEN service_group = 'PRODUCT_EVA'            AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_EVA,
       MAX(CASE WHEN service_group = 'PRODUCT_FAMILY'         AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_FAMILY,
       MAX(CASE WHEN service_group = 'PR_FAMILY_MANAGER'      AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_FAMILY_MANAGER,
       MAX(CASE WHEN service_group = 'PRODUCT_BEZL_INTERNET'  AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_BEZL_INTERNET,
       MAX(CASE WHEN service_group = 'PRODUCT_PRE5G'          AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_PRE5G,
       MAX(CASE WHEN service_group = 'PRODUCT_MEGASIL_BESPL'  AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_MEGASIL_BESPL,
       MAX(CASE WHEN service_group = 'PRODUCT_ULUCH'          AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_ULUCH,

       -- АКТУАЛЬНЫЕ УСЛУГИ (ДЕЙСТВУЮЩИЕ СЕЙЧАС)
       MAX(CASE WHEN service_group = 'PRODUCT_PERENOS'        AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_PERENOS,
       MAX(CASE WHEN service_group = 'PRODUCT_MEGASIL_OTHER'  AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_MEGASIL_OTHER,
       MAX(CASE WHEN service_group = 'PRODUCT_EVA'            AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_EVA,
       MAX(CASE WHEN service_group = 'PRODUCT_FAMILY'         AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_FAMILY,
       MAX(CASE WHEN service_group = 'PR_FAMILY_MANAGER'      AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_FAMILY_MANAGER,
       MAX(CASE WHEN service_group = 'PRODUCT_BEZL_INTERNET'  AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_BEZL_INTERNET,
       MAX(CASE WHEN service_group = 'PRODUCT_PRE5G'          AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_PRE5G,
       MAX(CASE WHEN service_group = 'PRODUCT_MEGASIL_BESPL'  AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_MEGASIL_BESPL,
       MAX(CASE WHEN service_group = 'PRODUCT_ULUCH'          AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_ULUCH,

       -- ЕВА-КАТЕГОРИЯ (АКТУАЛЬНАЯ)
       MAX(CASE WHEN service_group = 'EVA_PLUS'  AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_EVA_PLUS,
       MAX(CASE WHEN service_group = 'EVA_MLK'   AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_EVA_MLK,
       MAX(CASE WHEN service_group = 'EVA_VOLTE' AND end_date > SYSDATE THEN 1 ELSE 0 END) AS PRODUCT_CD_EVA_VOLTE,

       -- ЕВА-КАТЕГОРИЯ (НА SNAP_DATE)
       MAX(CASE WHEN service_group = 'EVA_PLUS'  AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_EVA_PLUS,
       MAX(CASE WHEN service_group = 'EVA_MLK'   AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_EVA_MLK,
       MAX(CASE WHEN service_group = 'EVA_VOLTE' AND start_date < snap_date AND end_date > snap_date THEN 1 ELSE 0 END) AS PRODUCT_EVA_VOLTE

FROM mr_nps_survey_results_service
GROUP BY sk_subs_id, snap_date;
*/



/*

------------------------------------------------------------------------------------------
-- СОЗДАНИЕ СПРАВОЧНИКА УСЛУГ: fin_ba.mr_segment_services -------------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Справочник отображает соответствие ID пакетов услуг определённым продуктовым группам.
-- Используется при формировании признаков по услугам.


DROP VIEW fin_ba.mr_segment_services;
CREATE VIEW fin_ba.mr_segment_services
AS
SELECT DISTINCT pack_id, 
  CASE WHEN PACK_ID IN (501887,501886,501885,501889,502755,502757,
                        502761,502752,503637,503639,503641,503643,503153,504500,
                        504502,504504,504508,501883,501882,501877,501884,502753,
                        502756,502760,502751,503636,503638,503640,503642,503148,
                        504499,504501,504503,504507,505559) THEN 'PRODUCT_FAMILY'
    WHEN PACK_ID in (20417, 20418, 20419, 20256) THEN 'PR_FAMILY_MANAGER'    
    WHEN PACK_ID in (25527,25529,18250,501511,504693,504912) THEN 'PRODUCT_EVA'
    WHEN pack_id in (24187,501585,501675,503174,504205) THEN 'PRODUCT_PRE5G'
    WHEN PACK_ID in (501755,24146,503182,501802,503128,505562) THEN 'PRODUCT_PERENOS'
    WHEN PACK_ID IN (501578,501579,501580,501581,501582,502586,502587,503196,505168,505169,505162,505163,505164,505165,505166,505572,505549) THEN 'PRODUCT_ULUCH'
    WHEN PACK_ID in (502420,502277,23973,23976,23974,23977,24298,24147,24870,23982,23978,24145,23983,
        25747,502280,502500,502501,502498,502499,502565,23970,23971,23975,23979,23981,23982,
        23983,23984,23986,24145,24147,24298,24847,24848) THEN 'PRODUCT_MEGASIL_OTHER'
    WHEN PACK_ID in (24225,24229,24864,24869,24846,24146,25748,502564,24226,24224,
                    24227,24228,504315,504690,504355,505560,505561,505893,505894,
                    504375,504316,504350,504353,503184,503185,503186,503191,503183,
                    503189,503192,503190,503188,503187,503195,503193,503194,503838,
                    503837,504152,504153,505199,505200,505197,505352,505350
                    ) THEN 'PRODUCT_MEGASIL_BESPL'
    WHEN PACK_ID in (20557,20738,25425,503194,23940,502294,21369,21377,21373,502429,21066,21067,15542,10341,15140,10107,
              18253,10340,15141,12109,12089,20227,15467,17401,17402,17403,7889,7888,7887,18804,19862,15142,1015,
        1464,1170,20588,20587,17967,1168,17419,17770,17385,17383,9995,9996,14432,17026,17027,19777,19811,12229,
        3360,9992,9991,9990,14792,14791,9988,9989,3362,4107,17071,7493,7492,9647,7491,7494,9646,4109,4108,9645,
        4106,22466,22467,22465,23981,1259,9527,18160,15996,17386,17388,21381,15398,23933,22562,21308,
              11142,11132,22248,22249,22247,22244,11159,22246,22245,22243,11134,11145,22232,22231,11137,11146,
        11149,11139,11163,11162,22218,22217,22216,22212,22211,22210,11160,11161,22208,11129,11156,11154,11157,
        11153,11131,11144,22312,22311,22310,22309,22303,22302,22301,22297,11136,11141,11133,11152,11148,11150,
        11155,11130,11143,11135,11151,11147,22283,22189,11138,22268,22267,22266,22265,11140,11158) THEN 'PRODUCT_BEZL_INTERNET' END AS SERVICE_GROUP
FROM PUB_DS.BIS_PACKS
WHERE billing_filial_id=10
      AND 
     (PACK_ID IN (
    --СЕМЬЯ
    501887,501886,501885,501889,502755,502757,
    502761,502752,503637,503639,503641,503643,503153,504500,
    504502,504504,504508,501883,501882,501877,501884,502753,
    502756,502760,502751,503636,503638,503640,503642,503148,
    504499,504501,504503,504507, 20417, 20418, 20419, 20256,
    --ЕВА
    25529,18250,501511,504693,504912,
    --PRE5G
    24187,501585,501675,503174,
    --ПЕРЕНОС 
    501755,24146,503182,501802,503128,
    --УЛУЧШАЙЗЕР
    501578,501579,501580,501581,501582,502586,
    502587,503196,505168,505169,
    --ПРОЧИЕ МЕГАСИЛЫ
    502420,502277,23973,23976,23974,23977,24298,24147,24870,23982,23978,24145,23983,25747,502280,502500,502501,
    502498,502499,502565,23970,23971,23975,23979,23981,23982,23983,23984,23986,24145,24147,24298,24847,24848,
    --ПРОЧИЕ МЕГАСИЛЫ БЕСПЛАТНО
    24224,24226,24227,24228,25748,502564,24225,24229,24864,24869,24846,
    --БЕЗЛИМИТНЫЙ ИНТЕРНЕТ
    20557,20738,25425,503194,23940,502294,21369,21377,21373,502429,21066,21067,15542,10341,15140,10107
      ,18253,10340,15141,12109,12089,20227,15467,17401,17402,17403,7889,7888,7887,18804,19862,15142,1015,1464,1170,20588,20587,17967,1168
      ,17419,17770,17385,17383,9995,9996,14432,17026,17027,19777,19811,12229,3360,9992,9991,9990,14792,14791,9988,9989,3362,4107,17071,7493
      ,7492,9647,7491,7494,9646,4109,4108,9645,4106,22466,22467,22465,23981,1259,9527,18160,15996,17386,17388,21381,15398,23933,22562,21308
      ,11142,11132,22248,22249,22247,22244,11159,22246,22245,22243,11134,11145,22232,22231,11137,11146,11149,11139,11163,11162,22218,22217
      ,22216,22212,22211,22210,11160,11161,22208,11129,11156,11154,11157,11153,11131,11144,22312,22311,22310,22309,22303,22302,22301,22297
      ,11136,11141,11133,11152,11148,11150,11155,11130,11143,11135,11151,11147,22283,22189,11138,22268,22267,22266,22265,11140,11158))
*/