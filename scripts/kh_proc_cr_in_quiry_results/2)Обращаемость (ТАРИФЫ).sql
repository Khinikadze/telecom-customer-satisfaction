----------------------------------------------------------------------
--ШАГ 1. ФОРМИРОВАНИЕ СПИСКА АБОНЕНТОВ ПО МОДЕЛИ ОТТОКА---------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Проверка готовности таблиц 
-- Этот закомментированный блок предназначен для предварительной проверки 
-- количества записей в таблице оттока по филиалам на определенную дату
/*SELECT filial_id, COUNT(*) 
FROM PUB_DS.F_SUBS_CHURN_TRG_WEEKLY
WHERE start_date='01.03.2025'
GROUP BY filial_id*/

-- КОММЕНТАРИЙ: Создание представления для работы с тарифами
-- Позволяет унифицировать и консолидировать информацию о тарифных планах
-- из различных источников с учетом актуальных периодов действия
/*
CREATE VIEW fin_ba.mr_tariff
AS
SELECT DISTINCT
  dtp.TRPL_TRPL_ID,
  brp.NAME_R,
  brp.RTPL_ID,
  dtp.name_line_tp, 
  group_tariff
FROM pub_ds.BIS_RATE_PLANS brp,
     (SELECT m.rtpl_rtpl_id, tar.NAME_MB_AAB3M AS group_tariff
      FROM products.M_TARIFF_PLANS m
      LEFT JOIN products.D_TARIFF_PLANS d 
        ON nvl(m.TRPL_TRPL_ID,-1) = d.TRPL_TRPL_ID 
       AND nvl(m.FILIAL_ID,-1) = NVL(d.FILIAL_ID,-1)
       AND nvl(m.BILLING_FILIAL_ID,-1) = d.BILLING_FILIAL_ID
      LEFT JOIN products.D_RT_DETAIL_GROUPS tar 
        ON d.rtdg_rtdg_id = tar.rtdg_id
      WHERE m.billing_filial_id = 10) gt,
     products.D_TARIFF_PLANS dtp,
     products.M_TARIFF_PLANS mtp,
     gfm_stg.RATE_PLAN_TYPES rpt,
     PRODUCTS.D_RT_CONTRIBUTION_GROUPS rcg
WHERE brp.START_DATE <= SYSDATE 
  AND brp.END_DATE > SYSDATE 
  AND mtp.START_DATE <= SYSDATE 
  AND mtp.END_DATE > SYSDATE 
  AND brp.RTPL_ID = mtp.rtpl_rtpl_id
  AND brp.RTPL_ID = gt.rtpl_rtpl_id
  AND brp.BILLING_FILIAL_ID = mtp.billing_filial_id
  AND NVL(mtp.TRPL_TRPL_ID,-1) = dtp.TRPL_TRPL_ID(+)
  AND NVL(mtp.FILIAL_ID,-1) = NVL(dtp.FILIAL_ID(+),-1)  
  AND NVL(mtp.BILLING_FILIAL_ID,-1) = dtp.BILLING_FILIAL_ID(+)
  AND rpt.rptp_id = brp.RPTP_RPTP_ID      
  AND rcg.RTCG_ID(+) = dtp.RTCG_RTCG_ID
*/
-- ОСНОВНОЙ БЛОК: Формирование списка абонентов
-- Назначение: Агрегация данных для анализа оттока с использованием 
-- множества источников и применением сложной логики объединения
DECLARE 
  start_dt DATE;    -- Начальная дата периода анализа
  end_dt DATE;      -- Конечная дата периода анализа
  delta NUMERIC;    -- Смещение для корректной работы с месячными данными
BEGIN
  -- Установка параметров для анализа
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');  -- Начало периода
  end_dt   := TO_DATE('01.03.2025', 'DD.MM.YYYY');  -- Конец периода
  delta    := -1;                                   -- Смещение для предыдущего месяца

  -- ПОДГОТОВИТЕЛЬНЫЙ ЭТАП: Очистка данных
  -- Удаление существующих записей за указанный период 
  -- для предотвращения дублирования
  DELETE FROM fin_ba.kh_proc_cr_inquiry_results_tariff 
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  -- ОСНОВНОЙ ЭТАП: Вставка агрегированных данных
  -- Объединение информации из множества источников с целью 
  -- комплексного анализа абонентского поведения
  INSERT INTO fin_ba.kh_proc_cr_inquiry_results_tariff (
         COMP_OS_ID, BILLING_FILIAL_ID, ITPC_ITPC_ID, SK_SUBS_ID,
         snap_date, snap_week, snap_month, snap_quarter, SK_BMT_ID,
         SUBS_OS_OS_ID, CR_OS_OS_ID, inqr_id, LAT, LON,
         DESCRIPTION, CRCAT_CRCAT_ID, CR_WEIGHT, CODE_LEVEL_1,
         CODE_LEVEL_2, CODE_LEVEL_3, CODE_LEVEL_4, FULL_NAME, FLAG_LOCAL_GENERAL,
         filial_id, subcat_pyramid_name, cat_pyramid_name, SUBS_SUBS_ID,
         days_in_fb_s12, mega_sms_cnt, all_mega_sms_cnt, cnt_inq, cnt_lk, roam_exp,
         days_since_chng, mou_lvl, dou_lvl, mou_m1, dou_m1, revenue_wo_itc_vas_roam_ma3,
         is_dou_unlim_curr, is_mou_unlim_curr, mega_circle_prc, inquiry_action_max_time,
         device_model, device_price, device_type_end, lifetime, segment_name,
         name_r, name_line_tp, group_tariff, network_4g_share,
         PROSTOI_DUR, INC_NUMBER,
         share_volte, share_vowifi,
         rep1_2022, rep2_2022, rep1_2023, repop_2023, repsimp_2023, repbandl_2023,
         overload, sqi_data, sqi_voice, speed_lte, bns, share_vpn, device_os,
         duration_2g_percent, duration_3g_percent, duration_4g_percent,
         spd_lower_than_500_kbps_rate, spd_lower_than_2000_kbps_rate, spd_lower_than_10000_kbps_rate,
         network_serv_model, product_risk_base, product_debitor, profile_bearer_4g
  )
  SELECT DISTINCT 
         -- Основные поля из таблицы оценки
         cr.COMP_OS_ID, cr.BILLING_FILIAL_ID, cr.ITPC_ITPC_ID, cr.SK_SUBS_ID,
         cr.snap_date, cr.snap_week, cr.snap_month, cr.snap_quarter, cr.SK_BMT_ID,
         cr.SUBS_OS_OS_ID, cr.CR_OS_OS_ID, cr.inqr_id, cr.LAT, cr.LON,
         cr.DESCRIPTION, cr.CRCAT_CRCAT_ID, cr.CR_WEIGHT, cr.CODE_LEVEL_1,
         cr.CODE_LEVEL_2, cr.CODE_LEVEL_3, cr.CODE_LEVEL_4, cr.FULL_NAME, cr.FLAG_LOCAL_GENERAL,
         cr.filial_id, cr.subcat_pyramid_name, cr.cat_pyramid_name, cr.SUBS_SUBS_ID,

         -- Отток и характеристики устройства
         churn.days_in_fb_s12, churn.mega_sms_cnt, churn.all_mega_sms_cnt, churn.cnt_inq,
         churn.cnt_lk, churn.roam_exp, churn.days_since_chng,
         churn.mou_lvl, churn.dou_lvl, churn.mou_m1, churn.dou_m1,
         churn.revenue_wo_itc_vas_roam_ma3, churn.is_dou_unlim_curr, churn.is_mou_unlim_curr,
         churn.mega_circle_prc, churn.inquiry_action_max_time,
         churn.device_model, churn.device_price, churn.device_type_end, churn.lifetime,

         -- Сегментация
         segm.segment_name,

         -- Тарифная информация
         tariff_report.name_r, tariff_report.name_line_tp, tariff_report.group_tariff,

         -- Потребление LTE
         kpi_m.NETWORK_4G_SHARE,

         -- Простои
         prost.PROSTOI_DUR, prost.INC_NUMBER,

         -- VoLTE, VoWiFi, устройство, доп. метрики
         kub.share_volte, kub.share_vowifi,
         kub.rep1_2022, kub.rep2_2022, kub.rep1_2023, kub.repop_2023, kub.repsimp_2023, kub.repbandl_2023,

         -- Перегрузки и скорость
         kub2.DAYS_WITH_OVRL AS overload,
         kub2.sqi_data, kub2.sqi_voice,
         kub2.data_dl_avg_speed AS speed_lte,

         -- Прочее
         kub.bns, kub.share_vpn, kub.os AS device_os,

         -- Сетевые уровни и качество
         smart.duration_2g_percent, smart.duration_3g_percent, smart.duration_4g_percent,
         smart.spd_lower_than_500_kbps_rate,
         smart.spd_lower_than_2000_kbps_rate,
         smart.spd_lower_than_10000_kbps_rate,

         -- Приоритет
         sm.network_serv_model,

         -- Долговая активность
         NULL AS product_risk_base,
         debt.debt_type,

         -- Профиль 4G
         kub.bearer_4g AS profile_bearer_4g

  FROM 
    -- Основная таблица с первичными оценками
    (SELECT /*+ parallel (8)*/ * 
     FROM fin_ba.KH_CR_INQUIRY_DAILY 
     WHERE snap_date >= start_dt AND snap_date < end_dt) cr

    -- 1. Факторы оттока
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/ 
             start_date, filial_id, sk_subs_id, subs_subs_id,
             trpl_trpl_id, rtpl_rtpl_id, days_in_fb_s12,
             mega_sms_cnt, all_mega_sms_cnt, cnt_inq, cnt_lk, roam_exp,
             days_since_chng, mou_lvl, dou_lvl,
             mou_m1, dou_m1, revenue_wo_itc_vas_roam_ma3,
             is_dou_unlim_curr, dou_ratio_curr_week, is_mou_unlim_curr, mou_ratio_curr_week,
             mega_circle_prc, inquiry_action_max_time, device_model,
             device_price, device_type_end, lifetime
      FROM PUB_DS.F_SUBS_CHURN_TRG_WEEKLY
      WHERE billing_filial_id = 10  
        AND start_date >= trunc(start_dt, 'iw')
        AND start_date < trunc(end_dt + 7, 'iw')
    ) churn
      ON cr.sk_subs_id = churn.sk_subs_id
     AND cr.snap_week = churn.start_date

    -- 2. Сегментация с названиями сегментов
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/ 
             a.start_date, a.sk_subs_id, a.lifetime, a.model_device, a.segm_id_end, b.segment_name
      FROM PUB_DS.A_SUBS_DIGIT_SEGMENTS a
      LEFT OUTER JOIN (
        SELECT segm_id, MAX(segment_name) AS segment_name
        FROM PUB_DS.D_DWH_SEGMENTS 
        WHERE end_date > SYSDATE
        GROUP BY segm_id
      ) b ON a.segm_id_end = b.segm_id
      WHERE a.billing_filial_id = 10 
        AND a.start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
        AND a.start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    ) segm
      ON cr.sk_subs_id = segm.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = segm.start_date

    -- 3. Тарифное представление
    LEFT OUTER JOIN fin_ba.mr_tariff tariff_report
      ON churn.rtpl_rtpl_id = tariff_report.rtpl_id

    -- 4. Потребление 4G
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/ 
             start_date, sk_subs_id, dou, dou_4g,
             CASE 
               WHEN NVL(dou,0)<100 THEN 'a. <100 MB' 
               WHEN NVL(dou_4g,0)/NVL(dou,0)<0.8 THEN 'b. <80%'
               WHEN NVL(dou_4g,0)/NVL(dou,0)<0.9 THEN 'c. 80-90%'
               ELSE 'd. >90%' 
             END AS NETWORK_4G_SHARE
      FROM pub_ds.a_subs_kpis_monthly
      WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
        AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    ) kpi_m
      ON cr.sk_subs_id = kpi_m.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = kpi_m.start_date

    -- 5. Простои за последние 28 дней
    LEFT OUTER JOIN (
      SELECT cr.sk_subs_id, cr.snap_date,
             SUM(downtime_consume_hour) AS PROSTOI_DUR,
             COUNT(DISTINCT INC_INC_ID) AS INC_NUMBER
      FROM fin_ba.KH_CR_INQUIRY_DAILY cr
      INNER JOIN pub_ds.f_inc_subs_weekly postr
         ON cr.sk_subs_id = postr.sk_subs_id
        AND cr.snap_week >= postr.start_date
        AND cr.snap_week - postr.start_date <= 28
      GROUP BY cr.sk_subs_id, cr.snap_date
    ) prost
      ON cr.sk_subs_id = prost.sk_subs_id
     AND cr.snap_date = prost.snap_date

    -- 6. Долговая активность по коммуникационным кампаниям
    LEFT OUTER JOIN (
      SELECT cr.snap_month, cr.sk_subs_id, MAX(debt.debt_type) AS debt_type
      FROM fin_ba.KH_CR_INQUIRY_DAILY cr
      LEFT JOIN PUB_STG.D_CIM_COMPAIGN_CUSTOMERS cc
        ON cc.sk_subs_id = cr.sk_subs_id
      INNER JOIN (
        SELECT communication_id,
               MAX(
                 CASE 
                   WHEN comp_name = 'превентивное_информирование' THEN '1. Превент'
                   WHEN is_tw = 1 THEN '2. Другой номер'
                   ELSE '3. Номер с ДЗ'
                 END
               ) AS debt_type
        FROM MAXIM_MAKARENKOV.CIM_COMPAIGN
        GROUP BY communication_id
      ) debt
        ON cc.communication_id = debt.communication_id
      WHERE cr.snap_date - TRUNC(cc.RUN_DTTM,'DD') BETWEEN 0 AND 90
        AND cc.RUN_DTTM BETWEEN DATE '2023-04-01' AND DATE '2024-12-01'
        AND cc.disposition_type = 1
        AND cc.response_type > 0
      GROUP BY cr.snap_month, cr.sk_subs_id
    ) debt
      ON cr.sk_subs_id = debt.sk_subs_id
     AND cr.snap_month = debt.snap_month

    -- 7. Качество сети и поведение (куб MCAAS)
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/
             start_date, sk_subs_id, os, 
             share_volte, share_vowifi, 
             NULL AS rep1_2022, NULL AS rep2_2022,
             NULL AS rep1_2023, NULL AS repop_2023,
             NULL AS repsimp_2023, NULL AS repbandl_2023, 
             bns, share_vpn, bearer_4g
      FROM mf_mcaas_tools.a_subs1m_b2c_factor_m
      WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
        AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    ) kub
      ON cr.sk_subs_id = kub.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = kub.start_date

    -- 8. Перегрузка, SQI, средняя скорость
    LEFT OUTER JOIN (
      SELECT start_date, sk_subs_id, 
             DAYS_WITH_OVRL, sqi_voice, sqi_data, data_dl_avg_speed
      FROM MF_MCAAS_QR.A_SUBS_CALLS_PM_QUALITY_DATA_M
      WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
        AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    ) kub2
      ON cr.sk_subs_id = kub2.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = kub2.start_date

    -- 9. Приоритет абонента (сервисная модель)
    LEFT OUTER JOIN (
      SELECT sk_subs_id, snap_date, priority_priority_id AS network_serv_model
      FROM pub_ds.s_subs_priority_daily
      WHERE billing_filial_id = 10
        AND snap_date >= start_dt
        AND snap_date <= end_dt
    ) sm
      ON cr.sk_subs_id = sm.sk_subs_id
     AND cr.snap_date = sm.snap_date

    -- 10. Нахождение в слоях сети и метрики скорости
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/
             report_month, sk_subs_id, 
             duration_2g_percent, duration_3g_percent, duration_4g_percent,
             spd_lower_than_500_kbps_rate,
             spd_lower_than_2000_kbps_rate,
             spd_lower_than_10000_kbps_rate
      FROM REP_DS.A_NRI_SUBS_STATS_MONTHLY 
      WHERE report_month >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
        AND report_month < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    ) smart
      ON cr.sk_subs_id = smart.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = smart.report_month;

  -- ОКОНЧАТЕЛЬНАЯ ОПЕРАЦИЯ
  COMMIT;

END;