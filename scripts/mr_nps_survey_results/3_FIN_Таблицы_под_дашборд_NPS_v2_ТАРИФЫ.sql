----------------------------------------------------------------------
-- ШАГ 1. ФОРМИРОВАНИЕ ВИТРИНЫ ПО ОПРОСАМ И ФАКТОРАМ ОТТОКА -----------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Скрипт формирует витрину fin_ba.mr_nps_survey_results_tariff,
-- объединяя результаты опросов с широким набором данных: от факторов оттока
-- до потребления, устройств, тарифов, сегментов, технических метрик.

DECLARE 
  start_dt DATE;    
  end_dt   DATE;    
  delta    NUMERIC; 
BEGIN
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.02.2025', 'DD.MM.YYYY');
  delta    := -1;

  DELETE FROM fin_ba.mr_nps_survey_results_tariff 
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  INSERT INTO fin_ba.mr_nps_survey_results_tariff (
    measure_id, filial_id, sk_subs_id, subs_subs_id, msisdn,
    snap_date, snap_week, snap_month, snap_quarter,
    mnco_group_id, os_os_id, ans, value_1, value_2, value_3, value_4,
    survey_arpu_group, survey_city_size, survey_gender, survey_age,
    survey_time_group, survey_day, survey_weekday, survey_weekday_type,
    days_in_fb_s12, mega_sms_cnt, all_mega_sms_cnt, cnt_inq, cnt_lk, roam_exp,
    days_since_chng, mou_lvl, dou_lvl, mou_m1, dou_m1, revenue_wo_itc_vas_roam_ma3,
    is_dou_unlim_curr, is_mou_unlim_curr, mega_circle_prc, inquiry_action_max_time,
    device_model, device_price, device_type_end, lifetime, segment_name, 
    name_r, name_line_tp, group_tariff, NETWORK_4G_SHARE,
    PROSTOI_DUR, INC_NUMBER,
    share_volte, share_vowifi, 
    rep1_2022, rep2_2022, rep1_2023, repop_2023, repsimp_2023, repbandl_2023, 
    overload, sqi_data, sqi_voice, speed_lte, bns, share_vpn, device_os,
    duration_2g_percent, duration_3g_percent, duration_4g_percent,
    spd_lower_than_500_kbps_rate, spd_lower_than_2000_kbps_rate, spd_lower_than_10000_kbps_rate,
    network_serv_model, PRODUCT_RISK_BASE, PRODUCT_DEBITOR, SURVEY_OPERATOR_ID,
    PROFILE_bearer_4g
  )
  SELECT DISTINCT
    nps.measure_id, filial_id, nps.sk_subs_id, subs_subs_id, segm.msisdn,
    nps.snap_date, nps.snap_week, nps.snap_month, nps.snap_quarter,
    mnco_group_id, os_os_id, ans, value_1, value_2, value_3, value_4,
    survey_arpu_group, survey_city_size, survey_gender, survey_age,
    survey_time_group, survey_day, survey_weekday, survey_weekday_type,
    days_in_fb_s12, mega_sms_cnt, all_mega_sms_cnt, cnt_inq, cnt_lk, roam_exp,
    days_since_chng, mou_lvl, dou_lvl, mou_m1, dou_m1, revenue_wo_itc_vas_roam_ma3,
    is_dou_unlim_curr, is_mou_unlim_curr, mega_circle_prc, inquiry_action_max_time,
    device_model, device_price, device_type_end, churn.lifetime,
    segment_name, name_r, name_line_tp, group_tariff, NETWORK_4G_SHARE,
    PROSTOI_DUR, INC_NUMBER,
    share_volte, share_vowifi, 
    rep1_2022, rep2_2022, rep1_2023, repop_2023, repsimp_2023, repbandl_2023, 
    DAYS_WITH_OVRL AS overload, sqi_data, sqi_voice, data_dl_avg_speed AS speed_lte,
    bns, share_vpn, os AS device_os,
    duration_2g_percent, duration_3g_percent, duration_4g_percent,
    spd_lower_than_500_kbps_rate, spd_lower_than_2000_kbps_rate, spd_lower_than_10000_kbps_rate,
    network_serv_model, NULL AS PRODUCT_RISK_BASE, debt_type, SURVEY_OPERATOR_ID,
    bearer_4g AS PROFILE_bearer_4g

  FROM 
  (
    SELECT /*+ parallel(8)*/ *
    FROM mr_nps_survey_results_input
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) nps

  -- Факторы оттока
  LEFT OUTER JOIN (
    SELECT /*+ parallel(8)*/
           start_date, filial_id, sk_subs_id, subs_subs_id,
           trpl_trpl_id, rtpl_rtpl_id, days_in_fb_s12,
           mega_sms_cnt, all_mega_sms_cnt,
           cnt_inq, cnt_lk, roam_exp,
           days_since_chng,
           mou_lvl, dou_lvl, mou_m1, dou_m1,
           revenue_wo_itc_vas_roam_ma3,
           is_dou_unlim_curr, dou_ratio_curr_week,
           is_mou_unlim_curr, mou_ratio_curr_week,
           mega_circle_prc, inquiry_action_max_time,
           device_model, device_price, device_type_end, lifetime
    FROM PUB_DS.F_SUBS_CHURN_TRG_WEEKLY
    WHERE billing_filial_id = 10
      AND start_date >= TRUNC(start_dt, 'IW')
      AND start_date < TRUNC(end_dt + 7, 'IW')
  ) churn
  ON nps.sk_subs_id = churn.sk_subs_id
  AND nps.snap_week = churn.start_date

  -- Сегмент и название тарифа
  LEFT OUTER JOIN (
    SELECT /*+ parallel(8)*/
           a.start_date, a.sk_subs_id, a.msisdn, a.lifetime, 
           a.model_device, a.segm_id_end, b.segment_name
    FROM PUB_DS.A_SUBS_DIGIT_SEGMENTS a
    LEFT JOIN (
      SELECT segm_id, MAX(segment_name) AS segment_name
      FROM PUB_DS.D_DWH_SEGMENTS
      WHERE end_date > SYSDATE
      GROUP BY segm_id
    ) b ON a.segm_id_end = b.segm_id
    WHERE billing_filial_id = 10
      AND a.start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
      AND a.start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  ) segm
  ON nps.sk_subs_id = segm.sk_subs_id
  AND ADD_MONTHS(nps.snap_month, delta) = segm.start_date

  LEFT OUTER JOIN fin_ba.mr_tariff tariff_report
  ON churn.rtpl_rtpl_id = tariff_report.rtpl_id

  -- Потребление LTE
  LEFT OUTER JOIN (
    SELECT /*+ parallel(8)*/
           start_date, sk_subs_id, dou, dou_4g,
           CASE WHEN NVL(dou, 0) < 100 THEN 'a. <100 MB'
                WHEN NVL(dou_4g, 0)/NVL(dou, 0) < 0.8 THEN 'b. <80%'
                WHEN NVL(dou_4g, 0)/NVL(dou, 0) < 0.9 THEN 'c. 80-90%'
                ELSE 'd. >90%' END AS NETWORK_4G_SHARE
    FROM pub_ds.a_subs_kpis_monthly
    WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
      AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  ) kpi_m
  ON nps.sk_subs_id = kpi_m.sk_subs_id
  AND ADD_MONTHS(nps.snap_month, delta) = kpi_m.start_date

  -- Простои
  LEFT OUTER JOIN (
    SELECT nps.sk_subs_id, nps.snap_date,
           SUM(downtime_consume_hour) AS PROSTOI_DUR,
           COUNT(DISTINCT INC_INC_ID) AS INC_NUMBER
    FROM mr_nps_survey_results_input nps
    INNER JOIN pub_ds.f_inc_subs_weekly postr
      ON nps.sk_subs_id = postr.sk_subs_id
     AND nps.snap_week >= postr.start_date
     AND nps.snap_week - postr.start_date <= 28
    GROUP BY nps.sk_subs_id, nps.snap_date
  ) prost
  ON nps.sk_subs_id = prost.sk_subs_id
  AND nps.snap_date = prost.snap_date

  -- Дебиторская нагрузка (DEBT)
  LEFT OUTER JOIN (
    SELECT nps.snap_month, nps.sk_subs_id, MAX(debt_type) AS debt_type
    FROM fin_ba.mr_nps_survey_results_input nps
    LEFT JOIN pub_stg.d_cim_compaign_customers cc
      ON cc.sk_subs_id = nps.sk_subs_id
    INNER JOIN (
      SELECT communication_id,
             MAX(CASE 
                  WHEN comp_name = 'превентивное_информирование' THEN '1. Превент'
                  WHEN is_tw = 1 THEN '2. Другой номер'
                  ELSE '3. Номер с ДЗ' END) AS debt_type
      FROM maxim_makarenkov.cim_compaign
      GROUP BY communication_id
    ) debt ON cc.communication_id = debt.communication_id
    WHERE nps.snap_date - TRUNC(cc.run_dttm, 'dd') BETWEEN 0 AND 90
      AND cc.run_dttm BETWEEN DATE '2023-04-01' AND DATE '2024-12-01'
      AND cc.disposition_type = 1
      AND cc.response_type > 0
    GROUP BY nps.snap_month, nps.sk_subs_id
  ) debt
  ON nps.sk_subs_id = debt.sk_subs_id
  AND nps.snap_month = debt.snap_month

  -- Факторы 1М (MCAAS)
  LEFT OUTER JOIN (
    SELECT /*+ parallel(8)*/
           start_date, sk_subs_id, os,
           share_volte, share_vowifi,
           NULL AS rep1_2022, NULL AS rep2_2022, NULL AS rep1_2023, 
           NULL AS repop_2023, NULL AS repsimp_2023, NULL AS repbandl_2023,
           bns, share_vpn, bearer_4g
    FROM mf_mcaas_tools.a_subs1m_b2c_factor_m
    WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
      AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  ) kub
  ON nps.sk_subs_id = kub.sk_subs_id
  AND ADD_MONTHS(nps.snap_month, delta) = kub.start_date

  -- KPI (технические)
  LEFT OUTER JOIN (
    SELECT start_date, sk_subs_id,
           days_with_ovrl_cnt AS DAYS_WITH_OVRL,
           sqi_voice, sqi_data, data_dl_avg_speed
    FROM pub_ds.a_tech_kpis_qty_monthly
    WHERE start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
      AND start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  ) kub2
  ON nps.sk_subs_id = kub2.sk_subs_id
  AND ADD_MONTHS(nps.snap_month, delta) = kub2.start_date

  -- Сервисная модель
  LEFT OUTER JOIN (
    SELECT /*+ parallel(10)*/
           sk_subs_id, snap_date, priority_priority_id AS network_serv_model
    FROM pub_ds.s_subs_priority_daily
    WHERE billing_filial_id = 10
      AND snap_date BETWEEN start_dt AND end_dt
  ) sm
  ON nps.sk_subs_id = sm.sk_subs_id
  AND nps.snap_date = sm.snap_date

  -- Распределение по технологиям (2G/3G/4G)
  LEFT OUTER JOIN (
    SELECT /*+ parallel(8)*/
           report_month, sk_subs_id,
           duration_2g_percent, duration_3g_percent, duration_4g_percent,
           spd_lower_than_500_kbps_rate,
           spd_lower_than_2000_kbps_rate,
           spd_lower_than_10000_kbps_rate
    FROM rep_ds.a_nri_subs_stats_monthly
    WHERE report_month >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
      AND report_month < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  ) smart
  ON nps.sk_subs_id = smart.sk_subs_id
  AND ADD_MONTHS(nps.snap_month, delta) = smart.report_month;

  COMMIT;
END;