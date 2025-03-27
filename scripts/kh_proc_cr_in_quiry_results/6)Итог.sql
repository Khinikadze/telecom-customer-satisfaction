---------------------------------------------------------------------
--ШАГ 1. ЗАПОЛНЕНИЕ ВРЕМЕННОЙ ТАБЛИЦЫ ОБОГАЩЁННЫМИ ДАННЫМИ------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Вставка данных в kh_proc_cr_inquiry_results_temp.
-- Данные собираются по абоненту (sk_subs_id) с расчётом признаков:
-- - Продуктовых: активность, выручка, опции, переплаты;
-- - Поведенческих: OTT, устройство, трафик, голос/данные;
-- - Сетевых: SQI, скорость, VoLTE, простои, помехи;
-- - Маркетинговых: каналы обращения, реактивации, репрайсы;
-- - Географических: региональные признаки, филиал, сегменты;
-- - Персональных: мультисим, персонализированные кампании.

-- ИСТОЧНИКИ:
--  fin_ba.kh_proc_cr_inquiry_results_tariff         -- основа (ообращаемость + тариф)
--  fin_ba.kh_proc_cr_inquiry_results_service_view    -- услуги
--  fin_ba.kh_proc_cr_inquiry_results_vas             -- vas-опции
--  fin_ba.kh_proc_cr_inquiry_results_ott_view        -- поведение в приложениях
--  mr_nps_region                                     -- региональные признаки
--  PUB_DS.F_SUBS_CHURN_TRG_WEEKLY                    -- текущий тариф
--  PUB_DS.s_subs_priority_daily                      -- приоритет сети
--  PUB_STG.D_CIM_COMPAIGN_CUSTOMERS                  -- дебиторка
--  pub_ds.bis_subs_packs                             -- реактивации
--  AME_RISK_COMPETITOR_SUBS_v2                       -- рисковая база
--  репрайс 2022/2023/2024                            -- признаки репрайса
--  BD_SCIENTIST_USER.OLD_BASE_MULTISIM_PREPREDICT_V3 -- мультисим
--  pub_ds.F_INQUIRY + BIS_CMS_SITE_DEF               -- канал обращения М
--  дополнительный расчёт канала обращения (по логике Трух)
--  fsm_stg.mgfrootcausea1 / mgfrootcausem1           -- теплокарты
--  pub_ds.a_subs_app_data                            -- youtube, telegram (cd_* признаки)

-- В таблицу пишется готовая сегментированная информация в нормализованном формате.

-- СТРУКТУРА ВСТАВКИ:
-- INSERT INTO kh_proc_cr_inquiry_results_temp (
--    [ОСНОВНЫЕ ДАННЫЕ]
--    COMP_OS_ID, BILLING_FILIAL_ID, ITPC_ITPC_ID, SK_SUBS_ID, SNAP_DATE, ...
--    [ПРОДУКТОВЫЕ ПРИЗНАКИ]
--    PRODUCT_FINBLOCK, PRODUCT_MEGA_SMS, ..., PRODUCT_REVENUE_WO_IITC_VAS_ROAM, ...
--    [ПОВЕДЕНЧЕСКИЕ, УСТРОЙСТВО]
--    PROFILE_MOU, PROFILE_DOU, PROFILE_DEVICE_TYPE, ...
--    [СЕТЕВЫЕ ХАРАКТЕРИСТИКИ]
--    NETWORK_PROSTOI, NETWORK_VOLTE, NETWORK_OVERLOAD, NETWORK_SQI_DATA, ...
--    [OTT-ПРИЗНАКИ]
--    PROFILE_KINOPOISK, PROFILE_TELEGRAM, PROFILE_YOUTUBE, PROFILE_VPN, ...
--    [ГЕОГРАФИЯ / СЕГМЕНТАЦИЯ]
--    REGION_COMPETITOR, REGION_ABC_TOTAL, REGION_SVO, ...
--    [КАНАЛЫ ОБРАЩЕНИЙ]
--    CHANNEL_NAME, TRUX_CHANNEL_NAME
--    [СПЕЦ.ПРИЗНАКИ]
--    FLAG_TEPLOKARTI, RC_ID, MARK_TEPLOKARTI, ...
-- )


--ШАГ 0. ФОРМИРОВАНИЕ ТАБЛИЦЫ ------------
----------------------------------------------------------------------
----------------------------------------------------------------------  

  /*
  DROP TABLE fin_ba.kh_proc_cr_inquiry_results_temp;
  CREATE TABLE fin_ba.kh_proc_cr_inquiry_results_temp
  (
        COMP_OS_ID NUMERIC , BILLING_FILIAL_ID NUMERIC, ITPC_ITPC_ID NUMERIC, SK_SUBS_ID NUMBER(18),
        snap_date DATE, snap_week DATE,snap_month DATE, snap_quarter DATE, SK_BMT_ID NUMBER(10),
        SUBS_OS_OS_ID NUMBER(4), CR_OS_OS_ID NUMBER(4), inqr_id number(18), LAT NUMBER(13,10), LON NUMBER(13,10),
        DESCRIPTION VARCHAR2(4000), CRCAT_CRCAT_ID NUMBER(2), CR_WEIGHT NUMBER(4,2), CODE_LEVEL_1 VARCHAR2(128),
        CODE_LEVEL_2 VARCHAR2(128), CODE_LEVEL_3 VARCHAR2(128), CODE_LEVEL_4 VARCHAR2(128),FULL_NAME VARCHAR2(4000),FLAG_LOCAL_GENERAL NUMERIC, 
        filial_id number , subcat_pyramid_name  varchar2(50), cat_pyramid_name  varchar2(50), SUBS_SUBS_ID number, 
        product_finblock varchar2(50) , product_mega_sms varchar2(50) , product_mlk varchar2(50) , product_tariff_change varchar2(50) ,
        profile_mou varchar2(50) , profile_dou varchar2(50) , product_revenue_wo_iitc_vas_roam varchar2(50) ,
        profile_mega_circle_prc varchar2(50) , profile_device_price varchar2(50) , profile_device_type varchar2(50) ,
        profile_lifetime varchar2(50) , segment_name varchar2(50) , product_tp_fee varchar2(50) ,
        profile_util_voice varchar2(50) , profile_util_data varchar2(50) , product_name_line_tp varchar2(50) ,
        product_group_tariff varchar2(50) , network_4g_share varchar2(50) , network_prostoi varchar2(50) ,
        network_volte varchar2(50) , network_vowifi varchar2(50) , product_cd_reprice_2022 varchar2(50) ,
        product_cd_rep1_2023 varchar2(50) , product_cd_repop_2023 varchar2(50) , product_cd_repsimp_2023 varchar2(50) ,
        network_overload varchar2(50) , profile_device_os varchar2(50) , network_sqi_voice varchar2(50) ,
        network_speed_lte varchar2(50) , product_bns varchar2(50) , network_duration_2g varchar2(50) ,
        network_duration_4g varchar2(50) , network_spd_lower_than_2000 varchar2(50) , product_perenos varchar2(50) ,       
        product_eva varchar2(50) , product_family varchar2(50) , product_pre5g varchar2(50) ,
        product_megasil_bespl varchar2(50) , product_uluch varchar2(50) , product_cd_perenos varchar2(50) ,
        product_cd_eva varchar2(50) , product_cd_family varchar2(50) , product_cd_pre5g varchar2(50) ,
        product_cd_megasil_bespl varchar2(50) , product_cd_uluch varchar2(50) , product_pereplaty varchar2(50) ,
        profile_kinopoisk varchar2(50) , profile_telegram varchar2(50) , cd_profile_telegram varchar2(75) , profile_vkontakte varchar2(50) ,
        profile_vkontakte_video varchar2(50) , profile_whatsapp varchar2(50) , profile_whtsapp_call varchar2(50) ,
        profile_whatsapp_videocall varchar2(50) , profile_youtube varchar2(50) ,  cd_profile_youtube varchar2(75),profile_vpn varchar2(50) ,
        region_abc_total varchar2(50) , region_sqi_trend varchar2(50) , region_competitor varchar2(50) ,
        region_abc_coverage varchar2(50) , product_cd_line_tp varchar2(50) , product_cd_group_tariff varchar2(50) ,
        product_pers varchar2(50) , region_refarming varchar2(50) , region_improvement varchar2(50) ,
        region_svo varchar2(50) , region_filial varchar2(50) , network_serv_model varchar2(50) ,
        network_cd_serv_model varchar2(50) , product_cd_debitor varchar2(50) , product_cd_reactiv varchar2(50) ,
        product_cd_risk_base varchar2(50) , product_debitor varchar2(50) , network_sqi_data varchar2(50) ,
        network_radioblock varchar2(50) , profile_complex_factor_product varchar2(50) , profile_complex_factor_network varchar2(50) ,
        product_cd_rep1_2024 varchar2(50) , profile_cd_multi_sim varchar2(50) , product_cd_rep6_2024 varchar2(50),
        network_season_traf varchar2(30) , product_eva_type varchar2(30) , product_cd_eva_type varchar2(30), channel_name varchar2(300), trux_channel_name varchar2(300),
        rep9_2024 varchar2(50),
        FLAG_TEPLOKARTI varchar2(5), RC_ID  varchar2(20) , OPEN_TIME_TEPLOKARTI date , CLOSE_TIME_TEPLOKARTI date, MARK_TEPLOKARTI varchar2(50), DURATION_HOURS_TEPLOKARTI FLOAT      
  );
  COMMIT;
  */
  
  --------------------------------------------------------------------
  --ШАГ 1.1. УКАЗАНИЕ СПИСКА ПОЛЕЙ ДЛЯ ВСТАВКИ В ТАБЛИЦУ TEMP-----------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.kh_proc_cr_inquiry_results_temp
  (
  --------------------------------------------------------------------
  -- [1] ОСНОВНЫЕ ИДЕНТИФИКАТОРЫ И ВРЕМЕННЫЕ ХАРАКТЕРИСТИКИ ----------
  --------------------------------------------------------------------
  COMP_OS_ID, BILLING_FILIAL_ID, ITPC_ITPC_ID, SK_SUBS_ID,
  SNAP_DATE, SNAP_WEEK, SNAP_MONTH, SNAP_QUARTER, SK_BMT_ID,
  SUBS_OS_OS_ID, CR_OS_OS_ID, INQR_ID, LAT, LON,
  DESCRIPTION, CRCAT_CRCAT_ID, CR_WEIGHT,
  CODE_LEVEL_1, CODE_LEVEL_2, CODE_LEVEL_3, CODE_LEVEL_4,
  FULL_NAME, FLAG_LOCAL_GENERAL,
  FILIAL_ID, SUBCAT_PYRAMID_NAME, CAT_PYRAMID_NAME, SUBS_SUBS_ID,

  --------------------------------------------------------------------
  -- [2] ПРОДУКТОВЫЕ ПРИЗНАКИ: ФИНБЛОКИ, СМСКИ, ЛК, ПЕРЕНОСЫ, УСЛУГИ -
  --------------------------------------------------------------------
  PRODUCT_FINBLOCK, PRODUCT_MEGA_SMS, PRODUCT_MLK, PRODUCT_TARIFF_CHANGE, PROFILE_MOU,
  PROFILE_DOU, PRODUCT_REVENUE_WO_IITC_VAS_ROAM, PROFILE_MEGA_CIRCLE_PRC,
  PROFILE_DEVICE_PRICE, PROFILE_DEVICE_TYPE, PROFILE_LIFETIME, SEGMENT_NAME, PRODUCT_TP_FEE,

  --------------------------------------------------------------------  
  -- [3] АКТИВНОСТЬ В ГОЛОСЕ И ДАННЫХ (UTIL) + СЕТЬ И ТАРИФ ----------
  --------------------------------------------------------------------
  PROFILE_UTIL_VOICE, PROFILE_UTIL_DATA,
  PRODUCT_NAME_LINE_TP, PRODUCT_GROUP_TARIFF, NETWORK_4G_SHARE,
  NETWORK_PROSTOI, NETWORK_VOLTE, NETWORK_VOWIFI,
  PRODUCT_CD_REPRICE_2022, PRODUCT_CD_REP1_2023,
  PRODUCT_CD_REPOP_2023, PRODUCT_CD_REPSIMP_2023,
  NETWORK_OVERLOAD, PROFILE_DEVICE_OS, NETWORK_SQI_VOICE,
  NETWORK_SPEED_LTE, PRODUCT_BNS, NETWORK_DURATION_2G,
  NETWORK_DURATION_4G, NETWORK_SPD_LOWER_THAN_2000,

  --------------------------------------------------------------------
  -- [4] УСЛУГИ И ОПЦИИ: ПЕРЕНОСЫ, ЕВА, МЕГАСЕМЬЯ, УЛУЧШЕННЫЕ --------
  --------------------------------------------------------------------
  PRODUCT_PERENOS, PRODUCT_EVA, PRODUCT_FAMILY, PRODUCT_PRE5G, PRODUCT_MEGASIL_BESPL,
  PRODUCT_ULUCH, PRODUCT_CD_PERENOS, PRODUCT_CD_EVA, PRODUCT_CD_FAMILY, PRODUCT_CD_PRE5G,
  PRODUCT_CD_MEGASIL_BESPL, PRODUCT_CD_ULUCH, PRODUCT_PEREPLATY,

  --------------------------------------------------------------------
  -- [5] OTT-ПРИЗНАКИ И ЦИФРОВОЕ ПОВЕДЕНИЕ ---------------------------
  --------------------------------------------------------------------
  PROFILE_KINOPOISK, PROFILE_TELEGRAM, CD_PROFILE_TELEGRAM,
  PROFILE_VKONTAKTE, PROFILE_VKONTAKTE_VIDEO,
  PROFILE_WHATSAPP, PROFILE_WHTSAPP_CALL, PROFILE_WHATSAPP_VIDEOCALL,
  PROFILE_YOUTUBE, CD_PROFILE_YOUTUBE, PROFILE_VPN,

  --------------------------------------------------------------------
  -- [6] РЕГИОНАЛЬНЫЕ ПРИЗНАКИ И ТЕКУЩИЙ ТАРИФ -----------------------
  --------------------------------------------------------------------
  REGION_ABC_TOTAL, REGION_SQI_TREND, REGION_COMPETITOR, REGION_ABC_COVERAGE,
  PRODUCT_CD_LINE_TP, PRODUCT_CD_GROUP_TARIFF, PRODUCT_PERS,
  REGION_REFARMING, REGION_IMPROVEMENT, REGION_SVO, REGION_FILIAL,

    --------------------------------------------------------------------
    -- [7] СЕТЕВАЯ МОДЕЛЬ, ДЕБИТОРКА, РЕАКТИВАЦИЯ, РИСК ----------------
  --------------------------------------------------------------------
  NETWORK_SERV_MODEL, NETWORK_CD_SERV_MODEL,
  PRODUCT_CD_DEBITOR, PRODUCT_CD_REACTIV, PRODUCT_CD_RISK_BASE, PRODUCT_DEBITOR,
  NETWORK_SQI_DATA, NETWORK_RADIOBLOCK,

  --------------------------------------------------------------------
  -- [8] КОМПЛЕКСНЫЕ ФАКТОРЫ / СЕГМЕНТЫ / МУЛЬТИСИМ ------------------
  --------------------------------------------------------------------
  PROFILE_COMPLEX_FACTOR_PRODUCT, PROFILE_COMPLEX_FACTOR_NETWORK,
  PRODUCT_CD_REP1_2024, PROFILE_CD_MULTI_SIM, PRODUCT_CD_REP6_2024,

  --------------------------------------------------------------------
  -- [9] ЕВА-ТИПЫ, КАНАЛЫ ОБРАЩЕНИЙ, РЕПРАЙСЫ, ТЕПЛОКАРТЫ ------------
  --------------------------------------------------------------------
  PRODUCT_CD_EVA_TYPE, PRODUCT_EVA_TYPE,
  CHANNEL_NAME, TRUX_CHANNEL_NAME, REP9_2024,
  FLAG_TEPLOKARTI, RC_ID, OPEN_TIME_TEPLOKARTI,
  CLOSE_TIME_TEPLOKARTI, MARK_TEPLOKARTI, DURATION_HOURS_TEPLOKARTI
 )
 SELECT 
       tariff.COMP_OS_ID, tariff.BILLING_FILIAL_ID, tariff.ITPC_ITPC_ID, tariff.SK_SUBS_ID,
       tariff.SNAP_DATE, tariff.SNAP_WEEK, tariff.SNAP_MONTH, tariff.SNAP_QUARTER,
       tariff.SK_BMT_ID, tariff.SUBS_OS_OS_ID, tariff.CR_OS_OS_ID, tariff.inqr_id, tariff.LAT,
       tariff.LON, tariff.DESCRIPTION, tariff.CRCAT_CRCAT_ID, tariff.CR_WEIGHT,
       tariff.CODE_LEVEL_1, tariff.CODE_LEVEL_2, tariff.CODE_LEVEL_3, tariff.CODE_LEVEL_4, 
       tariff.FULL_NAME, tariff.FLAG_LOCAL_GENERAL, tariff.FILIAL_ID, tariff.SUBCAT_PYRAMID_NAME, 
       tariff.CAT_PYRAMID_NAME, tariff.SUBS_SUBS_ID,
       
       CASE WHEN NVL(DAYS_IN_FB_S12,0)=0 THEN '1. Нет ФБ'
            WHEN NVL(DAYS_IN_FB_S12,0)<1 THEN '2. Менее 1 дня'
            WHEN NVL(DAYS_IN_FB_S12,0)<5 THEN '3. 1-5 дней'  
            WHEN NVL(DAYS_IN_FB_S12,0)<30 THEN '4. 5-30 дней'    
            ELSE '5. Более 30 дней' END AS PRODUCT_FINBLOCK,
       CASE WHEN NVL(ALL_MEGA_SMS_CNT,0)=0 THEN '1. Нет SMS'
            WHEN NVL(ALL_MEGA_SMS_CNT,0)<5 THEN '2. Менее 5 SMS'
            WHEN NVL(ALL_MEGA_SMS_CNT,0)<10 THEN '3. 5-10 SMS'  
            WHEN NVL(ALL_MEGA_SMS_CNT,0)<30 THEN '4. 11-20 SMS'    
            ELSE '5. Более 21 SMS' END AS PRODUCT_MEGA_SMS,
       CASE WHEN NVL(CNT_LK,0)=0 THEN '1. Нет заходов'
            WHEN NVL(CNT_LK,0)<=1 THEN '2. 1 заход'
            WHEN NVL(CNT_LK,0)<=3 THEN '3. 1-3 заход'  
            ELSE '4. Более 3 заходов' END AS PRODUCT_MLK,              
       CASE WHEN DAYS_SINCE_CHNG IS NULL THEN '6. Прочее'
            WHEN NVL(DAYS_SINCE_CHNG,0)<30 THEN '1. До 30 дней'
            WHEN NVL(DAYS_SINCE_CHNG,0)<90 THEN '2. 31-90 дней'
            WHEN NVL(DAYS_SINCE_CHNG,0)<180 THEN '3. 91-180 дней'  
            WHEN NVL(DAYS_SINCE_CHNG,0)<366 THEN '4. 181-366 дней'  
            ELSE '5. Более 366 дней' END AS PRODUCT_TARIFF_CHANGE,        
       CASE WHEN NVL(MOU_M1,0)=0 then '1. Нет'
            WHEN NVL(MOU_M1,0)<5 then '2. До 5'
            WHEN NVL(MOU_M1,0)<100 then '3. 5-100'
            WHEN NVL(MOU_M1,0)<500 then '4. 100-500'    
            ELSE '5. Более 500' END  AS PROFILE_MOU,       
       CASE WHEN NVL(DOU_M1,0)=0 then '1. Нет' 
            WHEN NVL(DOU_M1,0)<100 then '2. До 100 МБ'
            WHEN NVL(DOU_M1,0)<1000 then '3. 0.1 - 1 ГБ' 
            WHEN NVL(DOU_M1,0)<10000 then '4. 1 - 10 ГБ' 
            ELSE '5. Более 10 ГБ' END AS PROFILE_DOU,
       CASE WHEN NVL(REVENUE_WO_ITC_VAS_ROAM_MA3,0)<100 THEN '1. До 100 руб'
            WHEN NVL(REVENUE_WO_ITC_VAS_ROAM_MA3,0)<500 THEN '2. 101-500 руб'
            ELSE '3. Более 500 руб' END AS PRODUCT_REVENUE_WO_IITC_VAS_ROAM,  
       CASE WHEN MEGA_CIRCLE_PRC IS NULL THEN '4. Прочее'
            WHEN MEGA_CIRCLE_PRC>0.85 THEN '1. Более 85%'
            WHEN MEGA_CIRCLE_PRC>0.15 THEN '2. 15...85%'             
            ELSE '3. Менее 15%' END AS PROFILE_MEGA_CIRCLE_PRC,
       CASE WHEN NVL(DEVICE_PRICE,0)<1 THEN '1. Нет данных'
            WHEN NVL(DEVICE_PRICE,0)<10000 THEN '2. До 10 тыс. руб.'
            WHEN NVL(DEVICE_PRICE,0)<25000 THEN '3. 10-25 тыс. руб.'  
            WHEN NVL(DEVICE_PRICE,0)<50000 THEN '4. 25-50 тыс. руб.'    
            ELSE '5. Более 50 тыс. руб' END AS PROFILE_DEVICE_PRICE,
       DEVICE_TYPE_END AS PROFILE_DEVICE_TYPE,
       CASE WHEN NVL(LIFETIME,0)<30 THEN '1. Менее 30 дней'
            WHEN NVL(LIFETIME,0)<183 THEN '2. До 6 мес.'
            WHEN NVL(LIFETIME,0)<366 THEN '3. От 6 мес. до года'
            ELSE '4. Более 1 года' END AS PROFILE_LIFETIME,      
       SEGMENT_NAME,
       CASE WHEN NVL(TP_FEE,0)<1 THEN '5. Прочее'
            WHEN NVL(TP_FEE,0)<=200 THEN '1. До 200 руб.'
            WHEN NVL(TP_FEE,0)<=500 THEN '2. 201-500 руб.'
            WHEN NVL(TP_FEE,0)<=1000 THEN '3. 501-1000 руб.'  
            ELSE '4. Более 1000 руб' END AS PRODUCT_TP_FEE,       
       CASE WHEN SPLIT_UTIL_VOICE IS NULL THEN 'Прочее'
            WHEN SPLIT_UTIL_VOICE IN ('a) 0-10%','b) 10-20%','c) 20-30%') THEN '1. 0-30%'
            WHEN SPLIT_UTIL_VOICE IN ( 'd) 30-40%', 'e) 40-50%', 'f) 50-60%',
                                       'g) 60-70%', 'h) 70-80%', 'i) 80-90%',
                                       'j) 90-100%' ) THEN '2. 30-100%'
            WHEN SPLIT_UTIL_VOICE IN ( 't) 0% (нет тариф. трафика)', 'u) 0% (нет трафика)',
                                       'w) нет пакета голоса') THEN '5. Нет трафика/пакета'
            WHEN SPLIT_UTIL_VOICE IN ( 'v) unlim') THEN '4. Безлимит'
            ELSE '3. >100%' END AS PROFILE_UTIL_VOICE,
       CASE WHEN SPLIT_UTIL_DATA IS NULL THEN 'Прочее'
            WHEN SPLIT_UTIL_DATA IN ('a) 0-10%','b) 10-20%','c) 20-30%') THEN '1. 0-30%'
            WHEN SPLIT_UTIL_DATA IN ( 'd) 30-40%', 'e) 40-50%', 'f) 50-60%',
                                       'g) 60-70%', 'h) 70-80%', 'i) 80-90%',
                                       'j) 90-100%' ) THEN '2. 30-100%'
            WHEN SPLIT_UTIL_DATA IN ( 't) 0% (нет тариф. трафика)', 'u) 0% (нет трафика)',
                                       'w) нет пакета голоса') THEN '5. Нет трафика/пакета'
            WHEN SPLIT_UTIL_DATA IN ( 'v) unlim') THEN '4. Безлимит'
            ELSE '3. >100%' END AS PROFILE_UTIL_DATA,  
       NVL(tariff.NAME_LINE_TP,'Прочее') AS PRODUCT_NAME_LINE_TP,
       tariff.group_tariff AS PRODUCT_GROUP_TARIFF,
       NETWORK_4G_SHARE,
       CASE WHEN NVL(PROSTOI_DUR,0)>=24 THEN '1. Более 24 часов'
         WHEN NVL(PROSTOI_DUR,0)>=6 THEN '2. 5-24 часа'
            WHEN NVL(PROSTOI_DUR,0)>=1 AND NVL(PROSTOI_DUR,0) <=5 THEN '3. 1-5 часов'
            WHEN NVL(INC_NUMBER,0)>=1 THEN '4. Риск простоя'
            ELSE '5. Нет простоя' END AS NETWORK_PROSTOI,              
       CASE WHEN NVL(SHARE_VOLTE,0)>=80 THEN '1. Более 80%'
            WHEN NVL(SHARE_VOLTE,0)>=1 THEN '2. До 80%'
            ELSE '3. Не VoLTE' END AS NETWORK_VOLTE,
       CASE WHEN NVL(SHARE_VOWIFI,0)>=0.01 THEN 'Есть'
            ELSE 'Нет' END AS NETWORK_VOWIFI,
       CASE WHEN NVL(reprice_2022,0)>0 THEN 'Да'ELSE 'Нет' END AS PRODUCT_REPRICE_2022,
       CASE WHEN NVL(reprice_2023,0)>0 THEN 'Да' ELSE 'Нет' END AS PRODUCT_REP1_2023,       
       CASE WHEN NVL(optout_2023,0)>0 THEN 'Да' ELSE 'Нет' END AS PRODUCT_REPOP_2023,
       CASE WHEN NVL(simpl_2023,0)>0 THEN 'Да' ELSE 'Нет' END AS PRODUCT_REPSIMP_2023,
       CASE WHEN NVL(OVERLOAD,0)=0 THEN '1. Нет'
            WHEN OVERLOAD>=15 THEN '3. Более 15 дней'
            WHEN OVERLOAD>=1 THEN '2. До 15 дней'  
            ELSE 'Прочее' END AS NETWORK_OVERLOAD,
       CASE WHEN DEVICE_OS IN ('Android', 'iOS') THEN DEVICE_OS ELSE 'Прочее' END AS PROFILE_DEVICE_OS,
       CASE WHEN SQI_VOICE IS NULL THEN '4. Прочее'
            WHEN SQI_VOICE>99.5 THEN '1. >99.5%'
            WHEN SQI_VOICE>98 THEN '2. 98..99.5%'
            ELSE '3. <98%' END AS NETWORK_SQI_VOICE,     
       CASE WHEN SPEED_LTE IS NULL THEN '5. Прочее'
            WHEN SPEED_LTE>10 THEN '4. >10 Mbps'
            WHEN SPEED_LTE>5 THEN '3. 5...10 Mbps'  
            WHEN SPEED_LTE>2 THEN '2. 2...5 Mbps'    
            ELSE '1. <2 Mbps' END AS NETWORK_SPEED_LTE,
       CASE WHEN BNS IS NULL THEN 'Прочее'
            WHEN BNS=1 THEN 'Да'
            ELSE 'Нет' END AS PRODUCT_BNS,
       CASE WHEN DURATION_2G_PERCENT IS NULL 
                 OR NVL(PROFILE_BEARER_4G,0)=0 
                 OR NVL(DEVICE_TYPE_END,'')<>'SMARTPHONE' THEN 'Прочее'
            WHEN DURATION_2G_PERCENT>20 THEN '3. Более 20%'
            WHEN DURATION_2G_PERCENT>5 THEN '2. 5...20%'  
            ELSE '1. Менее 5%' END AS NETWORK_DURATION_2G,
       CASE WHEN DURATION_4G_PERCENT IS NULL OR DURATION_4G_PERCENT<1 THEN 'Прочее'
            WHEN DURATION_4G_PERCENT>80 THEN 'Более 80%'  
            ELSE 'Менее 80%' END AS NETWORK_DURATION_4G,                                         
       CASE WHEN SPD_LOWER_THAN_2000_KBPS_RATE IS NULL THEN 'Прочее'
            WHEN SPD_LOWER_THAN_2000_KBPS_RATE>80 THEN '3. Более 80%'  
            WHEN SPD_LOWER_THAN_2000_KBPS_RATE>20 THEN '2. 20...80%'    
            ELSE '1. Менее 20%' END AS NETWORK_SPD_LOWER_THAN_2000, 
            
        -- Услуги       
       CASE WHEN NVL(PRODUCT_PERENOS,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_PERENOS,
       CASE WHEN NVL(PRODUCT_EVA,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_EVA,  
       CASE WHEN NVL(PRODUCT_FAMILY_MANAGER,0)=1 THEN 'Управление номерами'
         
            WHEN NVL(PRODUCT_FAMILY,0)=1 THEN 'МегаСемья' 
            ELSE 'Нет' END AS PRODUCT_FAMILY,  
       CASE WHEN NVL(PRODUCT_PRE5G,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_PRE5G,    
       CASE WHEN NVL(PRODUCT_MEGASIL_BESPL,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_MEGASIL_BESPL,      
       CASE WHEN NVL(PRODUCT_ULUCH,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_ULUCH,        
       CASE WHEN NVL(PRODUCT_CD_PERENOS,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_CD_PERENOS,  
       CASE WHEN NVL(PRODUCT_CD_EVA,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_CD_EVA,  
       CASE WHEN NVL(PRODUCT_CD_FAMILY_MANAGER,0)=1 THEN 'Управление номерами' 
            WHEN NVL(PRODUCT_CD_FAMILY,0)=1 THEN 'МегаСемья' 
            ELSE 'Нет' END AS PRODUCT_CD_FAMILY,  
       CASE WHEN NVL(PRODUCT_CD_PRE5G,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_CD_PRE5G,  
       CASE WHEN NVL(PRODUCT_CD_MEGASIL_BESPL,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_CD_MEGASIL_BESPL,    
       CASE WHEN NVL(PRODUCT_CD_ULUCH,0)=1 THEN 'Да' Else 'Нет' END AS PRODUCT_CD_ULUCH,   
         
       -- VAS
       CASE WHEN NVL(PRODUCT_PEREPL_MN,0)+NVL(PRODUCT_PEREPL_GOR,0)+  
                 NVL(PRODUCT_PEREPL_OPER,0)+NVL(PRODUCT_PEREPL_SMS,0)+
                 NVL(PRODUCT_UDERZH,0)>40 THEN '1. Более 40 руб'
            WHEN NVL(PRODUCT_PEREPL_MN,0)+NVL(PRODUCT_PEREPL_GOR,0)+  
                 NVL(PRODUCT_PEREPL_OPER,0)+NVL(PRODUCT_PEREPL_SMS,0)+
                 NVL(PRODUCT_UDERZH,0)>0 THEN '2. До 40 руб'     
            ELSE '3. Нет переплат' END AS PRODUCT_PEREPLATY, 
              
        -- OTT     
      CASE WHEN NVL(PROFILE_KINOPOISK,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_KINOPOISK,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_KINOPOISK,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_KINOPOISK,
      CASE WHEN NVL(PROFILE_TELEGRAM,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_TELEGRAM,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_TELEGRAM,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_TELEGRAM,
      cd_t.cd_PROFILE_TELEGRAM,
      CASE WHEN NVL(PROFILE_VKONTAKTE,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_VKONTAKTE,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_VKONTAKTE,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_VKONTAKTE,
      CASE WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_VKONTAKTE_VIDEO,                    
      CASE WHEN NVL(PROFILE_WHATSAPP,0)+NVL(PROFILE_WHATSAPP_MEDIA,0) >1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_WHATSAPP,0)+NVL(PROFILE_WHATSAPP_MEDIA,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_WHATSAPP,0)+NVL(PROFILE_WHATSAPP_MEDIA,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_WHATSAPP,   
      CASE WHEN NVL(PROFILE_WHTSAPP_CALL,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_WHTSAPP_CALL,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_WHTSAPP_CALL,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_WHTSAPP_CALL,            
      CASE WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_WHATSAPP_VIDEOCALL, 
      CASE WHEN NVL(PROFILE_YOUTUBE,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_YOUTUBE,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_YOUTUBE,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_YOUTUBE,
      cd_y.cd_PROFILE_YOUTUBE,        
      CASE WHEN NVL(PROFILE_VPN,0)>1000000000 THEN '1. Более 1GB'
           WHEN NVL(PROFILE_VPN,0)>100000000 THEN '2. 100MB - 1GB'
           WHEN NVL(PROFILE_VPN,0)>100 THEN '3. До 100MB'
           ELSE '4. Нет' END AS PROFILE_VPN,
      --РЕГИОН                   
      REGION_ABC_TOTAL,
      REGION_SQI_TREND,
      REGION_COMPETITOR,
      REGION_ABC_COVERAGE,
      --ТАРИФ НА ТЕКУЩУЮ ДАТУ
      NVL(cur_tariff.NAME_LINE_TP, 'Прочее') AS PRODUCT_CD_LINE_TP,
      cur_tariff.GROUP_TARIFF AS PRODUCT_CD_GROUP_TARIFF,
      CASE WHEN PRODUCT_PERS IS NULL THEN 'Нет миграции'
           WHEN PRODUCT_PERS = 'Без информирования' THEN 'Без информ.'
           ELSE 'Склейка и пр.' END AS PRODUCT_PERS,      
      ----РЕГИОН
      REGION_REFARMING,
      REGION_IMPROVEMENT,
      REGION_SVO,
      REGION_FILIAL,
      --СЕРВИСНАЯ МОДЕЛЬ НА СЕГОДНЯ
      CASE WHEN NVL(NETWORK_SERV_MODEL,0)=2 THEN 'PRIOR'
           WHEN NVL(NETWORK_SERV_MODEL,0)=3 THEN 'NORMAL'
           WHEN NVL(NETWORK_SERV_MODEL,0)=4 THEN 'BASE'
           ELSE 'Прочее' END AS NETWORK_SERV_MODEL, 
      CASE WHEN NVL(NETWORK_CD_SERV_MODEL,0)=2 THEN 'PRIOR'
           WHEN NVL(NETWORK_CD_SERV_MODEL,0)=3 THEN 'NORMAL'
           WHEN NVL(NETWORK_CD_SERV_MODEL,0)=4 THEN 'BASE'
           ELSE 'Прочее' END AS NETWORK_CD_SERV_MODEL,     
      -- РАБОТА С ДЕБИТОРКОЙ    
      CASE WHEN debitor IS NULL THEN 'нет' ELSE 'Да' END AS PRODUCT_CD_DEBITOR, 
      --РЕАКТИВАЦИИ
      CASE WHEN in_react IS NULL THEN 'нет' ELSE 'Да' END AS PRODUCT_CD_REACTIV,        
      --РИСКОВАЯ БАЗА
      CASE WHEN risk_base IS NULL THEN 'нет' ELSE 'Риск база' END AS PRODUCT_CD_RISK_BASE,      
      NVL(PRODUCT_DEBITOR, 'Прочее') AS PRODUCT_DEBITOR,         
      CASE WHEN SQI_DATA IS NULL THEN '4. Прочее'
            WHEN SQI_DATA>99.5 THEN '1. >99.5%'
            WHEN SQI_DATA>98 THEN '2. 98...99.5%'
            ELSE '3. <98%' END AS NETWORK_SQI_DATA,             
      --ПОМЕХИ НА РАДИО    
      CASE WHEN NVL(problemcell5,0)>=1 THEN '1. Топ 5 с помехами'
           WHEN NVL(problemcell,0)>=10 THEN '2. Помехи более 10 площадок'
           WHEN NVL(problemcell,0)>=1 THEN '3. Прочие помехи'  
           ELSE '4. Нет' END AS NETWORK_RADIOBLOCK,                  
      CASE WHEN  NVL(PRODUCT_DEBITOR, 'Прочее')<>'Прочее' OR
                 PRODUCT_PERS IS NOT NULL OR
                 --NVL(reprice_2022,0)>0 OR
                 NVL(reprice_2023,0)>0 OR     
                 NVL(optout_2023,0)>0 OR
                 NVL(simpl_2023,0)>0 THEN 'Негативный фактор'
                 ELSE 'Прочее' END AS PROFILE_COMPLEX_FACTOR_PRODUCT,
                                 
      'tbd' AS PROFILE_COMPLEX_FACTOR_NETWORK, 
      NVL(rep1_2024,'Нет') AS PRODUCT_CD_REP1_2024,
      CASE WHEN NVL(f2_class,'')='single_sim' THEN '1.Одна SIM'
           WHEN NVL(f2_class,'')='ms_main_not_mf' THEN '5.Мульти, МФ-дополн.' 
           WHEN NVL(f2_class,'')='inner_ms_not_main' THEN '3.Внутрен. дополн.' 
           WHEN NVL(f2_class,'')='inner_ms_main' THEN '2.Внутрен. основная'
           WHEN NVL(f2_class,'')='ms_main_mf' THEN '4.Мульти, МФ-основной' 
           ELSE '6.Прочее' END AS PROFILE_CD_MULTI_SIM,
      NVL(rep6_2024,'Нет') AS PRODUCT_CD_REP6_2024,
      /*,CASE WHEN seas.segment = 'Digital' THEN '1. Преобладает город'
           WHEN seas.segment = 'Oblast' THEN '2. Преобладает область'
           WHEN seas.segment = 'Equals' THEN '3. Поровну'
           ELSE '4. Нет трафика' END AS NETWORK_SEASON_TRAF*/
      CASE WHEN NVL(PRODUCT_CD_EVA_PLUS,0)=1 THEN 'Ева+'
            WHEN NVL(PRODUCT_CD_EVA_MLK,0)=1 THEN 'Ева МЛК'
            WHEN NVL(PRODUCT_CD_EVA_VOLTE,0)=1 THEN 'Ева Volte'
            Else 'Нет' END AS PRODUCT_CD_EVA_TYPE,
      CASE WHEN NVL(PRODUCT_EVA_PLUS,0)=1 THEN 'Ева+'
            WHEN NVL(PRODUCT_EVA_MLK,0)=1 THEN 'Ева МЛК'
            WHEN NVL(PRODUCT_EVA_VOLTE,0)=1 THEN 'Ева Volte'
            Else 'Нет' END AS PRODUCT_EVA_TYPE,
      CA.name,
      CHA.trux_channel_name,
      rep9_2024.rep9_2024,
      TEPL.FLAG_TEPLOKARTI , TEPL.RC_ID, TEPL.OPEN_TIME_TEPLOKARTI, TEPL.CLOSE_TIME_TEPLOKARTI , TEPL.MARK_TEPLOKARTI, TEPL.DURATION_HOURS_TEPLOKARTI 
  FROM 
    --РЕЗУЛЬТАТЫ ОБРАЩАЕМОСТИ
    (SELECT * FROM fin_ba.kh_proc_cr_inquiry_results_tariff tariff) tariff
    --УСЛУГИ
    LEFT OUTER JOIN fin_ba.kh_proc_cr_inquiry_results_service_view service
    ON tariff.sk_subs_id = service.sk_subs_id
       AND tariff.snap_date = service.snap_date
    --VAS
    LEFT OUTER JOIN fin_ba.kh_proc_cr_inquiry_results_vas vas
    ON tariff.sk_subs_id = vas.sk_subs_id
       AND tariff.snap_date = vas.snap_date
    --OTT
    LEFT OUTER JOIN fin_ba.kh_proc_cr_inquiry_results_ott_view ott
    ON tariff.sk_subs_id = ott.sk_subs_id
       AND tariff.snap_date = ott.snap_date
    --РЕГИОН
    LEFT OUTER JOIN mr_nps_region reg
    ON tariff.CR_OS_OS_ID = reg.os_os_id
    --ТАРИФ НА ТЕКУЩУЮ ДАТУ
    LEFT OUTER JOIN
    (SELECT /*+ parallel (8)*/ 
        sk_subs_id, name_r, name_line_tp, group_tariff
    FROM PUB_DS.F_SUBS_CHURN_TRG_WEEKLY churn
    LEFT OUTER JOIN fin_ba.mr_tariff tariff_report
      ON churn.rtpl_rtpl_id = tariff_report.rtpl_id
    WHERE start_date = trunc(sysdate, 'iw')-7 
        AND billing_filial_id=10) cur_tariff
    ON tariff.sk_subs_id = cur_tariff.sk_subs_id
    --СЕРВИСНАЯ МОДЕЛЬ НА СЕГОДНЯ
    LEFT OUTER JOIN
    (SELECT --+parallel(10)
       sk_subs_id, priority_priority_id AS network_cd_serv_model
      FROM pub_ds.s_subs_priority_daily
      WHERE billing_filial_id = 10 
          AND snap_date=trunc(sysdate,'dd')-7) sm
    ON tariff.sk_subs_id = sm.sk_subs_id
    --РАБОТА С ДЕБИТОРКОЙ
    LEFT OUTER JOIN
    (SELECT  --+parallel(10)
     DISTINCT cc.sk_subs_id, 1 AS debitor
     FROM PUB_STG.D_CIM_COMPAIGN_CUSTOMERS CC  
     WHERE RUN_DTTM between date '2023-04-01' and date '2024-12-01'     ---------- даты запусков кампаний
     and cc.communication_id in (select distinct COMMUNICATION_ID from MAXIM_MAKARENKOV.CIM_COMPAIGN)
     and cc.disposition_type=1 
     and cc.response_type>0
    ) deb
    ON tariff.sk_subs_id = deb.sk_subs_id
    --РЕАКТИВАЦИИ
    LEFT OUTER JOIN
    (SELECT /*+ parallel(10)*/ 
     DISTINCT subs_subs_id, 'Реактивация' AS in_react 
     FROM pub_ds.bis_subs_packs y
     where 1=1 and (y.navi_user like '%IVAN_CHISTOV_NOSMS%' or y.navi_user like '%KAMIL_IBRAGIMOV%')
           and y.start_date >= '01.08.2022') react
    ON tariff.subs_subs_id = react.subs_subs_id
    --РИСКОВАЯ БАЗА
    LEFT OUTER JOIN
    (SELECT  --+parallel(10) 
     DISTINCT sk_subs_id, 'Риск база' AS risk_base
     FROM AME_RISK_COMPETITOR_SUBS_v2
     WHERE flag_risk_big4=1 and flag_base_for_risk=1 
          AND report_month = TRUNC(SYSDATE-20, 'mm')) rb
    ON tariff.sk_subs_id = rb.sk_subs_id
    --РЕПРАЙС 2023
    LEFT OUTER JOIN 
    (SELECT  --+parallel(10) 
         DISTINCT sk_subs_id, 1 AS reprice_2023
    FROM 
    ((SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_DB_TP_PROD_final_best_s3)
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_DB_OPC_PROD_final)
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_Bundly_Final))) rep_2023
    ON tariff.sk_subs_id = rep_2023.sk_subs_id
    --СИМПЛИФИКАЦИЯ 2023
    LEFT OUTER JOIN 
    (SELECT  --+parallel(10) 
         DISTINCT sk_subs_id, 1 AS simpl_2023
    FROM alexey_v_trukhachev.REPRICE_1Q23_Simply_fin_AllAbon) simpl_2023
    ON tariff.sk_subs_id = simpl_2023.sk_subs_id
    --ОПТАУТ
    LEFT OUTER JOIN
    (SELECT  --+parallel(10) 
        DISTINCT sk_subs_id, 1 AS optout_2023
    FROM 
    ((SELECT sk_subs_id FROM alexey_v_trukhachev.TAV_REPRICE_1Q23_OPTOUT )
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.TAV_REPRICE_2Q23_OPTOUT ))) oo_2023
    ON tariff.sk_subs_id = oo_2023.sk_subs_id
    --РЕПРАЙС 2022
    LEFT OUTER JOIN 
    (SELECT  --+parallel(10) 
         DISTINCT sk_subs_id, 1 AS reprice_2022
    FROM 
    ((SELECT sk_subs_id FROM KEA_REPRICE_2022 )
    UNION
    (SELECT sk_subs_id FROM KEA_REPRICE_2022_2 )
    UNION
    (SELECT sk_subs_id FROM KEA_REPRICE_2022_3 )
    UNION
    (SELECT sk_subs_id FROM KEA_REPRICE_2022_SNG )
    UNION
    (SELECT sk_subs_id FROM KEA_REPRICE_2022_2_SNG )
    UNION
    (SELECT sk_subs_id FROM KEA_REPRICE_2022_B2B ))) rep_2022
    ON tariff.sk_subs_id = rep_2022.sk_subs_id
    --ПЕРСОНАЛЬНЫЙ
    LEFT OUTER JOIN 
    (SELECT sk_subs_id, MAX(case_) AS PRODUCT_PERS
    FROM
    (
      select  --+parallel(10)
        distinct vv.subs_subs_id, x.sk_subs_id, CASE_, dispatch_date, date_change 
               from (
                   select t1.subs_subs_id, t1.CASE_, t1.dispatch_date, t1.date_change from rep_core_b2c.Reprice_3Q23_freeze t1      
                   union select t2.subs_subs_id,t2.CASE_, t2.dispatch_date, t2.date_change from rep_core_b2c.Reprice_3Q23_freeze_no_inf t2      
                   union select subs_subs_id,case when COMMENT_S = '800_no_inf' or COMMENT_S = '20_no_inf' then 'Без информирования' when COMMENT_S = '316_ottok' then 'Отток' when COMMENT_S = '316_no_gold_fikt' then 'Фиктивная скидка' when COMMENT_S = '316_no_no' then 'Исключение' end CASE_, t.dispatch_date, t.date_change_new as date_change from alexey_ponomarev.PAV_REPRICE_3Q23_1_VOLNA_FOR_IT t
                  ) vv
               left join alexey_v_Trukhachev.TAV_REPRICE_3Q23_SMALL_TRANSFER_Xv3 x on vv.subs_subs_id = x.subs_subs_id  
    )
    GROUP BY sk_subs_id
    ) pers
    ON tariff.sk_subs_id = pers.sk_subs_id 
    --ПОМЕХИ НА РАДИО
    LEFT OUTER JOIN 
    (SELECT start_date, sk_subs_id, 
        MAX(has_traf_in_problemcell) problemcell, 
        MAX(has_traf_in_problemcell_top5) problemcell5
     FROM fin_ba.kh_proc_cr_inquiry_radioblock
     GROUP BY start_date, sk_subs_id) radioblock
    ON tariff.sk_subs_id = radioblock.sk_subs_id AND tariff.snap_month = radioblock.start_date
    --РЕПРАЙС 2024 ЯНВАРЬ
    LEFT OUTER JOIN
         (SELECT NVL(rep_fact.sk_subs_id, rep_plan.sk_subs_id) AS sk_subs_id,
         CASE --WHEN NVL(date_change_itog, SYSDATE+1)<SYSDATE THEN 'Переведен'
          WHEN NVL(dispatch_date, SYSDATE+1)<SYSDATE THEN 'Проинформирован'
         ELSE 'Переведен' END AS rep1_2024
    FROM 
    (SELECT sk_subs_id, MIN(dispatch_date) AS dispatch_date,
         MIN(date_change_itog) AS date_change_itog
    FROM rep_b2b.pav_reprice_1q2024_freeze
    GROUP BY sk_subs_id) rep_fact
    FULL OUTER JOIN
    (SELECT distinct sk_subs_id
    FROM alexey_ponomarev.PAV_REPRICE_1Q24_FOR_CASE
    WHERE CASE_LIFT in ('base_mig_2023','Opt_out_and_BP_2X','Pers_3_0')
    group by sk_subs_id) rep_plan
    ON rep_fact.sk_subs_id = rep_plan.sk_subs_id) reprice1_2024
    ON tariff.sk_subs_id = reprice1_2024.sk_subs_id
    --РЕПРАЙС 2024 ИЮНЬ + отмена скидки + репрайс БИ
    LEFT OUTER JOIN
    (-- updated by a_gavr 07/10/2024 begin
     SELECT /*+ parallel(12)*/
     t.sk_subs_id,
     CASE
       WHEN s.msisdn IS NOT NULL THEN 'Проинформирован'
       WHEN act_subs_tp.rtpl_rtpl_id = 502865 THEN 'Переведен' ELSE 'План' END rep6_2024
    FROM rep_core_b2c.pav_reprice_2q2024_bundle_freeze_history_copy t -- общий массив всех фризов
    LEFT JOIN alexey_linyov.msisdn_reprice_cvm_rezerv s -- таблица всех СМС, кого информировали 
    ON t.msisdn = CASE
         WHEN s.dispatch_date = '27.06.2024' THEN substr(s.msisdn, -10) ELSE s.msisdn END
    LEFT JOIN (SELECT sk_subs_id, rtpl_rtpl_id FROM pub_ds.s_subs_activities
           WHERE snap_date = trunc(SYSDATE - 1, 'dd')
             AND billing_filial_id = 10) act_subs_tp -- проверка наличия активного перс тарифа
    ON t.sk_subs_id = act_subs_tp.sk_subs_id
    WHERE
    VERS IN (0.1, 1.2, 2.1, 3.1, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.8, 4.9, 5.1, 6.1, 6.2, 6.3, 
         7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6)
    union
    --РЕПРАЙС БИ 2024 (с архивными тарифами, с бесплатной опцией БИ)
    SELECT distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_BI_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
          WHEN NVL(DISPATCH_DATE_BI_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep_bi_2024
    FROM rep_core_b2c.mta_reprice_unlim_data_id_503694_4q2024_freeze
    union
    --РЕПРАЙС ОТМЕНА СКИДКИ 2024 (абоны с бессрочной скидкой - либо оплностью отменяем, либо уменьшаем)
    SELECT distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_skidka_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
          WHEN NVL(DISPATCH_DATE_SKIDKA_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep_skidka_2024
    FROM rep_core_b2c.mta_reprice_skidka_4q2024_freeze
    -- updated by a_gavr 07/10/2024 end
    ) rep6_24
    on tariff.sk_subs_id = rep6_24.sk_subs_id
    --СЕГМЕНТ МУЛЬТИСИМ
    LEFT OUTER JOIN
    (SELECT --+parallel(10)
        sk_subs_id, 
         MAX(f2_class) AS f2_class
    FROM BD_SCIENTIST_USER.OLD_BASE_MULTISIM_PREPREDICT_V3
    WHERE score_date = (SELECT /*+ parallel (8)*/ 
              MAX(score_date)
              FROM BD_SCIENTIST_USER.OLD_BASE_MULTISIM_PREPREDICT_V3
              WHERE score_date>='26.02.2024')
    GROUP BY sk_subs_id) multi_sim
    ON tariff.sk_subs_id = multi_sim.sk_subs_id

    --КАНАЛ М 
    left join ( Select /*+ parallel(20)*/ * from pub_ds.F_INQUIRY b where CREATE_DATE >= '01.01.2023' AND BILLING_FILIAL_ID = 10) BA ON BA.INQR_ID  = tariff.INQR_ID
    left join ( Select /*+ parallel(20)*/ * from pub_ds.BIS_CMS_SITE_DEF where billing_filial_id = 10) CA on CA.stdf_id = BA.stdf_stdf_id

    --КАНАЛ ТРУХ
    LEFT JOIN (
      Select a.*
      from (

      Select DATE_MONTH, INQR_ID, "Канал" AS trux_channel_name
      from (
        Select
         trunc(FI.CREATE_DATE,'mm') DATE_MONTH,
         INQR_ID, 
         chnl_chnl_id,
         CASE WHEN HO1.FULL_OPER_NAME like '%Интернет%агазин%' then 'КЦ (Интернет-Магазин)' WHEN HO1.FULL_OPER_NAME like '%ентр%сохранен%клиентов%' then 'КЦ'--'КЦ (привлечение SD)' 
            WHEN BD.DEF='Контактный центр' and BC.CHNL='Звонок' then 'КЦ' 
            WHEN BD.DEF='Контактный центр' and BC.CHNL like '%очта%' then 'Почта' 
            WHEN BD.DEF='Контактный центр' and (BC.CHNL like '%сайт%' OR  BC.CHNL like '%Чат%' OR  BC.CHNL like '%ЧАТ%'  OR  BC.CHNL like '%FMC%' 
            OR  BC.CHNL like '%ичный%' OR  BC.CHNL like '%МЛК%') then 'Чат'  
            WHEN BD.DEF='Контактный центр' and (BC.CHNL like '%SMS%' OR BC.CHNL like '%Внешний КЦ%' OR BC.CHNL  like '%Факс%' OR BC.CHNL like '%Визит%' 
            OR BC.CHNL like '%Внутренний%' OR BC.CHNL like '%Персональный менеджер%') then 'SMS'     
            WHEN BD.DEF='Федеральные соц.сети' then 'ФСС'
            WHEN BD.DEF IN ('МегаФон Ритейл') then 'МФР' 
            WHEN BD.DEF IN ('Фирменный салон') then 'ФС' 
            WHEN BD.DEF IN ('Мультибрендовый салон','Мультибрендовый салон с обслуживанием') then 'МБС' 
        else 'Другие'
        end  as "Канал" 
       from PUB_DS.F_INQUIRY FI
       
       
         JOIN    --------Темы------------  
               (SELECT --+parallel (10)
              H.ITPC_ID,H.START_DATE,H.END_DATE,H.FULL_NAME,H.CLEAN_CODE_LEVEL_1,H.CODE_LEVEL_2,H.CODE_LEVEL_3,H.CODE_LEVEL_4,H.BILLING_FILIAL_ID
               FROM PUB_DS.H_TOPIC H 
               WHERE  
               (H.FULL_NAME LIKE '%Действие%'
               OR H.FULL_NAME  LIKE  '%Консультация%'
               OR H.FULL_NAME  LIKE  '%Недовольство%'
               OR H.FULL_NAME  LIKE  '%Претензия%'
               OR H.FULL_NAME  LIKE  '%Пожелание%'
               OR H.FULL_NAME  LIKE  '%Переоформление%'
               OR H.FULL_NAME  LIKE  '%Попытка продаж%'
               OR H.FULL_NAME  LIKE  '%Внутренний%'
               ) --AND H.FULL_NAME  LIKE  '%Тарифные опции%' 
               AND H.BILLING_FILIAL_ID in (10,9)
               ) HT ON HT.ITPC_ID=FI.ITPC_ITPC_ID AND  HT.START_DATE<=FI.CREATE_DATE AND HT.END_DATE>FI.CREATE_DATE AND HT.BILLING_FILIAL_ID=FI.BILLING_FILIAL_ID 
       
        JOIN    -----вид места контакта------
               (SELECT --+parallel (10) 
               BD1.NAME DEF, BD1.STDF_ID, BD1.BILLING_FILIAL_ID
               FROM PUB_DS.BIS_CMS_SITE_DEF BD1 
               WHERE BD1.NAME  IN  ( 'Контактный центр',/*'МегаФон Ритейл','Мультибрендовый салон','Мультибрендовый салон с обслуживанием','Фирменный салон',*/'Федеральные соц.сети'  )
               ) BD ON BD.STDF_ID=FI.STDF_STDF_ID AND BD.BILLING_FILIAL_ID=FI.BILLING_FILIAL_ID
        LEFT JOIN   -----канал контакта------
               (SELECT --+parallel (10) 
               BC1.CHNL_ID,  BC1.NAME CHNL , BC1.BILLING_FILIAL_ID
               FROM PUB_DS.BIS_CMS_COMM_CHANNEL BC1
               WHERE  1=1 
               ) BC ON BC.CHNL_ID=FI.CHNL_CHNL_ID AND BC.BILLING_FILIAL_ID=FI.BILLING_FILIAL_ID
               
             ---------------операторы--------------
       LEFT JOIN   (SELECT --+parallel (10) 
                HO.OPER_ID, HO.BILLING_FILIAL_ID, HO.START_DATE, HO.END_DATE, HO.LOGIN, HO.FULL_NAME as FULL_OPER_NAME
               FROM PUB_DS.H_OPERATOR HO
               ) HO1 ON HO1.OPER_ID=FI.FIRST_OPER_ID AND HO1.BILLING_FILIAL_ID=FI.BILLING_FILIAL_ID 
               AND HO1.START_DATE<=FI.CREATE_DATE AND HO1.END_DATE>=FI.CREATE_DATE   
       LEFT JOIN    ----автореги----
            (SELECT --+parallel (10) 
            distinct  DA.INQR_INQR_ID, DA.THEME_NAME as AUTOREG
             FROM CMUN_VLG.DMIKS_AUTOREGISTRATION DA
             WHERE DA.INQR_DATE >=  '01.01.2023' 
             ) AU ON AU.INQR_INQR_ID=FI.INQR_ID
         
      WHERE
         FI.BILLING_FILIAL_ID  IN  ( 10,9)
         AND FI.FILIAL_ID  IN  ( 1,2,3,4,5,6,7,8)
         AND FI.CLNT_NAME  NOT LIKE  'Тест%'
         AND FI.CREATE_DATE  >=  '01.01.2023'
         AND HT.FULL_NAME  NOT LIKE '%Переоформление договора%ОРЛС%(дочернее)%'
         AND AU.AUTOREG is null
         AND FI.Comp_Os_Id = 150  -- 150 МФ, 289 Yota
         AND (CASE WHEN BD.DEF in ('Контактный центр', 'Федеральные соц.сети') and (HT.FULL_NAME like ('%Действие%Абонентское обслуживание%Заключение договора%') 
         OR HT.FULL_NAME like '%Переоформление договора%ОРЛС%Переоформление договора абонента на GF (один номер)%') 
         then 'убрать' else 'оставить' END) in ('оставить')
      )
      group by DATE_MONTH, INQR_ID, "Канал"
      ) a
    ) CHA ON CHA.INQR_ID = tariff.INQR_ID

    --РЕПРАЙС 2024 СЕНТЯБРЬ
    --РЕПРАЙС БИ 2024 (с архивными тарифами, с бесплатной опцией БИ)
    LEFT JOIN ( 
    SELECT distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_BI_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
          WHEN NVL(DISPATCH_DATE_BI_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep9_2024
    FROM rep_core_b2c.mta_reprice_unlim_data_id_503694_4q2024_freeze
    union
    --РЕПРАЙС ОТМЕНА СКИДКИ 2024 (абоны с бессрочной скидкой - либо полностью отменяем, либо уменьшаем)
    SELECT distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_skidka_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
          WHEN NVL(DISPATCH_DATE_SKIDKA_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep9_2024
    FROM rep_core_b2c.mta_reprice_skidka_4q2024_freeze
    -- updated by a_gavr 07/10/2024 end
    ) rep9_2024
    on tariff.sk_subs_id = rep9_2024.sk_subs_id
    LEFT JOIN 
    (
      SELECT
      1 as FLAG_TEPLOKARTI,
      APP.RC_ID,
      CODE_CRM AS CODE_CRM_TEPLOKARTI,
      OPEN_TIME AS OPEN_TIME_TEPLOKARTI,
      CLOSE_TIME AS CLOSE_TIME_TEPLOKARTI,
      CASE
      WHEN WEIGHT < 15 THEN 'ВЕС МЕНЬШЕ 15'
      ELSE 'ВЕС БОЛЬШЕ 15'
      END AS MARK_TEPLOKARTI,
      ROUND((CAST(NVL(CLOSE_TIME, SYSDATE) AS DATE) - OPEN_TIME) * 24,2 ) AS DURATION_HOURS_TEPLOKARTI
      FROM fsm_stg.mgfrootcausea1 app
      LEFT JOIN fsm_stg.mgfrootcausem1 rc ON rc.rc_id = app.rc_id
    ) TEPL ON TEPL.CODE_CRM_TEPLOKARTI = tariff.INQR_ID
    LEFT JOIN 
    (
       SELECT subs_subs_id, 
         CASE WHEN NVL(PROFILE_YOUTUBE,0)>1000000000 THEN '1. Более 1GB'
          WHEN NVL(PROFILE_YOUTUBE,0)>100000000 THEN '2. 100MB - 1GB'
          WHEN NVL(PROFILE_YOUTUBE,0)>100 THEN '3. До 100MB'
          ELSE '4. Нет' 
        END AS cd_PROFILE_YOUTUBE
       FROM(
           SELECT /*+parallel(20)*/ subs_subs_id, (SUM(dl_volume)+SUM(ul_volume))/2 AS PROFILE_YOUTUBE
           FROM pub_ds.a_subs_app_data
           WHERE month_stamp IN ('01.04.2024', '01.05.2024') AND service_id LIKE '%youtube%'
           GROUP BY subs_subs_id
         )
    ) cd_y on cd_y.subs_subs_id = tariff.SUBS_SUBS_ID
    LEFT JOIN 
    (
       SELECT subs_subs_id, 
         CASE WHEN NVL(PROFILE_TELEGRAM,0)>1000000000 THEN '1. Более 1GB'
          WHEN NVL(PROFILE_TELEGRAM,0)>100000000 THEN '2. 100MB - 1GB'
          WHEN NVL(PROFILE_TELEGRAM,0)>100 THEN '3. До 100MB'
          ELSE '4. Нет' 
        END AS cd_PROFILE_TELEGRAM
       FROM(
           SELECT /*+parallel(20)*/ subs_subs_id, (SUM(dl_volume)+SUM(ul_volume))/2 AS PROFILE_TELEGRAM
           FROM pub_ds.a_subs_app_data
           WHERE month_stamp IN ('01.04.2024', '01.05.2024') AND service_id LIKE '%telegram%'
           GROUP BY subs_subs_id
         )
    ) cd_t on cd_t.subs_subs_id = tariff.SUBS_SUBS_ID
;
commit;


--КОПИРОВАНИЕ ДАННЫХ В ЦЕЛЕВУЮ ТАБЛИЦУ
DELETE FROM fin_ba.kh_proc_cr_inquiry_results
WHERE 1=1; 
INSERT INTO fin_ba.kh_proc_cr_inquiry_results
SELECT * FROM fin_ba.kh_proc_cr_inquiry_results_temp;
DROP TABLE fin_ba.kh_proc_cr_inquiry_results_temp;
COMMIT;