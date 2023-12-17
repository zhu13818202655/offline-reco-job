
-------------------------v5版,改变的表命名为v5,其余表名不变------------------------------------------------------------------------------
--------------------------------步骤1-----------------------------------
--drop table if exists xianzhi.bdasire_card_a_usr_v4;
CREATE TABLE xianzhi.bdasire_card_a_usr_v4 (user_id string, user_type string, prod_id string, msgst varchar(100) ) partitioned by(dt string) stored AS parquet;

---添加实验组、对照组1至相应分区,5天之后才能跑
INSERT overwrite TABLE xianzhi.bdasire_card_a_usr_v4 partition(dt='19000000')
SELECT user_id,
       user_type,
       prod_id,
       msgst
FROM xianzhi.bdasire_card_a_usr_v2
WHERE dt='19000000';


--从package拷贝表、results拷贝表中取数单独跑
INSERT overwrite TABLE xianzhi.bdasire_card_a_usr_v4 partition(dt='20220602')
SELECT a.user_id,
       a.user_type,
       a.prod_id,
       c.msgst from
  (SELECT user_id,user_type,item_id AS prod_id
   FROM xianzhi.bdasire_card_crowd_package_bak
   WHERE channel_id='PSMS_IRE'
     AND scene_id='scene_debit_card_ca'
     AND user_type IN ('exp','ctl')
     AND dt='20220531') a
LEFT JOIN
  (SELECT user_id,
          tel_number,
          content
   FROM xianzhi.bdasire_card_sms_reco_results_bak
   WHERE dt='20220531') b ON a.user_id=b.user_id
LEFT JOIN
  (SELECT mblno,
          failreason,
          msgst,
          msgcntnt
   FROM shdata.s35_edb_sms_outslog
   WHERE failreason='DELIVRD'
     AND msgst='0'
     AND dt='20220602'
     AND msgcntnt rlike '关注上海银行微信公众号，点击“我要办卡丨社保卡—借记卡预约办卡' ) c ON b.tel_number=c.mblno
WHERE c.msgst='0';


---添加空白对照组至19000000分区,使用复制的package表
INSERT overwrite TABLE xianzhi.bdasire_card_a_usr_v4 partition(dt='19000000') WITH temp1 as
  ( SELECT DISTINCT a.cust_id AS user_id
   FROM
     ( SELECT DISTINCT cust_id
      FROM apprbw.rbw_cstpro_ind_cre_crd
      WHERE dt='20220430'
        AND if_valid_hold_crd='1') AS a
   LEFT JOIN
     (SELECT cust_id,count(1),sum(CASE WHEN if_hold_card='1' THEN 1 ELSE 0 END)
      FROM apprbw.rbw_cstpro_ind_deb_crd
      WHERE dt='20220430'
      GROUP BY cust_id
      HAVING sum(CASE WHEN if_hold_card='1' THEN 1 ELSE 0 END)>0) AS b ON a.cust_id=b.cust_id
   LEFT JOIN
     (SELECT cust_id,1 AS pmbs_ind
      FROM appcha.cha_gj_pmbs_cust_act_label
      WHERE data_dt='20220430'
        AND pmbs_cust_ind='1'
      GROUP BY cust_id) AS c ON a.cust_id=c.cust_id
   WHERE b.cust_id IS NULL
     AND c.pmbs_ind=1 --手机银行过滤条件
 ),
       temp2 AS
  ( SELECT *
   FROM temp1 LEFT anti
   JOIN
     (SELECT DISTINCT user_id
      FROM xianzhi.bdasire_card_crowd_package_bak
      WHERE dt BETWEEN '20220515' AND '20220531' --需修改dt，因实验组和对照组1在扩充;缺5月15日的数据,需补充
        AND channel_id='PSMS_IRE'
      UNION ALL SELECT DISTINCT user_id
      FROM xianzhi.bdasire_card_a_usr
      WHERE dt='20220515' --缺失数据打补丁
 ) d ON temp1.user_id=d.user_id)
SELECT user_id,
       '0' AS user_type,
       'card9' AS prod_id,
       cast('1' AS varchar(100)) AS msgst
FROM temp2
ORDER BY rand() LIMIT 50000;

------——————————————————---步骤2-----------------------------
--微信银行，只需要往下添加分区即可
--drop table if exists xianzhi.bdasire_card_a_act;
CREATE TABLE xianzhi.bdasire_card_a_act (cust_id string, cardtype string, pweixin_postCardinfo_cfxq bigint, pweixin_postCardinfo_cfsq bigint, pweixin_postCardinfo_cfsx bigint ) partitioned by(dt string) stored AS parquet;

--往下添加分区
INSERT overwrite TABLE xianzhi.bdasire_card_a_act partition(dt='20220616') WITH TEMP as
  ( SELECT LOCALTIME,useridentifier,custom_data,event_identifier, regexp_extract(custom_data,'("CardType":")(.*?)(",")',2) CardType,dt
   FROM shdata.s70_custom_data_json
   WHERE dt='20220616' --改为分区时间
     AND event_identifier IN ('pweixin_postCardinfo_cfxq','pweixin_postCardinfo_cfsq','pweixin_postCardinfo_cfsx')
     AND useridentifier IS NOT NULL
     AND useridentifier<> '' --  and regexp_extract(custom_data,'("CardType":")(.*?)(",")',2)='CCGM'
 ),
                                                                                     temp2 as
  ( SELECT d.cifno cust_id,a.CardType,a.dt,a.event_identifier
   FROM TEMP a
   LEFT JOIN shdata.s21_ecusrdevice b ON a.useridentifier=b.deviceno
   LEFT JOIN shdata.s21_ecusr_h c ON b.userseq=c.userseq
   AND a.dt=c.dt
   LEFT JOIN shdata.s21_ecextcifno_h d ON c.cifseq=d.cifseq
   AND a.dt=d.dt
   WHERE b.deviceno <> ""
     AND d.cifnotype='C')
SELECT cust_id,
       CardType,
       sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfxq' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfxq,
       sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfsq' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfsq,
       sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfsx' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfsx
FROM temp2
GROUP BY cust_id,
         CardType,
         dt;
--show partitions xianzhi.bdasire_card_a_act;


--------------------------步骤4-------------------------------
DROP TABLE IF EXISTS xianzhi.bdasire_card_a_table_v5;
CREATE TABLE IF NOT EXISTS xianzhi.bdasire_card_a_table_v5 (user_id string, user_type string, prod_id string, dt string, dt_sub1 string, fenhang string, pre_tot_asset_n_zhixiao_month_avg_bal decimal(38,6) ,tot_asset_n_zhixiao_month_avg_bal decimal(38,6) ,pre_aum_tm decimal(38,6) ,aum_tm decimal(38,6) ,pre_dpst_tm decimal(38,6) ,dpst_tm decimal(38,6) ) partitioned by(dt_cur string) stored AS parquet;


--特征临时表创建
--drop table if exists xianzhi.bdasire_card_a_table_temp_v4;
CREATE TABLE IF NOT EXISTS xianzhi.bdasire_card_a_table_temp_v4 (user_id string, user_type string, prod_id string, dt string, dt_sub1 string ) partitioned by(dt_cur string) stored AS parquet;
INSERT overwrite TABLE xianzhi.bdasire_card_a_table_temp_v4 partition(dt_cur='20220615') --填统计的当前日期
SELECT user_id,
       user_type,
       prod_id,
       dt,
       from_unixtime(unix_timestamp(to_date(date_sub( from_unixtime(unix_timestamp(dt,'yyyyMMdd'),'yyyy-MM-dd'),1)),'yyyy-MM-dd'),'yyyyMMdd') dt_sub1
FROM
  (SELECT user_id,
          user_type,
          prod_id,
          dt ,
          row_number() over (partition BY user_id
                             ORDER BY dt DESC) num
   FROM xianzhi.bdasire_card_a_usr_v4
   WHERE dt BETWEEN '20220517' AND '20220602' --填短信发送的时间范围
     AND msgst IS NOT NULL
     OR dt='19000000') t1 --补充空白对照组
 WHERE t1.num=1;


--特征表
INSERT overwrite TABLE xianzhi.bdasire_card_a_table_v5 partition(dt_cur='20220615') --填当前统计日期
SELECT a.user_id,
       a.user_type,
       a.prod_id,
       a.dt,
       a.dt_sub1 ,
       (CASE
            WHEN c.fenhang rlike '上海银行*' THEN regexp_extract(c.fenhang,'(上海银行)(.*?)()',2)
            WHEN c.fenhang rlike '.*(分行级)' THEN regexp_extract(c.fenhang,'()(.*?)(.分行级.)',2)
            ELSE c.fenhang
        END) fenhang ,
       (CASE
            WHEN a.user_type='0' THEN d.tot_asset_n_zhixiao_month_avg_bal
            ELSE b.tot_asset_n_zhixiao_month_avg_bal
        END) pre_tot_asset_n_zhixiao_month_avg_bal ,
       c.tot_asset_n_zhixiao_month_avg_bal ,
       (CASE
            WHEN a.user_type='0' THEN e3.aum_tm
            ELSE e1.aum_tm
        END) pre_aum_tm ,
       e2.aum_tm ,
       (CASE
            WHEN a.user_type='0' THEN e3.dpst_tm
            ELSE e1.dpst_tm
        END) pre_dpst_tm ,
       e2.dpst_tm
FROM
  (SELECT *
   FROM xianzhi.bdasire_card_a_table_temp_v4
   WHERE dt_cur='20220615') a --填当前日期
LEFT JOIN apprbw.rbw_lsyb_ls_crd b ON a.user_id=b.cust_id
AND a.dt_sub1=b.dt
LEFT JOIN apprbw.rbw_lsyb_ls_crd c ON a.user_id=c.cust_id
AND a.dt_cur=c.dt
LEFT JOIN
  (SELECT *
   FROM apprbw.rbw_lsyb_ls_crd
   WHERE dt='20220517') d --空白组取最早营销日期5月17日的aum
 ON a.user_id=d.cust_id
LEFT JOIN appmlm.mlm_rtl_asset_bal e1 ON a.user_id=e1.cust_id
AND a.dt_sub1=e1.dt
LEFT JOIN appmlm.mlm_rtl_asset_bal e2 ON a.user_id=e2.cust_id
AND a.dt_cur=e2.dt
LEFT JOIN
  (SELECT *
   FROM appmlm.mlm_rtl_asset_bal
   WHERE dt='20220517') e3 --空白组取最早营销日期5月17日的aum
 ON a.user_id=e3.cust_id;
-----------结果输出-----------
------按分行、组别的集成表-----
show partitions xianzhi.bdasire_card_a_result_v4;
drop table if exists xianzhi.bdasire_card_a_result_v5;
create table if not exists xianzhi.bdasire_card_a_result_v5
  (fenhang string comment '分行',
  user_type string comment '组名',
  cnt bigint comment '组人数',
  pweixin_postCardinfo_cfxq_uct bigint comment '点击微信银行预约办卡列表页人数',
  pweixin_postCardinfo_cfxq_ct bigint comment '点击微信银行预约办卡列表页次数',
  pweixin_postCardinfo_cfxq_uct_per decimal(38,6) comment '点击微信银行预约办卡列表页人数占比',
  pweixin_postCardinfo_cfxq_ct_per decimal(38,6) comment '点击微信银行预约办卡列表页次数/总人数',
  pweixin_postCardinfo_cfsq_uct bigint comment '点击借记卡详情页人数',
  pweixin_postCardinfo_cfsq_ct bigint comment '点击借记卡详情页次数',
  pweixin_postCardinfo_cfsq_uct_per decimal(38,6) comment '点击借记卡详情页人占比数',
  pweixin_postCardinfo_cfsq_ct_per decimal(38,6) comment '点击借记卡详情页次数/总人数',
  pweixin_postCardinfo_cfsx_uct bigint comment '点击预约办卡信息录入页人数',
  pweixin_postCardinfo_cfsx_ct bigint comment '点击预约办卡信息录入页次数',
  pweixin_postCardinfo_cfsx_uct_per decimal(38,6) comment '点击预约办卡信息录入页人数占比',
  pweixin_postCardinfo_cfsx_ct_per decimal(38,6) comment '点击预约办卡信息录入页次数/总人数',
  app_entrance_uct bigint comment '点击手机银行“借记卡申请”图标人数',
  app_entrance_ct bigint comment '点击手机银行“借记卡申请”图标次数',
  app_entrance_uct_per decimal(38,6) comment '点击手机银行“借记卡申请”图标人数占比',
  app_entrance_ct_per decimal(38,6) comment '点击手机银行“借记卡申请”图标次数/总人数',
  app_choose_uct bigint comment '点击产品展示页人数',
  app_choose_ct bigint comment '点击产品展示页次数',
  app_choose_uct_per decimal(38,6) comment '点击产品展示页人数占比',
  app_choose_ct_per decimal(38,6) comment '点击产品展示页次数/总人数',
  app_apply_uct bigint comment '点击卡片详情页“立即申请”人数',
  app_apply_ct bigint comment '点击卡片详情页“立即申请”次数',
  app_apply_uct_per decimal(38,6) comment '点击卡片详情页“立即申请”人数占比',
  app_apply_ct_per decimal(38,6) comment '点击卡片详情页“立即申请”次数/总人数',
  app_write_uct bigint comment '填写信息页填写完人数',
  app_write_ct bigint comment '填写信息页填写完次数',
  app_write_uct_per decimal(38,6) comment '填写信息页填写完人数占比',
  app_write_ct_per decimal(38,6) comment '填写信息页填写完次数/总人数',
  app7_act1_uct bigint comment '手机银行7.0浏览借记卡曝光人数',
  app7_act1_ct bigint comment '手机银行7.0浏览借记卡曝光次数',
  app7_act1_uct_per decimal(38,6) comment '手机银行7.0浏览借记卡曝光人数占比',
  app7_act1_ct_per decimal(38,6) comment '手机银行7.0浏览借记卡曝次数/总人数',
  app7_act2_uct bigint comment '手机银行7.0借记卡卡面点击人数',
  app7_act2_ct bigint comment '手机银行7.0借记卡卡面点击次数',
  app7_act2_uct_per decimal(38,6) comment '手机银行7.0借记卡卡面点击人数占比',
  app7_act2_ct_per decimal(38,6) comment '手机银行7.0借记卡卡面点击次数/总人数',
  app7_act32_uct bigint comment '手机银行7.0点击卡片确认页的立即申请按钮人数',
  app7_act32_ct bigint comment '手机银行7.0点击卡片确认页的立即申请按钮次数',
  app7_act32_uct_per decimal(38,6) comment '手机银行7.0点击卡片确认页的立即申请按钮人数占比',
  app7_act32_ct_per decimal(38,6) comment '手机银行7.0点击卡片确认页的立即申请按钮次数/总人数',
  app7_act4_uct bigint comment '手机银行7.0点击申请验证的下一步按钮人数',
  app7_act4_ct bigint comment '手机银行7.0点击申请验证的下一步按钮次数',
  app7_act4_uct_per decimal(38,6) comment '手机银行7.0点击申请验证的下一步按钮人数占比',
  app7_act4_ct_per decimal(38,6) comment '手机银行7.0点击申请验证的下一步按钮次数/总人数',
  app7_act52_uct bigint comment '手机银行7.0卡片申请成功页曝光人数',
  app7_act52_ct bigint comment '手机银行7.0卡片申请成功页曝光次数',
  app7_act52_uct_per decimal(38,6) comment '手机银行7.0卡片申请成功页曝光人数占比',
  app7_act52_ct_per decimal(38,6) comment '手机银行7.0卡片申请成功页曝光次数/总人数',
  apply_ind_cnt bigint comment '申请人数',
  apply_ind_per decimal(38,6) comment '申请人数/总人数',
  pre_tot_asset_n_zhixiao_month_avg_bal decimal(38,6) comment '短信发送前一天月日均aum_不含直销总计',
  tot_asset_n_zhixiao_month_avg_bal decimal(38,6) comment '当天月日均aum_不含直销总计',
  pre_aum_tm decimal(38,6) comment '短信发送前一天aum余额总计',
  aum_tm decimal(38,6) comment '当天aum余额总计',
  pre_dpst_tm decimal(38,6) comment '短信发送前一天活期存款余额总计',
  dpst_tm decimal(38,6) comment '当天活期存款余额总计'
  ) partitioned by(dt_cur string)
  stored as parquet;
--1.输出按组别date_sum分区的表
insert overwrite table xianzhi.bdasire_card_a_result_v5 partition(dt_cur='20220615_sum')  ---填date_sum或者date，视需求而定
       with temp as(
         select a.*,b.pweixin_postCardinfo_cfxq,b.pweixin_postCardinfo_cfsq,b.pweixin_postCardinfo_cfsx
                ,c.app_entrance_cnt,d.app_choose_cnt,e1.app_apply_cnt,f.app_write_cnt
                ,temp7_1.app7_act1,temp7_2.app7_act2,temp7_3.app7_act3,temp7_32.app7_act32
                ,temp7_4.app7_act4,temp7_5.app7_act5,temp7_52.app7_act52,g.apply_ind
           from 
             (select * from xianzhi.bdasire_card_a_table_v5 where dt_cur='20220615') a  --修改日期范围
             left join
             (select cust_id,sum(pweixin_postCardinfo_cfxq) pweixin_postCardinfo_cfxq
                     ,sum(pweixin_postCardinfo_cfsq) pweixin_postCardinfo_cfsq,sum(pweixin_postCardinfo_cfsx) pweixin_postCardinfo_cfsx
                from xianzhi.bdasire_card_a_act 
               where dt between '20220517' and '20220615'  --修改行为数据日期范围
                 and cardtype in ('CCGG','CCGH','CCGI','CCGJ','CCGM','CCGN','CCKO','CCKP','CCLN')
               group by cust_id
             ) b
                 on a.user_id=b.cust_id
             left join
             (select user_id,count(1) app_entrance_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='syzs_sy_23381_28544') temp_c 
               group by user_id) c
                 on a.user_id=c.user_id
             left join
             (select user_id,count(1) app_choose_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_sy_jjklb_jjklb') temp_d 
               group by user_id) d
                 on a.user_id=d.user_id
             left join
             (select user_id,count(1) app_apply_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_cpxqy_na_ljsq') temp_e 
               group by user_id) e1
                 on a.user_id=e1.user_id
             left join
             (select user_id,count(1) app_write_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_xsydkk_dmy_na_xyb') temp_f 
               group by user_id) f
                 on a.user_id=f.user_id
             left join
             (select cust_id,count(1) app7_act1 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_sy_na_na_pg_1'
             ) temp7_1_1 group by cust_id
             ) temp7_1
                 on a.user_id=temp7_1.cust_id
             left join
             (select cust_id,count(1) app7_act2 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id in
                      ('pmbs_jjksq_sy_qxfx_1_km_2','pmbs_jjksq_sy_qxfx_2_km_2'
                      ,'pmbs_jjksq_sy_qxfx_3_km_2','pmbs_jjksq_sy_qxfx_4_km_2'
                      ,'pmbs_jjksq_sy_qxfx_5_km_2','pmbs_jjksq_sy_qxfx_6_km_2'
                      ,'pmbs_jjksq_sy_qxfx_7_km_2','pmbs_jjksq_sy_qxfx_8_km_2'
                      ,'pmbs_jjksq_sy_qxfx_9_km_2','pmbs_jjksq_sy_qxfx_10_km_2'
                      ,'pmbs_jjksq_sy_qxfx_1_km_3','pmbs_jjksq_sy_qxfx_2_km_3'
                      ,'pmbs_jjksq_sy_qxfx_3_km_3','pmbs_jjksq_sy_qxfx_4_km_3'
                      ,'pmbs_jjksq_sy_qxfx_5_km_3','pmbs_jjksq_sy_qxfx_6_km_3'
                      ,'pmbs_jjksq_sy_qxfx_7_km_3','pmbs_jjksq_sy_qxfx_8_km_3'
                      ,'pmbs_jjksq_sy_qxfx_9_km_3','pmbs_jjksq_sy_qxfx_10_km_3')
             ) temp7_2_1 group by cust_id
             ) temp7_2
                 on a.user_id=temp7_2.cust_id
             left join
             (select cust_id,count(1) app7_act3 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_cpxqy_na_na_ljsqan_5'
             ) temp7_3_1 group by cust_id
             ) temp7_3
                 on a.user_id=temp7_3.cust_id
             left join
             (select cust_id,count(1) app7_act32 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_cpxqy_kpqr_na_ljsqan_6'
             ) temp7_3_2 group by cust_id
             ) temp7_32
                 on a.user_id=temp7_32.cust_id
             left join
             (select cust_id,count(1) app7_act4 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_sqyz_na_na_xyban_7'
             ) temp7_4_1 group by cust_id
             ) temp7_4
                 on a.user_id=temp7_4.cust_id
             left join
             (select cust_id,count(1) app7_act5 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_kpsqdzxx_na_na_tjan_17'
             ) temp7_5_1 group by cust_id
             ) temp7_5
                 on a.user_id=temp7_5.cust_id
             left join
             (select cust_id,count(1) app7_act52 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_kpsqcgy_na_na_pg_18'
             ) temp7_5_2 group by cust_id
             ) temp7_52
                 on a.user_id=temp7_52.cust_id
             left join
             (select cust_id,1 as apply_ind
                from apprbw.rbw_cstpro_ind_deb_crd
               where dt='20220615' and firsr_apply_dt>='20220517'  --统计日期、营销开始日期，需修改dt统计日期
               group by cust_id) g 
                 on a.user_id=g.cust_id
       )
select 'sum' as fenhang --填ifnull(fenhang,'') as fenhang或者'sum' as fenhang，视分区而定
       ,user_type,count(1) cnt,sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end) pweixin_postCardinfo_cfxq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfxq),0) pweixin_postCardinfo_cfxq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfxq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end) pweixin_postCardinfo_cfsq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsq),0) pweixin_postCardinfo_cfsq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end) pweixin_postCardinfo_cfsx_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsx),0) pweixin_postCardinfo_cfsx_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsx),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_ct_per
       ,sum(case when app_entrance_cnt>0 then 1 else 0 end) app_entrance_cnt
       ,nvl(sum(app_entrance_cnt),0) app_entrance_ct
       ,cast(sum(case when app_entrance_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_entrance_per
       ,cast(nvl(sum(app_entrance_cnt),0)/count(1) as decimal(38,6)) app_entrance_ct_per
       ,sum(case when app_choose_cnt>0 then 1 else 0 end) app_choose_cnt
       ,nvl(sum(app_choose_cnt),0) app_choose_ct
       ,cast(sum(case when app_choose_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_choose_per
       ,cast(nvl(sum(app_choose_cnt),0)/count(1) as decimal(38,6)) app_choose_ct_per
       ,sum(case when app_apply_cnt>0 then 1 else 0 end) app_apply_cnt
       ,nvl(sum(app_apply_cnt),0) app_apply_ct
       ,cast(sum(case when app_apply_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_apply_per
       ,cast(nvl(sum(app_apply_cnt),0)/count(1) as decimal(38,6)) app_apply_ct_per
       ,sum(case when app_write_cnt>0 then 1 else 0 end) app_write_cnt
       ,nvl(sum(app_write_cnt),0) app_write_ct
       ,cast(sum(case when app_write_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_write_per
       ,cast(nvl(sum(app_write_cnt),0)/count(1) as decimal(38,6)) app_write_ct_per 
       ,sum(case when app7_act1>0 then 1 else 0 end) app7_act1_uct
       ,nvl(sum(app7_act1),0) app7_act1_ct
       ,cast(sum(case when app7_act1>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act1_per
       ,cast(nvl(sum(app7_act1),0)/count(1) as decimal(38,6)) app7_act1_ct_per
       ,sum(case when app7_act2>0 then 1 else 0 end) app7_act2_uct
       ,nvl(sum(app7_act2),0) app7_act2_ct
       ,cast(sum(case when app7_act2>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act2_per
       ,cast(nvl(sum(app7_act2),0)/count(1) as decimal(38,6)) app7_act2_ct_per
       ,sum(case when app7_act32>0 then 1 else 0 end) app7_act32_uct
       ,nvl(sum(app7_act32),0) app7_act32_ct
       ,cast(sum(case when app7_act32>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act32_per
       ,cast(nvl(sum(app7_act32),0)/count(1) as decimal(38,6)) app7_act32_ct_per
       ,sum(case when app7_act4>0 then 1 else 0 end) app7_act4_uct
       ,nvl(sum(app7_act4),0) app7_act4_ct
       ,cast(sum(case when app7_act4>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act4_per
       ,cast(nvl(sum(app7_act4),0)/count(1) as decimal(38,6)) app7_act4_ct_per
       ,sum(case when app7_act52>0 then 1 else 0 end) app7_act52_uct
       ,nvl(sum(app7_act52),0) app7_act52_ct
       ,cast(sum(case when app7_act52>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act52_per
       ,cast(nvl(sum(app7_act52),0)/count(1) as decimal(38,6)) app7_act52_ct_per
       ,nvl(sum(apply_ind),0) apply_ind_cnt
       ,cast(nvl(sum(apply_ind),0)/count(1) as decimal(38,6)) apply_ind_per
       ,sum(pre_tot_asset_n_zhixiao_month_avg_bal),sum(tot_asset_n_zhixiao_month_avg_bal)
       ,sum(pre_aum_tm),sum(aum_tm)
       ,sum(pre_dpst_tm),sum(dpst_tm)
  from temp
 group by fenhang,user_type
 order by fenhang,user_type;
--2.按分行、组别date分区的表-------------
insert overwrite table xianzhi.bdasire_card_a_result_v5 partition(dt_cur='20220615')  ---填date_sum或者date，视需求而定
       with temp as(
         select a.*,b.pweixin_postCardinfo_cfxq,b.pweixin_postCardinfo_cfsq,b.pweixin_postCardinfo_cfsx
                ,c.app_entrance_cnt,d.app_choose_cnt,e1.app_apply_cnt,f.app_write_cnt
                ,temp7_1.app7_act1,temp7_2.app7_act2,temp7_3.app7_act3,temp7_32.app7_act32
                ,temp7_4.app7_act4,temp7_5.app7_act5,temp7_52.app7_act52,g.apply_ind
           from 
             (select * from xianzhi.bdasire_card_a_table_v5 where dt_cur='20220615') a  --修改日期范围
             left join
             (select cust_id,sum(pweixin_postCardinfo_cfxq) pweixin_postCardinfo_cfxq
                     ,sum(pweixin_postCardinfo_cfsq) pweixin_postCardinfo_cfsq,sum(pweixin_postCardinfo_cfsx) pweixin_postCardinfo_cfsx
                from xianzhi.bdasire_card_a_act 
               where dt between '20220517' and '20220615'  --修改行为数据日期范围
                 and cardtype in ('CCGG','CCGH','CCGI','CCGJ','CCGM','CCGN','CCKO','CCKP','CCLN')
               group by cust_id
             ) b
                 on a.user_id=b.cust_id
             left join
             (select user_id,count(1) app_entrance_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='syzs_sy_23381_28544') temp_c 
               group by user_id) c
                 on a.user_id=c.user_id
             left join
             (select user_id,count(1) app_choose_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_sy_jjklb_jjklb') temp_d 
               group by user_id) d
                 on a.user_id=d.user_id
             left join
             (select user_id,count(1) app_apply_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_cpxqy_na_ljsq') temp_e 
               group by user_id) e1
                 on a.user_id=e1.user_id
             left join
             (select user_id,count(1) app_write_cnt from (
               select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                where dt between '20220517' and '20220615' --修改行为数据日期范围
                  and event_identifier='jjksq_xsydkk_dmy_na_xyb') temp_f 
               group by user_id) f
                 on a.user_id=f.user_id
             left join
             (select cust_id,count(1) app7_act1 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_sy_na_na_pg_1'
             ) temp7_1_1 group by cust_id
             ) temp7_1
                 on a.user_id=temp7_1.cust_id
             left join
             (select cust_id,count(1) app7_act2 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id in
                      ('pmbs_jjksq_sy_qxfx_1_km_2','pmbs_jjksq_sy_qxfx_2_km_2'
                      ,'pmbs_jjksq_sy_qxfx_3_km_2','pmbs_jjksq_sy_qxfx_4_km_2'
                      ,'pmbs_jjksq_sy_qxfx_5_km_2','pmbs_jjksq_sy_qxfx_6_km_2'
                      ,'pmbs_jjksq_sy_qxfx_7_km_2','pmbs_jjksq_sy_qxfx_8_km_2'
                      ,'pmbs_jjksq_sy_qxfx_9_km_2','pmbs_jjksq_sy_qxfx_10_km_2'
                      ,'pmbs_jjksq_sy_qxfx_1_km_3','pmbs_jjksq_sy_qxfx_2_km_3'
                      ,'pmbs_jjksq_sy_qxfx_3_km_3','pmbs_jjksq_sy_qxfx_4_km_3'
                      ,'pmbs_jjksq_sy_qxfx_5_km_3','pmbs_jjksq_sy_qxfx_6_km_3'
                      ,'pmbs_jjksq_sy_qxfx_7_km_3','pmbs_jjksq_sy_qxfx_8_km_3'
                      ,'pmbs_jjksq_sy_qxfx_9_km_3','pmbs_jjksq_sy_qxfx_10_km_3')
             ) temp7_2_1 group by cust_id
             ) temp7_2
                 on a.user_id=temp7_2.cust_id
             left join
             (select cust_id,count(1) app7_act3 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_cpxqy_na_na_ljsqan_5'
             ) temp7_3_1 group by cust_id
             ) temp7_3
                 on a.user_id=temp7_3.cust_id
             left join
             (select cust_id,count(1) app7_act32 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_cpxqy_kpqr_na_ljsqan_6'
             ) temp7_3_2 group by cust_id
             ) temp7_32
                 on a.user_id=temp7_32.cust_id
             left join
             (select cust_id,count(1) app7_act4 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_sqyz_na_na_xyban_7'
             ) temp7_4_1 group by cust_id
             ) temp7_4
                 on a.user_id=temp7_4.cust_id
             left join
             (select cust_id,count(1) app7_act5 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_kpsqdzxx_na_na_tjan_17'
             ) temp7_5_1 group by cust_id
             ) temp7_5
                 on a.user_id=temp7_5.cust_id
             left join
             (select cust_id,count(1) app7_act52 from (
               select cust_id from appcha.p_bh_elec_act_evt
                where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                  and channel_type='PMBS' and pt_sys='S38'
                  and act_id='pmbs_jjksq_kpsqcgy_na_na_pg_18'
             ) temp7_5_2 group by cust_id
             ) temp7_52
                 on a.user_id=temp7_52.cust_id
             left join
             (select cust_id,1 as apply_ind
                from apprbw.rbw_cstpro_ind_deb_crd
               where dt='20220615' and firsr_apply_dt>='20220517'  --统计日期、营销开始日期，需修改dt统计日期
               group by cust_id) g 
                 on a.user_id=g.cust_id
       )
select ifnull(fenhang,'') as fenhang --填ifnull(fenhang,'') as fenhang或者'sum' as fenhang，视分区而定
       ,user_type,count(1) cnt,sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end) pweixin_postCardinfo_cfxq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfxq),0) pweixin_postCardinfo_cfxq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfxq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end) pweixin_postCardinfo_cfsq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsq),0) pweixin_postCardinfo_cfsq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end) pweixin_postCardinfo_cfsx_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsx),0) pweixin_postCardinfo_cfsx_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsx),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_ct_per
       ,sum(case when app_entrance_cnt>0 then 1 else 0 end) app_entrance_cnt
       ,nvl(sum(app_entrance_cnt),0) app_entrance_ct
       ,cast(sum(case when app_entrance_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_entrance_per
       ,cast(nvl(sum(app_entrance_cnt),0)/count(1) as decimal(38,6)) app_entrance_ct_per
       ,sum(case when app_choose_cnt>0 then 1 else 0 end) app_choose_cnt
       ,nvl(sum(app_choose_cnt),0) app_choose_ct
       ,cast(sum(case when app_choose_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_choose_per
       ,cast(nvl(sum(app_choose_cnt),0)/count(1) as decimal(38,6)) app_choose_ct_per
       ,sum(case when app_apply_cnt>0 then 1 else 0 end) app_apply_cnt
       ,nvl(sum(app_apply_cnt),0) app_apply_ct
       ,cast(sum(case when app_apply_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_apply_per
       ,cast(nvl(sum(app_apply_cnt),0)/count(1) as decimal(38,6)) app_apply_ct_per
       ,sum(case when app_write_cnt>0 then 1 else 0 end) app_write_cnt
       ,nvl(sum(app_write_cnt),0) app_write_ct
       ,cast(sum(case when app_write_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_write_per
       ,cast(nvl(sum(app_write_cnt),0)/count(1) as decimal(38,6)) app_write_ct_per 
       ,sum(case when app7_act1>0 then 1 else 0 end) app7_act1_uct
       ,nvl(sum(app7_act1),0) app7_act1_ct
       ,cast(sum(case when app7_act1>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act1_per
       ,cast(nvl(sum(app7_act1),0)/count(1) as decimal(38,6)) app7_act1_ct_per
       ,sum(case when app7_act2>0 then 1 else 0 end) app7_act2_uct
       ,nvl(sum(app7_act2),0) app7_act2_ct
       ,cast(sum(case when app7_act2>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act2_per
       ,cast(nvl(sum(app7_act2),0)/count(1) as decimal(38,6)) app7_act2_ct_per
       ,sum(case when app7_act32>0 then 1 else 0 end) app7_act32_uct
       ,nvl(sum(app7_act32),0) app7_act32_ct
       ,cast(sum(case when app7_act32>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act32_per
       ,cast(nvl(sum(app7_act32),0)/count(1) as decimal(38,6)) app7_act32_ct_per
       ,sum(case when app7_act4>0 then 1 else 0 end) app7_act4_uct
       ,nvl(sum(app7_act4),0) app7_act4_ct
       ,cast(sum(case when app7_act4>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act4_per
       ,cast(nvl(sum(app7_act4),0)/count(1) as decimal(38,6)) app7_act4_ct_per
       ,sum(case when app7_act52>0 then 1 else 0 end) app7_act52_uct
       ,nvl(sum(app7_act52),0) app7_act52_ct
       ,cast(sum(case when app7_act52>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act52_per
       ,cast(nvl(sum(app7_act52),0)/count(1) as decimal(38,6)) app7_act52_ct_per
       ,nvl(sum(apply_ind),0) apply_ind_cnt
       ,cast(nvl(sum(apply_ind),0)/count(1) as decimal(38,6)) apply_ind_per
       ,sum(pre_tot_asset_n_zhixiao_month_avg_bal),sum(tot_asset_n_zhixiao_month_avg_bal)
       ,sum(pre_aum_tm),sum(aum_tm)
       ,sum(pre_dpst_tm),sum(dpst_tm)
  from temp
 group by fenhang,user_type
 order by fenhang,user_type;
--3.目标人群全体的输出表-------------
insert overwrite table xianzhi.bdasire_card_a_result_v5 partition(dt_cur='20220615_all')  ---填date_sum或者date，视需求而定
       with temp1 as(
         select distinct a.cust_id as user_id from (
           select distinct cust_id
             from apprbw.rbw_cstpro_ind_cre_crd
            where dt='20220430'
              and if_valid_hold_crd='1') as a
                                                   left join
                                                   (select cust_id,count(1),sum(case when if_hold_card='1' then 1 else 0 end)
                                                      from apprbw.rbw_cstpro_ind_deb_crd
                                                     where dt='20220430'
                                                     group by cust_id
                                                    having sum(case when if_hold_card='1' then 1 else 0 end)>0) as b
                                                       on a.cust_id=b.cust_id
                                                   left join
                                                   (select cust_id,1 as pmbs_ind
                                                      from appcha.cha_gj_pmbs_cust_act_label
                                                     where data_dt='20220430'
                                                       and pmbs_cust_ind='1'
                                                     group by cust_id) as c on
                                                     a.cust_id=c.cust_id
          where b.cust_id is null
            and c.pmbs_ind=1 --手机银行过滤条件   --全体用户表
       ),temp2 as(
         select a.*,c.fenhang
                ,b.tot_asset_n_zhixiao_month_avg_bal pre_tot_asset_n_zhixiao_month_avg_bal
                ,c.tot_asset_n_zhixiao_month_avg_bal
                ,e1.aum_tm pre_aum_tm,e1.dpst_tm pre_dpst_tm
                ,e2.aum_tm,e2.dpst_tm
           from temp1 a
                left join
                (select * from apprbw.rbw_lsyb_ls_crd where dt='20220517') b  --取最早营销日期的aum5月17日  
                    on a.user_id=b.cust_id
                left join
                (select * from apprbw.rbw_lsyb_ls_crd where dt='20220615') c --修改为当前日期
                    on a.user_id=c.cust_id
                left join
                (select * from appmlm.mlm_rtl_asset_bal where dt='20220517') e1  --取最早营销日期的aum5月17日  
                    on a.user_id=e1.cust_id
                left join
                (select * from appmlm.mlm_rtl_asset_bal where dt='20220615') e2 --修改为当前日期
                    on a.user_id=e2.cust_id
       ),
       temp as(
         select a.*,b.pweixin_postCardinfo_cfxq,b.pweixin_postCardinfo_cfsq,b.pweixin_postCardinfo_cfsx
                ,c.app_entrance_cnt,d.app_choose_cnt,e1.app_apply_cnt,f.app_write_cnt
                ,temp7_1.app7_act1,temp7_2.app7_act2,temp7_3.app7_act3,temp7_32.app7_act32
                ,temp7_4.app7_act4,temp7_5.app7_act5,temp7_52.app7_act52,g.apply_ind
           from temp2 a
                left join 
                (select cust_id,sum(pweixin_postCardinfo_cfxq) pweixin_postCardinfo_cfxq
                        ,sum(pweixin_postCardinfo_cfsq) pweixin_postCardinfo_cfsq,sum(pweixin_postCardinfo_cfsx) pweixin_postCardinfo_cfsx
                   from xianzhi.bdasire_card_a_act 
                  where dt between '20220517' and '20220615'  --修改日期范围
                    and cardtype in ('CCGG','CCGH','CCGI','CCGJ','CCGM','CCGN','CCKO','CCKP','CCLN')
                  group by cust_id
                ) b
                    on a.user_id=b.cust_id
                left join
                (select user_id,count(1) app_entrance_cnt from (
                  select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                   where dt between '20220517' and '20220615' --修改日期范围
                     and event_identifier='syzs_sy_23381_28544') temp_c 
                  group by user_id) c
                    on a.user_id=c.user_id
                left join
                (select user_id,count(1) app_choose_cnt from (
                  select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                   where dt between '20220517' and '20220615' --修改日期范围
                     and event_identifier='jjksq_sy_jjklb_jjklb') temp_d 
                  group by user_id) d
                    on a.user_id=d.user_id
                left join
                (select user_id,count(1) app_apply_cnt from (
                  select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                   where dt between '20220517' and '20220615' --修改日期范围
                     and event_identifier='jjksq_cpxqy_na_ljsq') temp_e 
                  group by user_id) e1
                    on a.user_id=e1.user_id
                left join
                (select user_id,count(1) app_write_cnt from (
                  select split_part(useridentifier,'_',2) user_id from shdata.s70_event
                   where dt between '20220517' and '20220615' --修改日期范围
                     and event_identifier='jjksq_xsydkk_dmy_na_xyb') temp_f 
                  group by user_id) f
                    on a.user_id=f.user_id
                left join
                (select cust_id,count(1) app7_act1 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_sy_na_na_pg_1'
                ) temp7_1_1 group by cust_id
                ) temp7_1
                    on a.user_id=temp7_1.cust_id
                left join
                (select cust_id,count(1) app7_act2 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id in
                         ('pmbs_jjksq_sy_qxfx_1_km_2','pmbs_jjksq_sy_qxfx_2_km_2'
                         ,'pmbs_jjksq_sy_qxfx_3_km_2','pmbs_jjksq_sy_qxfx_4_km_2'
                         ,'pmbs_jjksq_sy_qxfx_5_km_2','pmbs_jjksq_sy_qxfx_6_km_2'
                         ,'pmbs_jjksq_sy_qxfx_7_km_2','pmbs_jjksq_sy_qxfx_8_km_2'
                         ,'pmbs_jjksq_sy_qxfx_9_km_2','pmbs_jjksq_sy_qxfx_10_km_2'
                         ,'pmbs_jjksq_sy_qxfx_1_km_3','pmbs_jjksq_sy_qxfx_2_km_3'
                         ,'pmbs_jjksq_sy_qxfx_3_km_3','pmbs_jjksq_sy_qxfx_4_km_3'
                         ,'pmbs_jjksq_sy_qxfx_5_km_3','pmbs_jjksq_sy_qxfx_6_km_3'
                         ,'pmbs_jjksq_sy_qxfx_7_km_3','pmbs_jjksq_sy_qxfx_8_km_3'
                         ,'pmbs_jjksq_sy_qxfx_9_km_3','pmbs_jjksq_sy_qxfx_10_km_3')
                ) temp7_2_1 group by cust_id
                ) temp7_2
                    on a.user_id=temp7_2.cust_id
                left join
                (select cust_id,count(1) app7_act3 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_cpxqy_na_na_ljsqan_5'
                ) temp7_3_1 group by cust_id
                ) temp7_3
                    on a.user_id=temp7_3.cust_id
                left join
                (select cust_id,count(1) app7_act32 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_cpxqy_kpqr_na_ljsqan_6'
                ) temp7_3_2 group by cust_id
                ) temp7_32
                    on a.user_id=temp7_32.cust_id
                left join
                (select cust_id,count(1) app7_act4 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_sqyz_na_na_xyban_7'
                ) temp7_4_1 group by cust_id
                ) temp7_4
                    on a.user_id=temp7_4.cust_id
                left join
                (select cust_id,count(1) app7_act5 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_kpsqdzxx_na_na_tjan_17'
                ) temp7_5_1 group by cust_id
                ) temp7_5
                    on a.user_id=temp7_5.cust_id
                left join
                (select cust_id,count(1) app7_act52 from (
                  select cust_id from appcha.p_bh_elec_act_evt
                   where pt_td between '2022-05-17' and '2022-06-15' --修改手机银行7.0行为数据日期范围
                     and channel_type='PMBS' and pt_sys='S38'
                     and act_id='pmbs_jjksq_kpsqcgy_na_na_pg_18'
                ) temp7_5_2 group by cust_id
                ) temp7_52
                    on a.user_id=temp7_52.cust_id
                left join
                (select cust_id,1 as apply_ind
                   from apprbw.rbw_cstpro_ind_deb_crd
                  where dt='20220615' and firsr_apply_dt>='20220517'  --填申请的日期范围
                  group by cust_id) g 
                    on a.user_id=g.cust_id
       )
select 'all' as fenhang,'all' as user_type,count(1) cnt
       ,sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end) pweixin_postCardinfo_cfxq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfxq),0) pweixin_postCardinfo_cfxq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfxq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfxq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfxq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end) pweixin_postCardinfo_cfsq_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsq),0) pweixin_postCardinfo_cfsq_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsq>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsq),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsq_ct_per
       ,sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end) pweixin_postCardinfo_cfsx_cnt
       ,nvl(sum(pweixin_postCardinfo_cfsx),0) pweixin_postCardinfo_cfsx_ct
       ,cast(sum(case when pweixin_postCardinfo_cfsx>0 then 1 else 0 end)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_per
       ,cast(nvl(sum(pweixin_postCardinfo_cfsx),0)/count(1) as decimal(38,6)) pweixin_postCardinfo_cfsx_ct_per
       ,sum(case when app_entrance_cnt>0 then 1 else 0 end) app_entrance_cnt
       ,nvl(sum(app_entrance_cnt),0) app_entrance_ct
       ,cast(sum(case when app_entrance_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_entrance_per
       ,cast(nvl(sum(app_entrance_cnt),0)/count(1) as decimal(38,6)) app_entrance_ct_per
       ,sum(case when app_choose_cnt>0 then 1 else 0 end) app_choose_cnt
       ,nvl(sum(app_choose_cnt),0) app_choose_ct
       ,cast(sum(case when app_choose_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_choose_per
       ,cast(nvl(sum(app_choose_cnt),0)/count(1) as decimal(38,6)) app_choose_ct_per
       ,sum(case when app_apply_cnt>0 then 1 else 0 end) app_apply_cnt
       ,nvl(sum(app_apply_cnt),0) app_apply_ct
       ,cast(sum(case when app_apply_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_apply_per
       ,cast(nvl(sum(app_apply_cnt),0)/count(1) as decimal(38,6)) app_apply_ct_per
       ,sum(case when app_write_cnt>0 then 1 else 0 end) app_write_cnt
       ,nvl(sum(app_write_cnt),0) app_write_ct
       ,cast(sum(case when app_write_cnt>0 then 1 else 0 end)/count(1) as decimal(38,6)) app_write_per
       ,cast(nvl(sum(app_write_cnt),0)/count(1) as decimal(38,6)) app_write_ct_per 
       ,sum(case when app7_act1>0 then 1 else 0 end) app7_act1_uct
       ,nvl(sum(app7_act1),0) app7_act1_ct
       ,cast(sum(case when app7_act1>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act1_per
       ,cast(nvl(sum(app7_act1),0)/count(1) as decimal(38,6)) app7_act1_ct_per
       ,sum(case when app7_act2>0 then 1 else 0 end) app7_act2_uct
       ,nvl(sum(app7_act2),0) app7_act2_ct
       ,cast(sum(case when app7_act2>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act2_per
       ,cast(nvl(sum(app7_act2),0)/count(1) as decimal(38,6)) app7_act2_ct_per
       ,sum(case when app7_act32>0 then 1 else 0 end) app7_act32_uct
       ,nvl(sum(app7_act32),0) app7_act32_ct
       ,cast(sum(case when app7_act32>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act32_per
       ,cast(nvl(sum(app7_act32),0)/count(1) as decimal(38,6)) app7_act32_ct_per
       ,sum(case when app7_act4>0 then 1 else 0 end) app7_act4_uct
       ,nvl(sum(app7_act4),0) app7_act4_ct
       ,cast(sum(case when app7_act4>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act4_per
       ,cast(nvl(sum(app7_act4),0)/count(1) as decimal(38,6)) app7_act4_ct_per
       ,sum(case when app7_act52>0 then 1 else 0 end) app7_act52_uct
       ,nvl(sum(app7_act52),0) app7_act52_ct
       ,cast(sum(case when app7_act52>0 then 1 else 0 end)/count(1) as decimal(38,6)) app7_act52_per
       ,cast(nvl(sum(app7_act52),0)/count(1) as decimal(38,6)) app7_act52_ct_per
       ,nvl(sum(apply_ind),0) apply_ind_cnt
       ,cast(nvl(sum(apply_ind),0)/count(1) as decimal(38,6)) apply_ind_per
       ,sum(pre_tot_asset_n_zhixiao_month_avg_bal),sum(tot_asset_n_zhixiao_month_avg_bal)
       ,sum(pre_aum_tm),sum(aum_tm)
       ,sum(pre_dpst_tm),sum(dpst_tm)
  from temp
 group by fenhang,user_type
 order by fenhang,user_type;
