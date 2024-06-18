create or replace PACKAGE BODY "PK_TLS_BONUS" as
  procedure pr_tls_connect is
  begin
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_CONNECT');
    delete ---+parallel(4)
    tb_tls_connect
    where 
    --to_date(month, 'YYYY-MM') < add_months(trunc(sysdate, 'MM'), -3) or
     (to_date(month, 'YYYY-MM') >= add_months(trunc(sysdate, 'MM'), -1) and
     month != '2019-09' --will delete later
     );
  
    commit;
  
    insert ---+append
    into tb_tls_connect
      (month
      ,username
      ,connected
      ,channel)
      with emp as
       (select username
              ,team
              ,date_valid_from
              ,nvl(date_valid_to, sysdate) date_valid_to
              ,channel
        from   tb_tls_employee users
        where  channel in ('CLX', 'ACLX', 'CCX', 'CCXP'))
      select ---+materialize parallel(4)
       to_char(time_of_call_start, 'YYYY-MM') month
      ,resource_name username
      ,count(*) connected
      ,emp.channel
      from   tb_tls_call_genesys g
      join   emp
      on     lower(g.resource_name) = emp.username
      and    g.time_of_call_start >= date_valid_from
      and    g.time_of_call_start < date_valid_to + 1
      and    time_of_call_start >= add_months(trunc(sysdate, 'MM'), -1)
      where  (durations >= 15 or (off_by = 'Agent' or off_by = 'Client'))
      and    durations > 0
      and    trunc(time_of_call_start) != date
       '2021-01-01'
      and    trunc(time_of_call_start) != date
       '2021-02-09'
      and    trunc(time_of_call_start) != date
       '2021-02-10'
      and    trunc(time_of_call_start) != date
       '2021-02-11'
      and    trunc(time_of_call_start) != date
       '2021-02-12'
      and    trunc(time_of_call_start) != date
       '2021-02-13'
      and    trunc(time_of_call_start) != date
       '2021-02-14'
      and    trunc(time_of_call_start) != date
       '2021-02-15'
      and    trunc(time_of_call_start) != date
       '2021-02-16'
      and    trunc(time_of_call_start) != date
       '2021-02-17'
      and    trunc(time_of_call_start) != date
       '2021-02-18'
      and    trunc(time_of_call_start) != date
       '2021-04-30'
      and    trunc(time_of_call_start) != date
       '2021-05-01'
      and    trunc(time_of_call_start) != date
       '2022-01-29'
      and    trunc(time_of_call_start) != date
       '2022-01-30'
      and    trunc(time_of_call_start) != date
       '2022-02-06' ---------add  ngay loai tru de khong tinh ranking
      and    ((time_of_call_start between '01-OCT-19' and '09-OCT-19' and
            (call_type = 'Inbound' or
            (custom_data_4 like '%ProAct%' or custom_data_4 like '%TLS%' or
            custom_data_4 like '%Pred%'))) or
            (time_of_call_start >= '09-OCT-19' and
            (call_type = 'Inbound' or name like '%SAS%')))
      group  by to_char(time_of_call_start, 'YYYY-MM')
               ,resource_name
               ,emp.channel;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
    --------------------------------------------------
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_CONNECT_CLIENT');
    delete ---+parallel(4)
    TB_TLS_CONNECT_CLIENT
    where  (to_date(month, 'YYYY-MM') >= add_months(trunc(sysdate, 'MM'), -1));
    commit;
  
    insert into ---+append 
    TB_TLS_CONNECT_CLIENT
      with emp as
       (select username
              ,team
              ,date_valid_from
              ,nvl(date_valid_to, sysdate) date_valid_to
              ,channel
        from   tb_tls_employee users
        where  channel in ('CLX', 'ACLX', 'CCX', 'CCXP'))
      select ---+materialize parallel(4)
       to_char(time_of_call_start, 'YYYY-MM') month
      ,resource_name username
      ,count(distinct cuid) connected_client
      from   tb_tls_call_genesys g
      join   emp
      on     lower(g.resource_name) = emp.username
      and    g.time_of_call_start >= date_valid_from
      and    g.time_of_call_start < date_valid_to + 1
      and    time_of_call_start >= add_months(trunc(sysdate, 'MM'), -1)
      where  (durations >= 15 or (off_by = 'Agent' or off_by = 'Client'))
      and    durations > 0
      and    ((time_of_call_start between '01-OCT-19' and '09-OCT-19' and
            (call_type = 'Inbound' or
            (custom_data_4 like '%ProAct%' or custom_data_4 like '%TLS%' or
            custom_data_4 like '%Pred%'))) or
            (time_of_call_start >= '09-OCT-19' and
            (call_type = 'Inbound' or name like '%SAS%')))
      group  by to_char(time_of_call_start, 'YYYY-MM')
               ,resource_name;
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  end;

  ----------------------------mobile app--------------------------------------------------------

  procedure pr_tls_mobile_app is
  begin
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE PR_TLS_MOBILE_APP');
    delete ---+parallel(4)
    tb_tls_mobile_app
    where  to_date(sign_up_month, 'YYYY-MM') >=
           add_months(trunc(sysdate, 'MM'), -1);
  
    commit;
  
    insert ----+append
    into tb_tls_mobile_app
      with tmp as
       (select ---+materialize parallel(4)
         to_char(mb.date_effective, 'yyyy-mm') sign_up_month
        ,gen.resource_name username
        ,d.id_cuid cuid
        ,mb.date_effective as sign_up_date
        ,gen.time_of_call_start call_date
        ,row_number() over(partition by gen.cuid order by gen.time_of_call_start desc) rk
        from   owner_dwh.dc_online_user mb
        join   owner_dwh.dc_client d
        on     mb.skp_client = d.skp_client
        join   tb_tls_call_genesys gen
        on     d.id_cuid = gen.cuid
        and    gen.time_of_call_start between mb.date_effective - 7 and
               mb.date_effective
        and    mb.date_effective >= add_months(trunc(sysdate, 'MM'), -1)
        and    gen.time_of_call_start >=
               add_months(trunc(sysdate, 'MM'), -1) - 24
        --and    gen.time_of_call_start < date '2021-10-01' --lay khoang ngay 24 cua thang truoc do
        where  gen.cuid is not null)
      select ---+materialize parallel(4)
       sign_up_month
      ,cuid
      ,sign_up_date
      ,username
      ,call_date
      from   tmp
      where  rk = 1;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
  end;

  ----------------get bonus for user sending lead--------------------------------------------------------------------
  procedure pr_tls_bonus_sending_lead(v_date in date default sysdate,
                                      v_num  in number default 7) is
  
  begin
  
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_BONUS_SENDING_LEAD');
    delete ---+parallel(4)
    tb_tls_bonus_sending_lead
    where  sign_date >= trunc(v_date) - v_num;
  
    commit;
  
    insert ---+append
    into tb_tls_bonus_sending_lead
      with ct as
       (select ---+materialize parallel(4)
         contract_code
        ,cuid
        ,sign_date
        ,preprocess_date
        ,'CCX' product
        ,orig_offer_id
        from   tb_tls_appl_ccx
        where  sign_date >= trunc(v_date) - v_num
        
        union all
        
        select ---+materialize parallel(4)
         contract_code
        ,cuid
        ,sign_date
        ,preprocess_date
        ,case
           when product_code like '%CLXMCB%' then
            'CLXCB'
           when product_code like '%CLW%' then
            'CLW'
           when product_code like '%CLXSA%' then
            'CLW'
           when product_code like '%CLX%' then
            'CLX'
         end product
        ,orig_offer_id
        from   tb_tls_appl_clx
        where  sign_date >= trunc(v_date) - v_num)
      
      select contract_code
            ,cuid
            ,sign_date
            ,product
            ,username
            ,time_of_call_start
      from   (select ---+materialize parallel(4)
               ct.contract_code
              ,ct.cuid
              ,ct.sign_date
              ,ct.product
              ,gen.username
              ,gen.time_of_call_start
              ,row_number() over(partition by gen.cuid order by gen.time_of_call_start desc) rk
              from   ct
              join   tb_tls_call_sas gen
              on     gen.cuid = to_char(ct.cuid)
              and    gen.time_of_call_start < ct.preprocess_date
              and    ct.product <> gen.channel
              and    ct.orig_offer_id = gen.orig_offer_id
              and    gen.durations >= 15
              where  decode(gen.response_result_2,
                            'none',
                            gen.code_disposition,
                            gen.response_result_2) = 'Sending Lead'
              and    gen.time_of_call_start >= trunc(sysdate - 75)
              and    ct.product in ('CLX', 'CCX')
              /*         union all
              select ct.contract_code
                    ,ct.cuid
                    ,ct.sign_date
                    ,ct.product
                    ,gen.username
                    ,gen.time_of_call_start
                    ,row_number() over(partition by gen.cuid order by gen.time_of_call_start desc) rk
              from   ct
              join   tb_tls_call_sas gen
              on     gen.cuid = to_char(ct.cuid)
              and    gen.time_of_call_start < ct.preprocess_date
              and    ct.product <> gen.channel
              and    gen.durations >= 15
              where  gen.response_result_2 = 'Sending Lead'
              and    gen.time_of_call_start >= trunc(sysdate - 75)
              and    ct.product in ('CLXCB')*/ ----commented  by quynhcao nov 02 2021
              
              )
      where  rk = 1
      union all
      
      select contract_code
            ,cuid
            ,sign_date
            ,'CLW' product
            ,username
            ,time_of_call_start
      from   (select ---+materialize parallel(4)
               clw.contract_code
              ,clw.cuid
              ,clw.sign_date
              ,clw.preprocess_date
              ,c.username
              ,c.time_of_call_start
              ,row_number() over(partition by clw. contract_code order by c.time_of_call_start desc) rk
              from   tb_tls_appl_clx clw
              join   tb_tls_call_sas c
              on     c.cuid = to_char(clw.cuid)
              and    c.time_of_call_start >= (trunc(v_date) - v_num) - 30
              and    c.time_of_call_start < clw.sign_date
              and    clw.sign_date < trunc(c.time_of_call_start) + 8
              where  c.durations >= '15'
              and    clw.product_code in ('CLWND', 'CLWAD')
              and    clw.preprocess_date >= (trunc(v_date) - v_num) - 15
              and    clw.sign_date >= trunc(v_date) - v_num
              and    decode(c.response_result_2,
                            'none',
                            c.code_disposition,
                            c.response_result_2) =
                     'Schedule_Appointment_with_SA')
      
      where  rk = 1;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
  end;

  ----------------------------get bonus for user pusing sign--------------------------------------------------------
  procedure pr_tls_bonus_pushing_sign(v_date in date default sysdate,
                                      v_num  in number default 7) is
  
  begin
  
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_BONUS_PUSHING_SIGN');
    delete ---+parallel(4)
    tb_tls_bonus_pushing_sign
    where  sign_date >= trunc(v_date) - v_num;
  
    commit;
  
    insert ----+append
    into tb_tls_bonus_pushing_sign
      with ct as
       (select contract_code
              ,cuid
              ,approve_date
              ,sign_date
        from   tb_tls_appl_clx
        where  sign_date >= trunc(v_date) - v_num
        and    preprocess_date >= trunc(v_date) - v_num - 15
        and    product_code not in ('CLWND', 'CLWAD'))
      
      select contract_code
            ,sign_date
            ,username           pushing_user
            ,time_of_call_start pushing_date
      from   (select ----+materialize parallel(4)
               ct.contract_code
              ,ct.cuid
              ,ct.sign_date
              ,gen.username
              ,row_number() over(partition by gen.cuid order by gen.time_of_call_start desc) rk
              ,gen.time_of_call_start
              from   ct
              join   tb_tls_call_sas gen
              on     gen.cuid = to_char(ct.cuid)
              and    gen.time_of_call_start > ct.approve_date
              and    gen.time_of_call_start < ct.sign_date
              where  gen.durations >= '15'
              and    gen.time_of_call_start >= trunc(v_date) - v_num - 30
              and    decode(gen.response_result_2,
                            'none',
                            gen.code_disposition,
                            gen.response_result_2) in
                     ('Push Sign - Client Agrees', 'Sign Agree','Client agrees') -- Add client agree from Nov 1st - Duy Pham
              
              )
      where  rk = 1
      
      union all
      
      select contract_code
            ,sign_date
            ,username           pushing_user
            ,time_of_call_start pushing_date
      from   (select ----+materialize parallel(4)
               clw.contract_code
              ,clw.cuid
              ,clw.sign_date
              ,clw.preprocess_date
              ,c.username
              ,c.time_of_call_start
              ,row_number() over(partition by clw. contract_code order by c.time_of_call_start desc) rk
              from   tb_tls_appl_clx clw
              join   tb_tls_call_sas c
              on     c.cuid = to_char(clw.cuid)
              and    c.time_of_call_start >= trunc(v_date) - v_num - 30
              and    c.time_of_call_start < clw.sign_date
              and    clw.sign_date < trunc(c.time_of_call_start) + 8
              where  c.durations >= '15'
              and    clw.product_code in ('CLWND', 'CLWAD')
              and    clw.preprocess_date >= trunc(v_date) - 15
              and    clw.sign_date >= trunc(v_date) - v_num
              and    decode(c.response_result_2,
                            'none',
                            c.code_disposition,
                            c.response_result_2) =
                     'Schedule_Appointment_with_SA')
      
      where  rk = 1;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  end;

  ------------------------------------------------------------------------------------
  procedure pr_tls_push_usage_call is
  begin
  
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_CALL_TASK_PUSH_USAGE');
  
    execute immediate 'truncate table TB_TLS_CALL_TASK_PUSH_USAGE';
  
    insert --+append
    into tb_tls_call_task_push_usage
      select ----+materialize parallel(4)
       SubStr(t1.id_source, 0, InStr(t1.id_source, '.') - 1) il_communication_id
      ,t1.skp_client
      ,t1.date_campaign_end
      ,t1.date_campaign_start
      ,t1.code_call_list_type
      ,t1.text_name_call_list
      from   owner_dwh.f_output_load_tt t1
      where  code_output_load_type = 'TLS'
      and    date_communication_start >= sysdate - 20
      and    t1.DTIME_CREATED >= sysdate - 30 ----Vi added on 20221213
            
      and    (text_name_call_list like '%USAGE%PUSH' or
            text_name_call_list like '%ACTIVATE%PUSH')
      and    nvl(code_call_list_type, 'xxx') != 'GAMI_TRX_LESS500K' --hoa q added 16 mar 23
      and    text_name_call_list != 'ATM USAGE PUSH';
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_PUSH_USAGE_CALL');
    delete ---+parallel(4)
    tb_tls_push_usage_call
    where  time_of_call_start /*date_campaign_start */
           >= trunc(sysdate) - 7; --- edited by quynhcao
    commit;
    insert --+append
    into tb_tls_push_usage_call
      with call_v_num_days as
       (select --+materialize parallel(4)
        
         t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,t1.record_type
        ,t1.cuid
        ,t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,decode(t1.response_result_2,
                'none',
                t1.code_disposition,
                t1.response_result_2) response_result_2
        ,t1.call_list_gen
        
        from   tb_tls_call_sas t1
        where  time_of_call_start >= trunc(sysdate) - 7
        and    t1.interaction_type_key <> 3 -- added by quynhcao nov 01 2021
        and    decode(t1.response_result_2,
                      'none',
                      t1.code_disposition,
                      t1.response_result_2) is not null
        and    username is not null
        and    t1.channel = 'CCXP'
        and    t1.call_type = 'Outbound'
        and    (t1.time_of_call_start < date
               '2023-03-16' or t1.time_of_call_start >= date '2023-03-21') -- hoanganh 26/04
        union
        select --+materialize parallel(4)
        
         s.time_of_call_start
        ,s.time_of_call_end
        ,s.durations
        ,case
           when s1.call_back_type = 'campaign' then
            'Campaign Callback'
           when s1.call_back_type = 'personal' then
            'Personal Callback'
           else
            'General'
         end as record_type
        ,s.cuid
        ,to_number(s1.il_communication_id) as il_communication_id
        ,s.call_id
        ,s.username
        ,decode(s.response_result_2,
                'none',
                s.code_disposition,
                s.response_result_2) response_result_2
        ,case
           when s1.call_back_call_list is null then
            'SAS_TLS_OUTBOUND_CCX_USAGE_PUSH'
           else
            s1.call_back_call_list
         end call_list_gen
        from   tb_tls_call_sas s
        left   join tb_call_sas_16_20_3 s1
        on     s1.call_id = s.call_id
        where  s.time_of_call_start >= date '2023-03-16' -- missing data call sas, cant recover, union with by tb_call_sas_16_20_3 resent manual
        and    s.time_of_call_start < date
         '2023-03-21'
        and    s.interaction_type_key <> 3 -- added by quynhcao nov 01 2021
        and    decode(s.response_result_2,
                      'none',
                      s.code_disposition,
                      s.response_result_2) is not null
        and    s.username is not null
        and    s.channel = 'CCXP'
        and    s.call_type = 'Outbound'
        and    s1.il_communication_id is not null -- hoanganh 26/04
        
        ) --- thao 24/08/2021
      ,
      call_push as
       (select --+materialize parallel(4)
         t2.date_campaign_start
        ,t2.date_campaign_end
        ,t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,t1.record_type
        ,t1.cuid
        ,t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,t1.response_result_2
        ,t1.call_list_gen
        ,t2.text_name_call_list
        ,skp_client
        ,t2.code_call_list_type
        from   call_v_num_days t1
        join   tb_tls_call_task_push_usage t2 --tb_tls_call_task_push_usage
        on     t1.il_communication_id = t2.il_communication_id),
      call_push_usage_agree as
       (select ---+materialize parallel(4)
         date_campaign_start
        ,date_campaign_end
        ,text_name_call_list
        ,time_of_call_start
        ,time_of_call_end
        ,durations
        ,case
           when call_list_gen = 'SAS_TLS_OUTBOUND_CCX_USAGE_PUSH' then
            '1st call'
         end call_type
        ,record_type
        ,cuid
        ,il_communication_id
        ,call_id
        ,username
        ,skp_client
        ,code_call_list_type
        from   call_push
        where  response_result_2 in ('Push Use - Client Agrees', 'Use Agree','Client agrees') -- Add client agree from Nov 1st - Duy Pham
        and    durations >= 15
        and    il_communication_id is not null
        
        union
        
        select ---+materialize parallel(4)
         date_campaign_start
        ,date_campaign_end
        ,t2.text_name_call_list
        ,t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,case
           when t1.il_communication_id is not null
                and t1.call_list_gen =
                'SAS_RTDM_TLS_OUTBOUND_CCX_USAGE_PUSH_CALLBACK' then
           
            'Call back RTDM'
         
           when t1.il_communication_id is null then
            'Call back Personal'
         end call_type
        ,t1.record_type
        ,t1.cuid
        ,t2.il_communication_id --t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,skp_client
        ,code_call_list_type
        from   call_v_num_days t1
        join   call_push t2
        on     t1.cuid = t2.cuid
        and    t1.time_of_call_start > t2.time_of_call_start
        and    t1.time_of_call_start < trunc(t2.time_of_call_start) + 4
        where  1 = 1
        and    t1.response_result_2 in ('Push Use - Client Agrees', 'Use Agree','Client agrees') -- Add client agree from Nov 1st - Duy Pham
        and    t1.durations >= 15 -----edited by quynhcao 02 nov2021
        /*union
        
        select t2.date_campaign_start
              ,t2.date_campaign_end
              ,t2.text_name_call_list
              ,t1.time_of_call_start
              ,t1.time_of_call_end
              ,t1.durations
              ,'Call back Personal' \*'Call back Manual'*\ call_type -- edited by quynhcao nov 01 2021
              ,t1.record_type
              ,t1.cuid
              ,t2.il_communication_id
              ,t1.call_id
              ,t1.username
              ,skp_client
              ,t2.code_call_list_type
        from   call_v_num_days t1
        join   call_push t2
        on     t1.cuid = t2.cuid
        and    t1.time_of_call_start > t2.time_of_call_start
        and    t1.time_of_call_start < trunc(t2.time_of_call_start) + 3
        where  1 = 1
        and    t1.response_result_2 = 'Push Use - Client Agrees'
        and    t1.durations >= 15
        and    t1.il_communication_id is null*/
        ),
      contract as
       (select ---+materialize parallel(4)
         a2.ID_CUID
        ,a2.NUM_APL_CONTRACT
        ,a2.CODE_APL_PROD_GR
        ,case
           when a2.DTIME_APL_SIGN = date '3000-01-01' then
            null
           else
            a2.DTIME_APL_SIGN
         end DTIME_APL_SIGN -- hoanganh 26/04/2023
        ,nvl(a2.DTIME_APL_CLOSE, date '3000-01-01') apl_close_dtm
        from   dm_crm_rep.f_crm_appl a2 ---ap_crm.tb_crm_appl a2
        where  CODE_APL_PROD_GR like 'CC%' -- hoanganh 13.09.2023
              --CODE_APL_PROD_GR in ('CCX', 'CC_BHX', 'CC_CC', 'CC_SC', 'CC_VCC')  
              --  and    date_decision >= trunc(sysdate) - 90 -- hoanganh edit 07022023
        and    decode(a2.DTIME_APL_SIGN,
                      date '3000-01-01',
                      null,
                      a2.DTIME_APL_SIGN) is not null
        and    (a2.DTIME_APL_CANCEL = date '3000-01-01') -- hoanganh 26/04/2023
        --and    skp_credit_status <> 4
        
        )
      /*card_replace as -- hoanganh 07022023
      (select --+materialize parallel(4)
       distinct t.text_contract_number
               ,t.skp_credit_case
               ,c.dtime_modified
               ,b.code
               ,ct.cuid
       from   owner_dwh.dc_contract t
       left   join owner_dwh.dc_card c
       on     c.skp_credit_case = t.skp_credit_case
       left   join owner_dwh.cl_card_status m
       on     c.skp_card_status = m.skp_card_status
       left   join owner_int.vh_hom_contract ctr
       on     ctr.CONTRACT_CODE = t.text_contract_number
       left   join owner_int.vh_hom_product b
       on     ctr.product_id = b.id
       inner  join owner_int.vh_hom_deal dl
       on     ctr.deal_code = dl.code
       left   join owner_int.vh_hom_client ct
       on     dl.client_id = ct.id
       where  c.dtime_modified >= trunc(sysdate) - 90
       and    m.name_card_status = 'ACTIVE')*/
      select ---+materialize parallel(4)
       t1.date_campaign_start
      ,t1.date_campaign_end
      ,t1.text_name_call_list
      ,t1.time_of_call_start
      ,t1.time_of_call_end
      ,t1.durations
      ,t1.call_type
      ,t1.record_type
      ,t1.cuid
      ,t1.il_communication_id
      ,t1.call_id
      ,t1.username
      ,t1.skp_client
      ,t1.code_call_list_type
       
      ,t2.NUM_APL_CONTRACT as contract_code
      ,case
         when t2.CODE_APL_PROD_GR = 'CCX' then
          'CCX'
         when t2.CODE_APL_PROD_GR like '%VC%' then
          'VCC'
         else
          'OTHER'
       end product_type ---edited by quynhcao 05jan2022
      ,t2.DTIME_APL_SIGN as apl_sign_dtm
      from   call_push_usage_agree t1
      left   join contract t2
      on     t1.cuid = t2.id_cuid
      and    t1.time_of_call_start < t2.apl_close_dtm
      and    t1.time_of_call_start > t2.DTIME_APL_SIGN
      
      group  by t1.date_campaign_start
               ,t1.date_campaign_end
               ,t1.text_name_call_list
               ,t1.time_of_call_start
               ,t1.time_of_call_end
               ,t1.durations
               ,t1.call_type
               ,t1.record_type
               ,t1.cuid
               ,t1.il_communication_id
               ,t1.call_id
               ,t1.username
               ,t1.skp_client
               ,t1.code_call_list_type
                
               ,t2.NUM_APL_CONTRACT
               ,case
                  when t2.CODE_APL_PROD_GR = 'CCX' then
                   'CCX'
                  when t2.CODE_APL_PROD_GR like '%VC%' then
                   'VCC'
                  else
                   'OTHER'
                end ---edited by quynhcao 05jan2022
               ,t2.DTIME_APL_SIGN;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
  end;

  -----------------------------------------------
  procedure pr_tls_push_usage_bonus is
  
  begin
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE tb_tmp_push_usage');
  
    execute immediate 'truncate table tb_tmp_push_usage';
    insert --+append
    into tb_tmp_push_usage
      with push as
       (select ---+materialize parallel(4)
         t.date_campaign_start
        ,t.date_campaign_end
        ,t.text_name_call_list
        ,t.time_of_call_start
        ,t.time_of_call_end
        ,t.durations
        ,t.call_type
        ,t.record_type
        ,t.cuid
        ,t.il_communication_id
        ,t.call_id
        ,t.username
        ,t.skp_client
        ,t.code_call_list_type
        ,t.contract_code
        ,t.product_type
        ,t.apl_sign_dtm
        ,row_number() over(partition by t.il_communication_id order by time_of_call_start desc) rn
         
        ,date_campaign_start + 8 end_apt_push_usage
        ,min(time_of_call_start) over(partition by t.il_communication_id) call_1st ---editted by quynhcao 02 nov2021
        from   tb_tls_push_usage_call t),
      emp as
       (select username
              ,team
              ,date_valid_from
              ,case
                 when date_valid_to is null then
                  sysdate
                 else
                  date_valid_to
               end date_valid_to
              ,channel
        from   tb_tls_employee users
        where  channel in ('CCX', 'CCXP')),
      map_transaction as
       (select ---+materialize parallel(4)
        distinct t1.product_type
                ,t1.date_campaign_start
                ,t1.end_apt_push_usage
                ,t1.text_name_call_list
                ,t1.code_call_list_type
                ,t1.time_of_call_start
                ,t1.durations
                ,t1.call_type
                ,t1.cuid
                ,t1.il_communication_id
                ,t1.call_id
                ,t1.username
                ,t4.name_card_acceptor
                ,t4.amt_billing
                ,t4.dtime_auth_transaction
                ,t1.contract_code
                ,t1.apl_sign_dtm
        from   push t1
        left   join owner_dwh.dc_contract t2
        on     t1.contract_code = t2.text_contract_number
        left   join owner_dwh.dc_card_account t3
        on     t3.skp_contract = t2.skp_contract
        left   join owner_dwh.f_card_transaction_tt t4
        on     t3.skp_card_account = t4.skp_card_account
        and    t4.dtime_auth_transaction > t1.call_1st
        and    t4.dtime_auth_transaction <= t1.end_apt_push_usage + 1
        and    t4.code_card_transaction_group = 'WITHDRAWAL'
        and    t4.dtime_auth_transaction >= t1.apl_sign_dtm
        where  rn = 1)
      select ---+materialize parallel(4)
       t1.product_type
      ,t1.date_campaign_start
      ,t1.end_apt_push_usage
      ,t1.text_name_call_list
      ,t1.code_call_list_type
      ,t1.time_of_call_start
      ,t1.durations
      ,t1.call_type
      ,t1.cuid
      ,t1.il_communication_id
      ,t1.call_id
      ,t1.username
      ,sum(amt_billing) total_amount
      ,count(case
               when dtime_auth_transaction < date '2022-10-01' then
                dtime_auth_transaction
               when dtime_auth_transaction >= date
                '2022-10-01'
                    and (Upper(t1.name_card_acceptor) like '%FOODY%' or
                    Upper(t1.name_card_acceptor) like '%SHOPEE%' or
                    Upper(t1.name_card_acceptor) like '%LAZADA%' or
                    Upper(t1.name_card_acceptor) like '%TIKI%' or
                    (Upper(t1.name_card_acceptor) like '%SENDO%' and
                    Upper(t1.name_card_acceptor) not like '%SENDONGA%') or
                    Upper(t1.name_card_acceptor) like '%GRAB%' or
                    (Upper(t1.name_card_acceptor) like '%BE GROUP%' and
                    Upper(t1.name_card_acceptor) != 'CNBENTRECTYKAYGROUP') or
                    Upper(t1.name_card_acceptor) like '%BAE%MIN%' or
                    Upper(t1.name_card_acceptor) like '%ZALOPAY%' or
                    Upper(t1.name_card_acceptor) like '%MOMO%' or
                    Upper(t1.name_card_acceptor) like '%GOOGLE%' or
                    Upper(t1.name_card_acceptor) like '%APPLE.COM/BILL%' or
                    Upper(t1.name_card_acceptor) like '%NETFLIX%' or
                    Upper(t1.name_card_acceptor) like '%TRAVELOKA%' or
                    Upper(t1.name_card_acceptor) like '%AGODA%' or
                    Upper(t1.name_card_acceptor) like '%BOOKING%COM%') then
                dtime_auth_transaction
               else
                null
             end) num_transaction --thuan adjust 7 oct 22
      ,min(dtime_auth_transaction) first_usage_date
      ,t1.contract_code
      ,t1.apl_sign_dtm
      ,emp.team
      ,count(distinct dtime_auth_transaction) total_transaction
      from   map_transaction t1
      left   join emp
      on     t1.username = emp.username
      and    t1.time_of_call_start >= emp.date_valid_from
      and    t1.time_of_call_start < nvl(emp.date_valid_to, sysdate) + 1
      group  by t1.date_campaign_start
               ,t1.text_name_call_list
               ,t1.time_of_call_start
               ,t1.durations
               ,t1.call_type
               ,t1.cuid
               ,t1.il_communication_id
               ,t1.call_id
               ,t1.username
               ,t1.end_apt_push_usage
               ,code_call_list_type
               ,t1.contract_code
               ,t1.product_type
               ,t1.apl_sign_dtm
               ,emp.team;
  
    commit;
  
    execute immediate 'TRUNCATE TABLE TB_TMP_BOD1_BF_1ST_PUSH';
    insert --+append
    into tb_tmp_bod1_bf_1st_push
      with usage_bef_push as
       (select ---+materialize parallel(4)
         c.contract_code
        ,c.name_credit_status
        ,c.cuid
        ,c.apt_date
        ,c.apt_user
        ,c.apt_team
        ,c.sign_date
        ,c.product_code
        ,p.time_of_call_start
        ,row_number() over(partition by c.cuid, c.sign_date order by p.time_of_call_start) rnk
        ,p.total_transaction
        from   tb_tls_ccx_bonus c
        join   tb_tmp_push_usage p
        on     c.cuid = p.cuid
        and    c.sign_date < p.time_of_call_start
        where  c.sign_date >= date
         '2020-08-01'
        and    c.name_credit_status not in ('Cancelled')
        --, 'Finished')
        ),
      map_transation as
       (select ---+materialize parallel(4)
        distinct p.contract_code
                ,p.name_credit_status
                ,p.cuid
                ,p.apt_date
                ,p.apt_user
                ,p.apt_team
                ,p.sign_date
                ,p.product_code
                ,p.time_of_call_start
                ,t4.name_card_acceptor
                ,t4.amt_billing
                ,t4.dtime_auth_transaction
                ,p.total_transaction
        from   usage_bef_push p
        join   owner_dwh.dc_contract t2
        on     p.contract_code = t2.text_contract_number
        join   owner_dwh.dc_card_account t3
        on     t3.skp_contract = t2.skp_contract
        join   owner_dwh.f_card_transaction_tt t4
        on     t3.skp_card_account = t4.skp_card_account
        and    t4.code_card_transaction_group = 'WITHDRAWAL'
        and    t4.dtime_auth_transaction >= p.sign_date
        and    t4.dtime_auth_transaction < time_of_call_start
        and    t4.dtime_auth_transaction >= date '2020-08-01'
        where  rnk = 1)
      select ---+materialize parallel(4)
       p2.contract_code
      ,p2.name_credit_status
      ,p2.cuid
      ,p2.apt_date
      ,p2.apt_user
      ,p2.apt_team
      ,p2.sign_date
      ,p2.product_code
      ,p2.time_of_call_start
      ,sum(p2.amt_billing) total_amount
      ,count(case
               when p2.dtime_auth_transaction < date '2022-10-01' then
                p2.dtime_auth_transaction
               when p2.dtime_auth_transaction >= date
                '2022-10-01'
                    and (Upper(p2.name_card_acceptor) like '%FOODY%' or
                    Upper(p2.name_card_acceptor) like '%SHOPEE%' or
                    Upper(p2.name_card_acceptor) like '%LAZADA%' or
                    Upper(p2.name_card_acceptor) like '%TIKI%' or
                    (Upper(p2.name_card_acceptor) like '%SENDO%' and
                    Upper(p2.name_card_acceptor) not like '%SENDONGA%') or
                    Upper(p2.name_card_acceptor) like '%GRAB%' or
                    (Upper(p2.name_card_acceptor) like '%BE GROUP%' and
                    Upper(p2.name_card_acceptor) != 'CNBENTRECTYKAYGROUP') or
                    Upper(p2.name_card_acceptor) like '%BAE%MIN%' or
                    Upper(p2.name_card_acceptor) like '%ZALOPAY%' or
                    Upper(p2.name_card_acceptor) like '%MOMO%' or
                    Upper(p2.name_card_acceptor) like '%GOOGLE%' or
                    Upper(p2.name_card_acceptor) like '%APPLE.COM/BILL%' or
                    Upper(p2.name_card_acceptor) like '%NETFLIX%' or
                    Upper(p2.name_card_acceptor) like '%TRAVELOKA%' or
                    Upper(p2.name_card_acceptor) like '%AGODA%' or
                    Upper(p2.name_card_acceptor) like '%BOOKING%COM%') then
                p2.dtime_auth_transaction
               else
                null
             end) num_transaction --thuan adjust 7 oct 22
      ,min(p2.dtime_auth_transaction) frist_usage_date
      ,p2.total_transaction
      from   map_transation p2
      group  by p2.contract_code
               ,p2.name_credit_status
               ,p2.cuid
               ,p2.apt_date
               ,p2.apt_user
               ,p2.apt_team
               ,p2.sign_date
               ,p2.product_code
               ,p2.time_of_call_start
               ,p2.total_transaction;
    commit;
    ------------------------------
  
    execute immediate 'TRUNCATE TABLE TB_TMP_BOD1';
    insert --+append
    into tb_tmp_bod1
      with having_push as
       (select ---+materialize parallel(4)
         t.contract_code
        ,case
           when t.product_type like '%VC%'
                and t.date_campaign_start between date '2021-12-01' and date
            '2022-02-28'
                and t.total_amount >= 200000 then
            1
           when t.total_amount >= 500000
                or (t.date_campaign_start >= date
                 '2022-10-01' and t.num_transaction >= 3) --thuan adjust 7 oct 22
            then
            1
         end flag_bonus_use --edit 06dec2021 by quynhcao
        ,t.total_transaction
        from   tb_tmp_push_usage t
        -- where  t.total_amount >= 500000
        union
        select ---+materialize parallel(4)
         t2.contract_code
        ,case
           when t2.product_code like '%VC%'
                and t2.sign_date between date '2021-12-01' and date
            '2022-02-28'
                and t2.total_amount >= 200000 then
            1
           when t2.total_amount >= 500000
                or
                (t2.sign_date >= date '2022-10-01' and t2.num_transaction >= 3) --thuan adjust 7 oct 22
            then
            1
         end flag_bonus_use --edit 06dec2021 by quynhcao
        ,t2.total_transaction
        from   tb_tmp_bod1_bf_1st_push t2
        --  where  t2.total_amount >= 500000
        )
      select ---+materialize parallel(4)
       t1.contract_code
      ,t1.sign_date
      ,t1.cuid
      ,t1.apt_user
      ,t1.t_amount_usage_bonus total_amount
      ,t1.number_trans_bonus num_transaction
      ,t1.f_use first_usage_date
      ,t1.apt_team
      ,t1.product_gr --edit 06dec2021 by quynhcao
      ,count(distinct t4.dtime_auth_transaction) total_transaction ----Vi added on 2022-10-14
      
      from   tb_tls_ccx_bonus t1
      left   join owner_dwh.dc_contract t2
      on     t1.contract_code = t2.text_contract_number
      left   join owner_dwh.dc_card_account t3
      on     t3.skp_contract = t2.skp_contract
      left   join owner_dwh.f_card_transaction_tt t4
      on     t3.skp_card_account = t4.skp_card_account
      and    t4.code_card_transaction_group = 'WITHDRAWAL'
      and    t4.dtime_auth_transaction >= t1.sign_date
      where  sign_date >= date '2020-08-01'
      and    name_credit_status not in ('Cancelled')
            --, 'Finished')
      and    not exists (select 1
              from   having_push h
              where  t1.contract_code = h.contract_code
              and    h.flag_bonus_use = 1 --edit 06dec2021 by quynhcao
              )
      group  by t1.contract_code
               ,t1.sign_date
               ,t1.cuid
               ,t1.apt_user
               ,t1.t_amount_usage_bonus
               ,t1.number_trans_bonus
               ,t1.f_use
               ,t1.apt_team
               ,t1.product_gr;
    commit;
  
    execute immediate 'TRUNCATE TABLE TB_TLS_BONUS_PUSH_USAGE';
    insert --+append
    into tb_tls_bonus_push_usage
      select ---+materialize parallel(4)
       date_campaign_start
      ,end_apt_push_usage
      ,text_name_call_list
      ,time_of_call_start
      ,durations
      ,call_type
      ,to_char(cuid) cuid
      ,contract_code
      ,il_communication_id
      ,call_id
      ,username
      ,code_call_list_type
      ,total_amount
      ,case
       
         when product_type like '%VC%'
              and date_campaign_start between date '2021-12-01' and date
          '2022-02-28'
              and total_amount >= 200000 then
          1
         when total_amount >= 500000
              or
              (date_campaign_start >= date '2022-10-01' and num_transaction >= 3) --thuan adjust 7 oct 22
          then
          1
       end flag_usage --edit 06dec2021 by quynhcao
      ,num_transaction
      ,first_usage_date
      ,product_type
      ,apl_sign_dtm
      ,to_char(end_apt_push_usage, 'yyyy-mm') bonus_mth
      ,team
      ,total_transaction
      from   tb_tmp_push_usage
      union
      select ---+materialize parallel(4)
       p.sign_date as date_campaign_start
      ,p.time_of_call_start as end_apt_push_usage
      ,'BOD1_BF_1st_push' as text_name_call_list
      ,p.sign_date as time_of_call_start
      ,0 as durations
      ,'BOD1_BF_1st_push' as call_type
      ,to_char(p.cuid) cuid
      ,p.contract_code
      ,null il_communication_id
      ,'' call_id
      ,p.apt_user username
      ,'' code_call_list_type
      ,total_amount
      ,case
       
         when product_code like '%VC%'
              and sign_date between date '2021-12-01' and date
          '2022-02-28'
              and total_amount >= 200000 then
          1
         when total_amount >= 500000
              or (sign_date >= date '2022-10-01' and num_transaction >= 3) --thuan adjust 7 oct 22
          then
          1
       end flag_usage --edit 06dec2021 by quynhcao
      ,num_transaction
      ,first_usage_date
      ,'CCX' product_type
      ,p.sign_date
      ,to_char(p.sign_date, 'yyyy-mm') bonus_mth
      ,p.apt_team team
      ,p.total_transaction
      from   tb_tmp_bod1_bf_1st_push p
      union
      select ---+materialize parallel(4)
       p.sign_date as date_campaign_start
      ,trunc(p.sign_date) + 45 as end_apt_push_usage
      ,'BOD1' as text_name_call_list
      ,p.sign_date as time_of_call_start
      ,0 as durations
      ,'BOD1' as call_type
      ,to_char(p.cuid) cuid
      ,p.contract_code
      ,null il_communication_id
      ,'' call_id
      ,p.apt_user username
      ,'' code_call_list_type
      ,to_number(total_amount) total_amount
      ,case
         when product_gr like '%Virtual%'
              and sign_date between date '2021-12-01' and date
          '2022-02-28'
              and total_amount >= 200000 then
          1
         when total_amount >= 500000
              or (sign_date >= date '2022-10-01' and num_transaction >= 3) --thuan adjust 7 oct 22
          then
          1
       end flag_usage --edit 06dec2021 by quynhcao
      ,num_transaction
      ,first_usage_date
      ,'CCX' product_type
      ,p.sign_date
      ,to_char(p.sign_date, 'yyyy-mm') bonus_mth
      ,p.apt_team team
      ,p.total_transaction
      from   tb_tmp_bod1 p;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  end;

  ----------------------------tb_tls_push_camp_bonus--------------------------------------------------------

  procedure pr_tls_push_camp_bonus is
  begin
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE PR_TLS_PUSH_CAMP_BONUS');
  
    delete ----+parallel(4)
    tb_tls_push_camp_bonus
    where  bonus_mth >=
           to_char(add_months(trunc(sysdate, 'mm'), -2), 'yyyy-mm'); -- to be modified
  
    commit;
  
    insert ---+append
    into tb_tls_push_camp_bonus
      select ----+materialize parallel(4)
       t.cuid
      ,'CCX_USAGE_PUSH_' || replace(t.bonus_mth, '-', '_') || '_' ||
       row_number() over(partition by bonus_mth order by t.date_campaign_start) as camp_id
      ,t.date_campaign_start as camp_start_date
      ,t.username as push_user
      ,t.time_of_call_start as push_date
      ,t.total_amount as bonus_amt_trans
      ,t.contract_code
      ,sysdate inserted_date
      ,t.bonus_mth
      ,'CCX' as product_gr
      ,t.NUM_TRANSACTION
      from   tb_tls_bonus_push_usage t
      where  t.bonus_mth >=
             to_char(add_months(trunc(sysdate, 'mm'), -2), 'yyyy-mm')
      and    t.username is not null
      and    t.flag_usage = 1
      --and    t.team like 'CCX%'
      ;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
  end;

  ----------------------------tb_tls_mc_contract--------------------------------------------------------

  procedure pr_tls_mc_contract is
  begin
    ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE PR_TLS_MC_CONTRACT');
--    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';
  
    delete ---+parallel(4)
    tb_tls_mc_contract
    where  trunc(signed_date, 'mm') >= add_months(trunc(sysdate, 'mm'), -2);
  
    commit;
  
    insert ---+append
    into tb_tls_mc_contract
    /* with ct as
     (select ---+materialize parallel(4)
       contract_code
      ,cuid
      ,approve_date
      ,sign_date
      ,name_credit_status
      from   tb_tls_appl_clx t -- cash loan
      where  t.preprocess_date >= add_months(trunc(sysdate, 'mm'), -3) - 15
      and    t.sign_date >= add_months(trunc(sysdate, 'mm'), -2)
      and    name_credit_status in ('Active', 'Signed', ' Finished')
            --and    sign_date < date '2021-10-01'
      and    cancel_date is null
      union all
      select ---+materialize parallel(4)
       contract_code
      ,cuid
      ,approve_date
      ,sign_date
      ,name_credit_status
      from   tb_tls_appl_ccx t1 -- credit cards: both ccx, vcc
      where  t1.preprocess_date >= add_months(trunc(sysdate, 'mm'), -3) - 15
      and    t1.sign_date >= add_months(trunc(sysdate, 'mm'), -2)
      and    name_credit_status in ('Active', 'Signed', ' Finished')
      and    sign_date > date '2022-03-31' -- updated on 29/4/2022
      and    cancel_date is null),
    call_push as
     (select ---+materialize parallel(4)
       ct.contract_code
      ,ct.cuid
    
      ,ct.sign_date
      ,ct.approve_date
      ,ct.name_credit_status
      ,gen.username
      ,row_number() over(partition by gen.cuid, ct.contract_code, trunc(ct.sign_date, 'mm') order by gen.time_of_call_end desc) rk
      ,gen.time_of_call_start
      ,gen.time_of_call_end
      ,gen.response_result_2
      from   ct
      join   tb_tls_call_sas gen
      on     gen.cuid = to_char(ct.cuid)
      and    gen.time_of_call_start >= add_months(trunc(sysdate, 'mm'), -3)
      and    gen.time_of_call_start > ct.approve_date
      and    gen.time_of_call_start < ct.sign_date
    
      where  gen.durations >= '15'
      and    lower(gen.response_result_2) = 'sign successful' ----Vi adjusted on  2022-12-13
      and    gen.team in ('QS_Internal', 'External Service'))
    
    select ---+materialize parallel(4)
     time_of_call_start
    ,time_of_call_end
    ,sign_date signed_date
    
    ,cuid
    ,contract_code
    ,username           user_name
    ,name_credit_status
    ,sysdate            inserted_date
    ,approve_date
    from   call_push
    
    where  rk = 1;*/
    
      with ct as
       (
       select ---+materialize parallel(4)
         contract_code
        ,cuid
        ,approve_date
        ,sign_date
        ,name_credit_status
        ,t.product_gr
        from   tb_tls_appl_clx t -- cash loan
        where  t.preprocess_date >= add_months(trunc(sysdate, 'mm'), -3) - 15
        and    t.sign_date >= add_months(trunc(sysdate, 'mm'), -2)
        and    name_credit_status in ('Active', 'Signed', 'Finished')
              --and    sign_date < date '2021-10-01'
        and    cancel_date is null
        union all
        select ---+materialize parallel(4)
         contract_code
        ,cuid
        ,approve_date
        ,sign_date
        ,name_credit_status
        ,t1.product_gr
        from   tb_tls_appl_ccx t1 -- credit cards: both ccx, vcc
        where  t1.preprocess_date >= add_months(trunc(sysdate, 'mm'), -3) - 15
        and    t1.sign_date >= add_months(trunc(sysdate, 'mm'), -2)
        and    name_credit_status in ('Active', 'Signed', 'Finished')
        and    sign_date > date '2022-03-31' -- updated on 29/4/2022
        and    cancel_date is null
--        union all
--        select ---+materialize parallel(4)
--         distinct
--         evid_srv
--        ,cus.cuid
--        ,case when date_approve > date'2300-01-01' 
--              then null else date_approve end as date_approve
--        ,case when date_sign > date'2300-01-01' 
--              then null else date_sign end as date_sign
--        ,case when status = 'A' then 'Active'
--              when status = 'K' then 'Finished'
--              when status = 'N' then 'Signed' end as status
--        from   ap_sales.sa_bonus_temp_contract_2015r t1 -- credit cards: both ccx, vcc
--        join   ap_sales.proc_con_cust cus
--               on cus.skp_client = t1.skp_client
--        where  t1.date_apply >= add_months(trunc(sysdate, 'mm'), -3) - 15
--        and    t1.date_sign >= add_months(trunc(sysdate, 'mm'), -2)
--        and    status in ('A','K','N')
--        and    date_sign > date '2022-03-31' -- updated on 29/4/2022
--        and    date_cancel > date'2300-01-01' 
--        and    product ='BNPL'
        ),
      call_push as
       (select ---+materialize parallel(4)
         ct.contract_code
        ,ct.cuid
         
        ,ct.sign_date
        ,ct.approve_date
        ,ct.name_credit_status
        ,gen.username
        ,row_number() over(partition by gen.cuid, ct.contract_code, trunc(ct.sign_date, 'mm') order by gen.time_of_call_end desc) rk
         --,sum(gen.durations) over(PARTITION BY gen.cuid, gen.username, trunc(gen.time_of_call_start)) sum_durations
        ,gen.time_of_call_start
        ,gen.time_of_call_end
        ,gen.response_result_2
        ,ct.product_gr
        from   ct
        join   tb_tls_call_sas gen
        on     gen.cuid = to_char(ct.cuid)
        and    gen.time_of_call_start >= add_months(trunc(sysdate, 'mm'), -3)
        and    gen.time_of_call_start > (ct.approve_date - INTERVAL '1' MINUTE) -- hoanganh 16.10.2023 from email approve brian (head tls)
        and    gen.time_of_call_start < ct.sign_date
        
        where  1 = 1
              -- and gen.durations >= '15'
        and    lower(nvl(code_disposition,gen.response_result_2)) = 'sign successful' ----Vi adjusted on  2022-12-13
        and    gen.team in ('QS_Internal', 'External Service')),
      total_call as
       (select gen.cuid
              ,ct.contract_code
              ,trunc(ct.sign_date, 'mm') sign_date
              ,sum(gen.durations) sum_durations
        from   ct
        join   tb_tls_call_sas gen
        on     gen.cuid = to_char(ct.cuid)
        and    gen.time_of_call_start >= add_months(trunc(sysdate, 'mm'), -3)
        and    gen.time_of_call_start >  (ct.approve_date - INTERVAL '1' MINUTE) -- hoanganh 16.10.2023 from email approve brian (head tls)
        and    gen.time_of_call_start < ct.sign_date
        where  gen.team in ('QS_Internal', 'External Service')
        group  by gen.cuid
                 ,ct.contract_code
                 ,trunc(ct.sign_date, 'mm')) -- hoanganh 12.09.2023
      
      select ---+materialize parallel(4)
       c.time_of_call_start
      ,c.time_of_call_end
      ,c.sign_date signed_date
       
      ,c.cuid
      ,c.contract_code
      ,c.username           user_name
      ,c.name_credit_status
      ,sysdate              inserted_date
      ,c.approve_date
      --,t.sum_durations
      ,c.product_gr
      from   call_push c
      join   total_call t
      on     c.cuid = t.cuid
      and    c.contract_code = t.contract_code
      and    trunc(c.sign_date, 'mm') = trunc(t.sign_date, 'mm')
      and    t.sum_durations >= 15 -- hoanganh 12.09.2023
      where  rk = 1;
  
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  
  end;
    ----------------------------bnpl--------------------------------------------------------
  procedure pr_tls_bnpl is
  begin
  ap_public.core_log_pkg.pstart(acloginfo => 'UPDATE TB_TLS_PUSH_USAGE_CALL_APPENDS');
  
 execute immediate 'truncate table TB_TLS_PUSH_USAGE_CALL_APPENDS';

 insert into  TB_TLS_PUSH_USAGE_CALL_APPENDS
  with ops as 
     (select distinct
            lower(a.USERNAME)           as USERNAME
      from   tb_tls_input_operator_bonus a
      where  a.product = 'CCX'
      and    a.bonus_term = to_char(add_months(sysdate, -1), 'yyyy-mm')
      and    a.team like 'Ex%') 
      
, call_v_num_days as
       (select /*+ parallel(4) */
        
         t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,t1.record_type
        ,t1.cuid
        ,t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,decode(t1.response_result_2,
                'none',
                t1.code_disposition,
                t1.response_result_2) response_result_2
        ,t1.call_list_gen
        
        from   tb_tls_call_sas t1
        join ops on lower(t1.username) = ops.username
        where  time_of_call_start >=TRUNC(add_months(sysdate,-2), 'MONTH')  - 8 

        and    t1.interaction_type_key <> 3 -- added by quynhcao nov 01 2021
        and    decode(t1.response_result_2,
                      'none',
                      t1.code_disposition,
                      t1.response_result_2) is not null
        and    t1.username is not null
        and    t1.channel = 'CCXP'
        and    t1.call_type = 'Outbound'
        and    (t1.time_of_call_start < date
               '2023-03-16' or t1.time_of_call_start >= date '2023-03-21'))
            
            
 , call_task_bnpl  as (
       select /*+ parallel(4) */
       SubStr(t1.id_source, 0, InStr(t1.id_source, '.') - 1) il_communication_id
      ,t1.skp_client
      ,t1.date_campaign_end
      ,t1.date_campaign_start
      ,t1.code_call_list_type
      ,t1.text_name_call_list
      from   owner_dwh.f_output_load_tt t1
      where  code_output_load_type = 'TLS'
      and    date_communication_start >= last_day(trunc(add_months(sysdate, -3))) + 1
     -- and    t1.DTIME_CREATED >= last_day(trunc(add_months(sysdate, -3))) + 1 ----Vi added on 20221213
            
      and    text_name_call_list like '%TLS_HPL_USAGE%'
      and    nvl(code_call_list_type, 'xxx') != 'GAMI_TRX_LESS500K' --hoa q added 16 mar 23
      and    text_name_call_list != 'ATM USAGE PUSH'
    )       
    
       ,call_push as
       (select --+materialize parallel(4)
         t2.date_campaign_start
        ,t2.date_campaign_end
        ,t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,t1.record_type
        ,t1.cuid
        ,t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,t1.response_result_2
        ,t1.call_list_gen
        ,t2.text_name_call_list
        ,skp_client
        ,t2.code_call_list_type
        from   call_v_num_days t1
        join   call_task_bnpl t2 --tb_tls_call_task_push_usage
        on     t1.il_communication_id = t2.il_communication_id)
      
      ,call_push_usage_agree as
       (select ---+materialize parallel(4)
         date_campaign_start
        ,date_campaign_end
        ,text_name_call_list
        ,time_of_call_start
        ,time_of_call_end
        ,durations
        ,case
           when call_list_gen = 'SAS_TLS_OUTBOUND_CCX_USAGE_PUSH' then
            '1st call'
         end call_type
        ,record_type
        ,cuid
        ,il_communication_id
        ,call_id
        ,username
        ,skp_client
        ,code_call_list_type
        from   call_push
        where  response_result_2 in ('Push Use - Client Agrees', 'Use Agree','Client agrees') -- Add client agree from Nov 1st - Duy Pham
        and    durations >= 15
        and    il_communication_id is not null
        
        union
        
        select ---+materialize parallel(4)
         date_campaign_start
        ,date_campaign_end
        ,t2.text_name_call_list
        ,t1.time_of_call_start
        ,t1.time_of_call_end
        ,t1.durations
        ,case
           when t1.il_communication_id is not null
                and t1.call_list_gen =
                'SAS_RTDM_TLS_OUTBOUND_CCX_USAGE_PUSH_CALLBACK' then
           
            'Call back RTDM'
         
           when t1.il_communication_id is null then
            'Call back Personal'
         end call_type
        ,t1.record_type
        ,t1.cuid
        ,t2.il_communication_id --t1.il_communication_id
        ,t1.call_id
        ,t1.username
        ,skp_client
        ,code_call_list_type
        from   call_v_num_days t1
        join   call_push t2
        on     t1.cuid = t2.cuid
        and    t1.time_of_call_start > t2.time_of_call_start
        and    t1.time_of_call_start < trunc(t2.time_of_call_start) + 4
        where  1 = 1
        and    t1.response_result_2 in ('Push Use - Client Agrees', 'Use Agree','Client agrees') -- Add client agree from Nov 1st - Duy Pham
        and    t1.durations >= 15 -----edited by quynhcao 02 nov2021
        ),
      contract as
       (select ---+materialize parallel(4)
         a2.ID_CUID
        ,a2.NUM_APL_CONTRACT
        ,a2.CODE_APL_PROD_GR
        ,case
           when a2.DTIME_APL_SIGN = date '3000-01-01' then
            null
           else
            a2.DTIME_APL_SIGN
         end DTIME_APL_SIGN -- hoanganh 26/04/2023
        ,nvl(a2.DTIME_APL_CLOSE, date '3000-01-01') apl_close_dtm
        from   dm_crm_rep.f_crm_appl a2 ---ap_crm.tb_crm_appl a2
        where  CODE_APL_PROD_GR like 'BNPL%' -- hoanganh 13.09.2023=
        and    decode(a2.DTIME_APL_SIGN,
                      date '3000-01-01',
                      null,
                      a2.DTIME_APL_SIGN) is not null
        and    (a2.DTIME_APL_CANCEL = date '3000-01-01') -- hoanganh 26/04/2023
        
        )
    
      select ---+materialize parallel(4)
       t1.date_campaign_start
      ,t1.date_campaign_end
      ,t1.text_name_call_list
      ,t1.time_of_call_start
      ,t1.time_of_call_end
      ,t1.durations
      ,t1.call_type
      ,t1.record_type
      ,t1.cuid
      ,t1.il_communication_id
      ,t1.call_id
      ,t1.username
      ,t1.skp_client
      ,t1.code_call_list_type
       
      ,t2.NUM_APL_CONTRACT as contract_code
      ,case
         when t2.CODE_APL_PROD_GR like 'BNPL%' then
          'BNPL'
         else
          'OTHER'
       end product_type ---edited by quynhcao 05jan2022
      ,t2.DTIME_APL_SIGN as apl_sign_dtm
      from   call_push_usage_agree t1
      left   join contract t2
      on     t1.cuid = t2.id_cuid
      and    t1.time_of_call_start < t2.apl_close_dtm
      and    t1.time_of_call_start > t2.DTIME_APL_SIGN
      
      group  by t1.date_campaign_start
               ,t1.date_campaign_end
               ,t1.text_name_call_list
               ,t1.time_of_call_start
               ,t1.time_of_call_end
               ,t1.durations
               ,t1.call_type
               ,t1.record_type
               ,t1.cuid
               ,t1.il_communication_id
               ,t1.call_id
               ,t1.username
               ,t1.skp_client
               ,t1.code_call_list_type
                
               ,t2.NUM_APL_CONTRACT
               ,case
                  when t2.CODE_APL_PROD_GR like 'BNPL%' then
                   'BNPL'
                  else
                   'OTHER'
                end ---edited by quynhcao 05jan2022
               ,t2.DTIME_APL_SIGN ;     
    commit;
    
    execute immediate 'truncate table tb_tls_bonus_push_usage_bnpl';
    commit;
    insert into tb_tls_bonus_push_usage_bnpl
     with push as
       (select ---+materialize parallel(4)
         t.date_campaign_start
        ,t.date_campaign_end
        ,t.text_name_call_list
        ,t.time_of_call_start
        ,t.time_of_call_end
        ,t.durations
        ,t.call_type
        ,t.record_type
        ,t.cuid
        ,t.il_communication_id
        ,t.call_id
        ,t.username
        ,t.skp_client
        ,t.code_call_list_type
        ,t.contract_code
        ,t.product_type
        ,t.apl_sign_dtm
        ,row_number() over(partition by t.il_communication_id order by time_of_call_start desc) rn
         
        ,date_campaign_start + 8 end_apt_push_usage
        ,min(time_of_call_start) over(partition by t.il_communication_id) call_1st ---editted by quynhcao 02 nov2021
        from   tb_tls_push_usage_call_appends t)

      ,emp as
       (select username
              ,team
              ,date_valid_from
              ,case
                 when date_valid_to is null then
                  sysdate
                 else
                  date_valid_to
               end date_valid_to
              ,channel
        from   tb_tls_employee users
        where  channel in ('CCX', 'CCXP')),
      map_transaction as
       (select ---+materialize parallel(4)
        distinct t1.product_type
                ,t1.date_campaign_start
                ,t1.end_apt_push_usage
                ,t1.text_name_call_list
                ,t1.code_call_list_type
                ,t1.time_of_call_start
                ,t1.durations
                ,t1.call_type
                ,t1.cuid
                ,t1.il_communication_id
                ,t1.call_id
                ,t1.username
                ,t4.name_card_acceptor
                ,t4.amt_billing
                ,t4.date_transaction
                ,t1.contract_code
                ,t1.apl_sign_dtm
        from   push t1
        left   join owner_dwh.dc_contract t2
        on     t1.contract_code = t2.text_contract_number
        left   join owner_dwh.dc_card_account t3
        on     t3.skp_contract = t2.skp_contract
        left   join owner_dwh.f_card_transaction_tt t4
        on     t3.skp_card_account = t4.skp_card_account
        and    t4.date_transaction > t1.call_1st
        and    t4.date_transaction <= t1.call_1st + 8
        and    t4.code_card_transaction_group = 'WITHDRAWAL'
        and    t4.date_transaction >= t1.apl_sign_dtm
        and     t4.SKP_CARD_TRANSACTION_TYPE = '1711699'
        and t4.code_status != 'n' 
        where  rn = 1)
    
        ,tb_tmp_push_usage_bnpl as (
      select ---+materialize parallel(4)
       t1.product_type
      ,t1.date_campaign_start
      ,t1.end_apt_push_usage
      ,t1.text_name_call_list
      ,t1.code_call_list_type
      ,t1.time_of_call_start
      ,t1.durations
      ,t1.call_type
      ,t1.cuid
      ,t1.il_communication_id
      ,t1.call_id
      ,t1.username
      ,sum(amt_billing) total_amount
      ,count(case
               when date_transaction < date '2022-10-01' then
                date_transaction
               when date_transaction >= date
                '2022-10-01'
                    and (Upper(t1.name_card_acceptor) like '%FOODY%' or
                    Upper(t1.name_card_acceptor) like '%SHOPEE%' or
                    Upper(t1.name_card_acceptor) like '%LAZADA%' or
                    Upper(t1.name_card_acceptor) like '%TIKI%' or
                    (Upper(t1.name_card_acceptor) like '%SENDO%' and
                    Upper(t1.name_card_acceptor) not like '%SENDONGA%') or
                    Upper(t1.name_card_acceptor) like '%GRAB%' or
                    (Upper(t1.name_card_acceptor) like '%BE GROUP%' and
                    Upper(t1.name_card_acceptor) != 'CNBENTRECTYKAYGROUP') or
                    Upper(t1.name_card_acceptor) like '%BAE%MIN%' or
                    Upper(t1.name_card_acceptor) like '%ZALOPAY%' or
                    Upper(t1.name_card_acceptor) like '%MOMO%' or
                    Upper(t1.name_card_acceptor) like '%GOOGLE%' or
                    Upper(t1.name_card_acceptor) like '%APPLE.COM/BILL%' or
                    Upper(t1.name_card_acceptor) like '%NETFLIX%' or
                    Upper(t1.name_card_acceptor) like '%TRAVELOKA%' or
                    Upper(t1.name_card_acceptor) like '%AGODA%' or
                    Upper(t1.name_card_acceptor) like '%BOOKING%COM%') then
                t1.date_transaction
               else
                null
             end) num_transaction_18 --thuan adjust 7 oct 22
      ,min(date_transaction) first_usage_date
      ,t1.contract_code
      ,t1.apl_sign_dtm
      ,emp.team
      ,count(date_transaction) total_transaction
      from   map_transaction t1
      left   join emp
      on     t1.username = emp.username
      and    t1.time_of_call_start >= emp.date_valid_from
      and    t1.time_of_call_start < nvl(emp.date_valid_to, sysdate) + 1
      group  by t1.date_campaign_start
               ,t1.text_name_call_list
               ,t1.time_of_call_start
               ,t1.durations
               ,t1.call_type
               ,t1.cuid
               ,t1.il_communication_id
               ,t1.call_id
               ,t1.username
               ,t1.end_apt_push_usage
               ,code_call_list_type
               ,t1.contract_code
               ,t1.product_type
               ,t1.apl_sign_dtm
               ,emp.team )
               
    ,push_usage_bnpl as (
    
     select ---+materialize parallel(4)
       date_campaign_start
      ,end_apt_push_usage
      ,text_name_call_list
      ,time_of_call_start
      ,durations
      ,call_type
      ,to_char(cuid) cuid
      ,contract_code
      ,il_communication_id
      ,call_id
      ,username
      ,code_call_list_type
      ,total_amount
      ,case
       
         when 
               total_amount >= 500000 then
          1
         when total_transaction >= 3 then
          1
       end flag_usage --edit 06dec2021 by quynhcao
      ,num_transaction_18
      ,first_usage_date
      ,product_type
      ,apl_sign_dtm
      ,to_char(end_apt_push_usage, 'yyyy-mm') bonus_mth
      ,team
      ,total_transaction
      from   tb_tmp_push_usage_bnpl)
      
      select * from push_usage_bnpl ;
      commit;
       
  insert into tb_tls_push_camp_bonus  
    select ----+materialize parallel(4)
       t.cuid
      ,'CCX_USAGE_PUSH_' || replace(t.bonus_mth, '-', '_') || '_' ||
       row_number() over(partition by bonus_mth order by t.date_campaign_start) as camp_id
      ,t.date_campaign_start as camp_start_date
      ,lower(t.username) as push_user
      ,t.time_of_call_start as push_date
      ,t.total_amount as bonus_amt_trans
      ,t.contract_code
      ,sysdate inserted_date
      ,t.bonus_mth
      ,'BNPL' as product_gr
      ,total_transaction as NUM_TRANSACTION
      from   tb_tls_bonus_push_usage_bnpl t 
      where  username is not null
      and    flag_usage = 1
      and bonus_mth >=  to_char(add_months(sysdate, -2), 'yyyy-mm') ;
      
    ap_public.core_log_pkg.pend(andmlrows => sql%rowcount);
    commit;
  end;
      
  procedure update_bonus is
  begin
  
    ap_public.core_log_pkg.pinit(aclogmodule  => 'AP_TLS',
                                 aclogprocess => 'PK_TLS_BONUS',
                                 adworkdate   => sysdate);
    pr_tls_connect;
    pr_tls_mobile_app;
    pr_tls_bonus_sending_lead;
    pr_tls_bonus_pushing_sign;
    pr_tls_push_usage_call;
    pr_tls_push_usage_bonus;
    pr_tls_push_camp_bonus;
    pr_tls_mc_contract;
    pr_tls_bnpl;
    ap_public.core_log_pkg.pfinish;
  
    
EXCEPTION
                WHEN OTHERS 
                THEN AP_PUBLIC.CORE_LOG_PKG.pError;
                ROLLBACK;
                RAISE;
    
  end;
end pk_tls_bonus;
