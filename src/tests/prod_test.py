import os

import pytest

if not os.environ.get('RECO_ENVIRONMENT'):
    os.environ['RECO_ENVIRONMENT'] = 'prod'

os.environ['OFFLINE_BACKEND_ADDR'] = ''
os.environ['LOWCODE_URL'] = ''
os.environ['REDIS_HOST'] = ''
os.environ['REDIS_PORT'] = '0'
os.environ['REDIS_PASSWORD'] = ''


@pytest.mark.skipif(
    os.environ['RECO_ENVIRONMENT'] == 'local', reason='只有 pytest --noconftest 时才可测试',
)
def test_prod_config():
    from configs import load_or_init_dag_cfg
    from core.utils import new_tmp_file

    tmppath = new_tmp_file()
    load_or_init_dag_cfg(tmppath)
    aio_config = load_or_init_dag_cfg(tmppath)
    base_config = aio_config.base_dag.base
    if base_config:
        assert base_config.hdfs_output_dir == '/user/tech/prod/reco_results'
        assert base_config.nas_output_dir == '/tianrang/reco_results'
        assert base_config.feat_engines[0].feat_dir == '/user/tech/prod/feature'
        assert base_config.offline_backend_address == 'http://bdasire-offline-reco-api-prod:5000'

        assert base_config.external_table.cre_crd_user == 'apprbw.rbw_cstpro_ind_cre_crd'
        assert base_config.external_table.deb_crd_user == 'apprbw.rbw_cstpro_ind_deb_crd'
        assert base_config.external_table.cust_act_label == 'appcha.cha_gj_pmbs_cust_act_label'
        assert base_config.external_table.user_tel_number == 'appims.ims_cust_phoneno'
        assert base_config.external_table.sms_feedback == 'shdata.s35_edb_sms_outslog'
        assert base_config.external_table.app_feedback == 'shdata.s70_event'
        assert base_config.external_table.app_action == 'appcha.p_bh_logon_contact_act'
        assert base_config.external_table.user_feat_ls_crd == 'apprbw.rbw_lsyb_ls_crd'
        # TODO(ryang): add more test
        assert base_config.external_table.hx_cancel_log == 'appmis.mis_f_card_bscmtn_info'
        assert base_config.external_table.hx_cancel_user == 'appmis.mis_f_cstbsc_info'
        assert base_config.external_table.mt_cancel_user == 'appmis.mis_f_crdbsc_info_mt'
        assert base_config.external_table.can_acct_type == 'appmis.e_mis_can_acct_type_to_bi'
        assert base_config.external_table.user_stat_mt == 'apprbw.e_rbw_cust_camp_popupw_stat_mt'
        assert base_config.external_table.custom_data == 'shdata.s70_custom_data_json'
        assert base_config.external_table.s21_ecusrdevice == 'shdata.s21_ecusrdevice'
        assert base_config.external_table.s21_ecusr_h == 'shdata.s21_ecusr_h'
        assert base_config.external_table.s21_ecextcifno_h == 'shdata.s21_ecextcifno_h'
        assert base_config.external_table.dim_jydm == 'appmis.mis_dim_jydm'
        assert base_config.partition_keep_day == 90
        assert base_config.offline_backend_address == 'http://bdasire-offline-reco-api-prod:5000'
        assert base_config.test_banners == []

        assert aio_config.reco_dags[0].scene.train.sample_dt == None
        # assert aio_config.reco_dags[0].scene.train.sample_prt == '20220630'

        assert aio_config.reco_dags[0].scene.ranking.model.model_dir == '/user/tech/prod/model'

        assert aio_config.reco_dags[0].scene.crowd_feedback.sms_default_send_num == 0
        assert aio_config.reco_dags[0].scene.user_pool.user_type_refresh_days == [1]
        assert aio_config.post_proc_dags[0].sms
        assert aio_config.post_proc_dags[0].sms.push_days == [1, 20]
