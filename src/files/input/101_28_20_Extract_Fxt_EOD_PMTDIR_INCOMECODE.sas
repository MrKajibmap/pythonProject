/**************************************************************************** 
 * Job:             101_28_20_Extract_Fxt_EOD_PMTDIR_INC  A52HJCF1.AR0000B8 * 
 *                  OMECODE                                                 * 
 * Description:                                                             * 
 *                                                                          * 
 * Metadata Server: sasmeta4.dwhgridtest.imb.ru                             * 
 * Port:            8564                                                    * 
 * Location:        /DWH_DWHDDS_STG/Jobs/101_Extract/101_28_FXT             * 
 *                                                                          * 
 * Server:          SASApp                                A5H5E9KJ.AS000002 * 
 *                                                                          * 
 * Source Tables:   VEOD_PMTDIR_INCOMECODE -              A52HJCF1.AH0001XU * 
 *                   FXT.VEOD_PMTDIR_INCOMECODE                             * 
 *                  VEOD_PMTDIR_INCOMECODE -              A52HJCF1.AH0001XU * 
 *                   FXT.VEOD_PMTDIR_INCOMECODE                             * 
 * Target Table:    EOD_PMTDIR_INCOMECODE_FULL -          A52HJCF1.AH0002XZ * 
 *                   work_stg.EOD_PMTDIR_INCOMECODE_FULL                    * 
 *                                                                          * 
 * Generated on:    22 ���� 2021 �. 12:14:01 GMT+03:00                      * 
 * Generated by:    mb35094@imb.ru                                          * 
 * Version:         SAS Data Integration Studio 4.904                       * 
 ****************************************************************************/ 

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A52HJCF1.AR0000B8);
%let etls_jobName = %nrquote(101_28_20_Extract_Fxt_EOD_PMTDIR_INCOMECODE);
%let etls_userID = %nrquote(mb35094@imb.ru);

/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%global etls_stepStartTime; 
/* initialize syserr to 0 */ 
data _null_; run;

%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet; 

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 

/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8564);
%let metaServer     = %nrquote(sasmeta4.dwhgridtest.imb.ru);

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;

%global etls_debug; 
%macro etls_setDebug; 
   %if %str(&etls_debug) ne 0 %then 
      OPTIONS MPRINT%str(;); 
%mend; 
%etls_setDebug; 

/*==========================================================================* 
 * Step:            ������ ������                         A52HJCF1.AT002F48 * 
 * Transform:       ������ ������                                           * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F48);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 0; 
%let _OUTPUT_count = 0; 


/*****************************************************************
*  ������:
*     $Id: transform_job_start.sas 4060:d0a33a5b822b 2015-06-19 09:27:21Z rusane $
*
******************************************************************
*  �����ǵ���:
*     ������������ ������ ������ ETL.
*
*  ���������:
*     ���
*
******************************************************************/

%macro transform_job_start;
   %etl_job_start;
%mend transform_job_start;

%transform_job_start;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/** Step end ������ ������ **/

/*==========================================================================* 
 * Step:            ��������� ������ �������              A52HJCF1.AT002F49 * 
 * Transform:       ��������� ������ �������                                * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    VEOD_PMTDIR_INCOMECODE -              A52HJCF1.AH0001XU * 
 *                   FXT.VEOD_PMTDIR_INCOMECODE                             * 
 * Target Table:    ��������� ������ �������tpOutRegistry A52HJCF1.AQ000NFZ * 
 *                   - work.W1DNEH0Z                                        * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F49);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

/* Access the data for FXT  */ 
LIBNAME FXT ORACLE  PATH="PAYHUB01.IMB.RU"  SCHEMA=PMH  AUTHDOMAIN="OraAuth_PAYHUB2DWH_PAYHUB01_T" ;
%rcSet(&syslibrc); 

%let SYSLAST = %nrquote(FXT.VEOD_PMTDIR_INCOMECODE); 

%let _INPUT_count = 1; 
%let _INPUT = FXT.VEOD_PMTDIR_INCOMECODE;
%let _INPUT_connect =  PATH="PAYHUB01.IMB.RU" AUTHDOMAIN="OraAuth_PAYHUB2DWH_PAYHUB01_T" 
;
%let _INPUT_engine = ORACLE;
%let _INPUT_memtype = VIEW;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/DWH_DWHDDS_STG/Tables/FXT/VEOD_PMTDIR_INCOMECODE%(Table%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let tpIn = FXT.VEOD_PMTDIR_INCOMECODE;
%let tpIn_connect =  PATH="PAYHUB01.IMB.RU" AUTHDOMAIN="OraAuth_PAYHUB2DWH_PAYHUB01_T" 
;
%let tpIn_engine = ORACLE;
%let tpIn_memtype = VIEW;
%let tpIn_options = %nrquote();
%let tpIn_alter = %nrquote();
%let tpIn_path = %nrquote(/DWH_DWHDDS_STG/Tables/FXT/VEOD_PMTDIR_INCOMECODE%(Table%));
%let tpIn_type = 1;
%let tpIn_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = work.W1DNEH0Z;
%let _OUTPUT_connect = ;
%let _OUTPUT_engine = ;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
%let _OUTPUT_path = %nrquote(/��������� ������ �������tpOutRegistry_A52HJCF1.AQ000NFZ%(WorkTable%));
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = RESOURCE_ID VERSION_ID AVAILABLE_DTTM;

%let tpOutRegistry = work.W1DNEH0Z;
%let tpOutRegistry_connect = ;
%let tpOutRegistry_engine = ;
%let tpOutRegistry_memtype = DATA;
%let tpOutRegistry_options = %nrquote();
%let tpOutRegistry_alter = %nrquote();
%let tpOutRegistry_path = %nrquote(/��������� ������ �������tpOutRegistry_A52HJCF1.AQ000NFZ%(WorkTable%));
%let tpOutRegistry_type = 1;
%let tpOutRegistry_label = %nrquote();
/* List of target columns to keep  */ 
%let tpOutRegistry_keep = RESOURCE_ID VERSION_ID AVAILABLE_DTTM;


proc datasets lib=work nolist nowarn memtype = (data view);
   delete W1DNEH0Z;
quit;

%let tpResourceId = %nrquote(BY_SOURCE);
%let tpVersion = %nrquote(MIN);

/* List of target columns to keep  */ 
%let _keep = RESOURCE_ID VERSION_ID AVAILABLE_DTTM;
/* List of target columns to keep  */ 
%let keep = RESOURCE_ID VERSION_ID AVAILABLE_DTTM;
/*****************************************************************
*  ������:
*     $Id: transform_registry_get.sas $
*
******************************************************************
*  �����ǵ���:
*     �������� ������ ������� ��� ��������� ������� � ������.
*     ������������ ���������������, ��������������� ����� ������
*
*  ���������:
*     tpIn            +  ��� �������� ������, ����������� �������
*     tpResourceId    +  ������������� �������
*                        �� ��������� BY_SOURCE, �.�. ����� ��������� �� ������� �������
*     tpVersion       +  ������������� ������
*                        �� ��������� MIN, �.�. ����� ������ �� ���������
*     tpOutRegistry   +  ��� ��������� ������, ������� ODS
*
******************************************************************
*  �����̷õ�:
*     %etl_extract_rsrc_registry_get
*
*  ��°��������� �������������˵:
*     ETL_RSRC_ID
*     ETL_RSRC_VERSION_ID
*     ETL_RSRC_AVAILABLE_DTTM
******************************************************************/

%macro transform_registry_get;

   %global ETL_RSRC_ID;
   %let ETL_RSRC_ID=;

   %global ETL_RSRC_VERSION_ID;
   %let ETL_RSRC_VERSION_ID=;

   %global ETL_RSRC_AVAILABLE_DTTM;
   %let ETL_RSRC_AVAILABLE_DTTM=;

   %etl_extract_common (
      mpData            = &tpIn,
      mpResourceId      = &tpResourceId,
      mpVersion         = &tpVersion,
      mpOutRegistry     = &tpOutRegistry,
      mpOutResourceKey  = ETL_RSRC_ID,
      mpOutVersionKey   = ETL_RSRC_VERSION_ID,
      mpOutAvailableKey = ETL_RSRC_AVAILABLE_DTTM
   );

%mend transform_registry_get;

%transform_registry_get;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/** Step end ��������� ������ ������� **/

/*==========================================================================* 
 * Step:            ��������� ���������������             A52HJCF1.AT002F4A * 
 * Transform:       ��������� ���������������                               * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    ��������� ������ �������tpOutRegistry A52HJCF1.AQ000NFZ * 
 *                   - work.W1DNEH0Z                                        * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F4A);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(work.W1DNEH0Z); 

%let _INPUT_count = 1; 
%let _INPUT = work.W1DNEH0Z;
%let _INPUT_connect = ;
%let _INPUT_engine = ;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/��������� ������ �������tpOutRegistry_A52HJCF1.AQ000NFZ%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let tpIn = work.W1DNEH0Z;
%let tpIn_connect = ;
%let tpIn_engine = ;
%let tpIn_memtype = DATA;
%let tpIn_options = %nrquote();
%let tpIn_alter = %nrquote();
%let tpIn_path = %nrquote(/��������� ������ �������tpOutRegistry_A52HJCF1.AQ000NFZ%(WorkTable%));
%let tpIn_type = 1;
%let tpIn_label = %nrquote();

%let _OUTPUT_count = 0; 

%let tpMacroFields = RESOURCE_ID AVAILABLE_DTTM;
%let tpMacroFields_count = 2;
%let tpMacroFields0 = 2;
%let tpMacroFields1 = RESOURCE_ID;
%let tpMacroFields2 = AVAILABLE_DTTM;

/*****************************************************************
* ������:
*   $Id: transform_macro_param.sas 3478 2012-10-09 09:23:53Z nesterenok $
*
******************************************************************
* �����ǵ���:
*   ������������� ��������������� �� ������ ������ �������, ���� <var_name> = <var_value>.
*   ���� ������� �����, �� ��������������� ������ ������ ("" � .) ��������.
*
* ���������:
*   tpIn             + ��� �������� ������
*   tpMacroFields    + ������ ����� ��� ��������� ���������������
*
******************************************************************/

%macro transform_macro_param;
   %macro _transform_macro_param_loop (var);
      call symput ("&var", cats(&var));
   %mend _transform_macro_param_loop;

   data _null_;
      /* ������������� */
      if 0 then set &tpIn;
      %util_loop (mpMacroName=_transform_macro_param_loop, mpWith=&tpMacroFields);

      /* ��������� �������� */
      set &tpIn (obs=1);
      %util_loop (mpMacroName=_transform_macro_param_loop, mpWith=&tpMacroFields);
   run;
   %error_check (mpStepType=DATA);

%mend transform_macro_param;

%transform_macro_param;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/** Step end ��������� ��������������� **/

/*==========================================================================* 
 * Step:            �������� ������                       A52HJCF1.AT002F4B * 
 * Transform:       �������� ������                                         * 
 * Description:     ��������� ����� �� ���������, ������������ MD5-�����    * 
 *                   �����.                                                 * 
 *                                                                          * 
 * Source Table:    VEOD_PMTDIR_INCOMECODE -              A52HJCF1.AH0001XU * 
 *                   FXT.VEOD_PMTDIR_INCOMECODE                             * 
 * Target Table:    �������� ������tpOutData -            A52HJCF1.AQ000NG0 * 
 *                   work.W1DMWR5N                                          * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F4B);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(FXT.VEOD_PMTDIR_INCOMECODE); 

%let _INPUT_count = 1; 
%let _INPUT = FXT.VEOD_PMTDIR_INCOMECODE;
%let _INPUT_connect =  PATH="PAYHUB01.IMB.RU" AUTHDOMAIN="OraAuth_PAYHUB2DWH_PAYHUB01_T" 
;
%let _INPUT_engine = ORACLE;
%let _INPUT_memtype = VIEW;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/DWH_DWHDDS_STG/Tables/FXT/VEOD_PMTDIR_INCOMECODE%(Table%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let tpIn = FXT.VEOD_PMTDIR_INCOMECODE;
%let tpIn_connect =  PATH="PAYHUB01.IMB.RU" AUTHDOMAIN="OraAuth_PAYHUB2DWH_PAYHUB01_T" 
;
%let tpIn_engine = ORACLE;
%let tpIn_memtype = VIEW;
%let tpIn_options = %nrquote();
%let tpIn_alter = %nrquote();
%let tpIn_path = %nrquote(/DWH_DWHDDS_STG/Tables/FXT/VEOD_PMTDIR_INCOMECODE%(Table%));
%let tpIn_type = 1;
%let tpIn_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = work.W1DMWR5N;
%let _OUTPUT_connect = ;
%let _OUTPUT_engine = ;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
%let _OUTPUT_path = %nrquote(/�������� ������tpOutData_A52HJCF1.AQ000NG0%(WorkTable%));
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = INCOMECODENUMBER INCOMECODENAME SOURCE_SYSTEM_CD ETL_EXTRACT_ID;
%let _OUTPUT_col_count = 4;
%let _OUTPUT_col0_name = INCOMECODENUMBER;
%let _OUTPUT_col0_table = work.W1DMWR5N;
%let _OUTPUT_col0_length = 5;
%let _OUTPUT_col0_type = $;
%let _OUTPUT_col0_format = $5.;
%let _OUTPUT_col0_informat = $5.;
%let _OUTPUT_col0_label = %nrquote(INCOMECODENUMBER);
%let _OUTPUT_col0_input0 = INCOMECODENUMBER;
%let _OUTPUT_col0_input0_table = FXT.VEOD_PMTDIR_INCOMECODE;
%let _OUTPUT_col0_exp = ;
%let _OUTPUT_col0_input = INCOMECODENUMBER;
%let _OUTPUT_col0_input_count = 1;
%let _OUTPUT_col1_name = INCOMECODENAME;
%let _OUTPUT_col1_table = work.W1DMWR5N;
%let _OUTPUT_col1_length = 255;
%let _OUTPUT_col1_type = $;
%let _OUTPUT_col1_format = $255.;
%let _OUTPUT_col1_informat = $255.;
%let _OUTPUT_col1_label = %nrquote(INCOMECODENAME);
%let _OUTPUT_col1_input0 = INCOMECODENAME;
%let _OUTPUT_col1_input0_table = FXT.VEOD_PMTDIR_INCOMECODE;
%let _OUTPUT_col1_exp = ;
%let _OUTPUT_col1_input = INCOMECODENAME;
%let _OUTPUT_col1_input_count = 1;
%let _OUTPUT_col2_name = SOURCE_SYSTEM_CD;
%let _OUTPUT_col2_table = work.W1DMWR5N;
%let _OUTPUT_col2_length = 3;
%let _OUTPUT_col2_type = $;
%let _OUTPUT_col2_format = $3.;
%let _OUTPUT_col2_informat = ;
%let _OUTPUT_col2_label = %nrquote(SOURCE_SYSTEM_CD);
%let _OUTPUT_col2_exp = ;
%let _OUTPUT_col2_input_count = 0;
%let _OUTPUT_col3_name = ETL_EXTRACT_ID;
%let _OUTPUT_col3_table = work.W1DMWR5N;
%let _OUTPUT_col3_length = 8;
%let _OUTPUT_col3_type = ;
%let _OUTPUT_col3_format = ;
%let _OUTPUT_col3_informat = ;
%let _OUTPUT_col3_label = %nrquote(ETL_EXTRACT_ID);
%let _OUTPUT_col3_exp = ;
%let _OUTPUT_col3_input_count = 0;

%let tpOutData = work.W1DMWR5N;
%let tpOutData_connect = ;
%let tpOutData_engine = ;
%let tpOutData_memtype = DATA;
%let tpOutData_options = %nrquote();
%let tpOutData_alter = %nrquote();
%let tpOutData_path = %nrquote(/�������� ������tpOutData_A52HJCF1.AQ000NG0%(WorkTable%));
%let tpOutData_type = 1;
%let tpOutData_label = %nrquote();
/* List of target columns to keep  */ 
%let tpOutData_keep = INCOMECODENUMBER INCOMECODENAME SOURCE_SYSTEM_CD ETL_EXTRACT_ID;
%let tpOutData_col_count = 4;
%let tpOutData_col0_name = INCOMECODENUMBER;
%let tpOutData_col0_table = work.W1DMWR5N;
%let tpOutData_col0_length = 5;
%let tpOutData_col0_type = $;
%let tpOutData_col0_format = $5.;
%let tpOutData_col0_informat = $5.;
%let tpOutData_col0_label = %nrquote(INCOMECODENUMBER);
%let tpOutData_col0_input0 = INCOMECODENUMBER;
%let tpOutData_col0_input0_table = FXT.VEOD_PMTDIR_INCOMECODE;
%let tpOutData_col0_exp = ;
%let tpOutData_col0_input = INCOMECODENUMBER;
%let tpOutData_col0_input_count = 1;
%let tpOutData_col1_name = INCOMECODENAME;
%let tpOutData_col1_table = work.W1DMWR5N;
%let tpOutData_col1_length = 255;
%let tpOutData_col1_type = $;
%let tpOutData_col1_format = $255.;
%let tpOutData_col1_informat = $255.;
%let tpOutData_col1_label = %nrquote(INCOMECODENAME);
%let tpOutData_col1_input0 = INCOMECODENAME;
%let tpOutData_col1_input0_table = FXT.VEOD_PMTDIR_INCOMECODE;
%let tpOutData_col1_exp = ;
%let tpOutData_col1_input = INCOMECODENAME;
%let tpOutData_col1_input_count = 1;
%let tpOutData_col2_name = SOURCE_SYSTEM_CD;
%let tpOutData_col2_table = work.W1DMWR5N;
%let tpOutData_col2_length = 3;
%let tpOutData_col2_type = $;
%let tpOutData_col2_format = $3.;
%let tpOutData_col2_informat = ;
%let tpOutData_col2_label = %nrquote(SOURCE_SYSTEM_CD);
%let tpOutData_col2_exp = ;
%let tpOutData_col2_input_count = 0;
%let tpOutData_col3_name = ETL_EXTRACT_ID;
%let tpOutData_col3_table = work.W1DMWR5N;
%let tpOutData_col3_length = 8;
%let tpOutData_col3_type = ;
%let tpOutData_col3_format = ;
%let tpOutData_col3_informat = ;
%let tpOutData_col3_label = %nrquote(ETL_EXTRACT_ID);
%let tpOutData_col3_exp = ;
%let tpOutData_col3_input_count = 0;


proc datasets lib=work nolist nowarn memtype = (data view);
   delete W1DMWR5N;
quit;

%let tpSrcSystem = %nrquote(FXT);
%let tpWhere = ;
%let tpCreateDigest = %nrquote(Yes);
%let tpResourceId = %nrquote(BY_SOURCE);
%let tpVersion = %nrquote(MIN);

/* List of target columns to keep  */ 
%let _keep = INCOMECODENUMBER INCOMECODENAME SOURCE_SYSTEM_CD ETL_EXTRACT_ID;
/* List of target columns to keep  */ 
%let keep = INCOMECODENUMBER INCOMECODENAME SOURCE_SYSTEM_CD ETL_EXTRACT_ID;
/*****************************************************************
*  ������:
*     $Id: transform_extract.sas 2953:806283d8673a 2014-05-21 06:26:42Z rusane $
*
******************************************************************
*  �����ǵ���:
*     ��������� ����� �� ���������, ������������ MD5-����� �����.
*     ����������� ��� ����, ��� ������� ��������� �������.
*
*  ���������:
*     tpIn                    +  ��� �������� ������, ������� �� ���������
*     tpOutData               +  ��� ��������� ������, ����������� �������
*     tpOutRegistry           -  ��� ��������� ������, ������ ����������� ������� � �������
*     tpSrcSystem             +  ��� ���������
*     tpResourceId            +  ������������� �������
*                                �� ��������� BY_SOURCE, �.�. ����� ��������� �� ������� �������
*     tpVersion               +  ������������� ������
*                                �� ��������� MIN, �.�. ����� ������ �� ���������
*     tpWhere                 -  ������� ������ �� �������� ������
*     tpCreateDigest          -  ��������� (Yes) ��� ��� (No) ���� � ������������ ���-������ (ETL_DIGEST_CD)
*
******************************************************************
*  �����������:
*     ���� �������� ��������� ����� 1 ������ � ������� (���������� �������),
*     �� ��� ��� ������� ������� ����� ���� ������.
*
******************************************************************/

%macro transform_extract;
   /* ���� ������������� ������� �� �����, �� ��� */
   %if %length(&tpResourceId) eq 0 %then %return;
   /* ���� ������������� ������ �� �����, �� ��� */
   %if %length(&tpVersion) eq 0 %then %return;

   /* �������� �������� ������ ������� */
   %if &_OUTPUT_count le 1 %then %do;
      %local lmvUID tpOutRegistry;
      %unique_id (mpOutKey=lmvUID);
      %let tpOutRegistry       = work.tr_extr_reg_&lmvUID.;
   %end;

   %global ETL_EXTRACT_RC;
   %let ETL_EXTRACT_RC = 0;
   %local lmvVersion;

   %etl_extract_common (
      mpData            =  &tpIn,
      mpResourceId      =  &tpResourceId,
      mpVersion         =  &tpVersion,
      mpOutRegistry     =  &tpOutRegistry,
      mpOutStatusKey    =  ETL_EXTRACT_RC,
      mpOutVersionKey   =  lmvVersion
   );

   %if &ETL_EXTRACT_RC = 1 %then %do;
      /* �������� �� ������������, ���� ��� ��������� ��� ��������� */
      %return;
   %end;
   %if &ETL_EXTRACT_RC = 2 %then
      /* ��������� �������� */
      %let ETL_EXTRACT_RC = 0;;
   %if &ETL_EXTRACT_RC ne 0 %then %return;

   /* �������� ������ �����, ��� ������� ��������� ������� */
   %local lmvInCols;
   %etl_get_input_columns(mpTableMacroName=tpOutData, mpOutKey=lmvInCols);
   %local lmvFieldDigest;
   %if &tpCreateDigest = Yes %then
      %let lmvFieldDigest = ETL_DIGEST_CD;;

   %etl_extract (
      mpIn=&tpIn,
      mpOut=&tpOutData,
      mpWhere=&tpWhere,
      mpKeep=&lmvInCols,
      mpResourceId=-1,
      mpExtractId=&lmvVersion,
      mpSrcSystem="&tpSrcSystem",
      mpFieldDigest=&lmvFieldDigest,
      mpFieldSrcSystem=SOURCE_SYSTEM_CD,
      mpFieldExtractId=ETL_EXTRACT_ID,
      mpFieldResourceId=ETL_RESOURCE_ID,
      mpIn_engine=&tpIn_engine
   );
%mend transform_extract;

%transform_extract;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/** Step end �������� ������ **/

/*==========================================================================* 
 * Step:            Extract                               A52HJCF1.AT002F4C * 
 * Transform:       Extract                                                 * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    �������� ������tpOutData -            A52HJCF1.AQ000NG0 * 
 *                   work.W1DMWR5N                                          * 
 * Target Table:    Extract - work.W1DMY7X0               A52HJCF1.AQ000NG1 * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F4C);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(work.W1DMWR5N); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

/*---- Map the columns  ----*/ 
proc datasets lib = work nolist nowarn memtype = (data view);
   delete W1DMY7X0;
quit;

%put %str(NOTE: Mapping columns ...);
proc sql;
   create view work.W1DMY7X0 as
      select
         INCOMECODENUMBER,
         INCOMECODENAME,
         SOURCE_SYSTEM_CD,
         (&AVAILABLE_DTTM) as ETL_AVAILABLE_DTTM length = 8
            format = DATETIME20.
            informat = DATETIME20.
            label = 'ETL_AVAILABLE_DTTM',
         (&RESOURCE_ID) as ETL_RESOURCE_ID length = 8
            format = 21.
            informat = 21.
            label = 'ETL_RESOURCE_ID',
         ETL_EXTRACT_ID
   from &SYSLAST
   ;
quit;

%let SYSLAST = work.W1DMY7X0;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/** Step end Extract **/

/*==========================================================================* 
 * Step:            Table Loader                          A52HJCF1.AT002F4D * 
 * Transform:       Table Loader (Version 2.1)                              * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    Extract - work.W1DMY7X0               A52HJCF1.AQ000NG1 * 
 * Target Table:    EOD_PMTDIR_INCOMECODE_FULL -          A52HJCF1.AH0002XZ * 
 *                   work_stg.EOD_PMTDIR_INCOMECODE_FULL                    * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F4D);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

/* Access the data for WORK_STG  */ 
LIBNAME work_stg BASE "!ETL_DATA_ROOT/work_stg";
%rcSet(&syslibrc); 

%let SYSLAST = %nrquote(work.W1DMY7X0); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

%global etls_tableExist;
%global etls_numIndex;
%global etls_lastTable;
%let etls_tableExist = -1; 
%let etls_numIndex = -1; 
%let etls_lastTable = &SYSLAST; 

/*---- Define load data macro  ----*/ 

/* --------------------------------------------------------------
   Load Technique Selection: Replace - EntireTable
   Constraint and index action selections: 'ASIS','INIT','ASIS','ASIS'
   Additional options selections... 
      Set unmapped to missing on updates: false 
   -------------------------------------------------------------- */
%macro etls_loader;

   %let etls_tableOptions = ;
   
   /* Determine if the target table exists  */ 
   %let etls_tableExist = %eval(%sysfunc(exist(work_stg.EOD_PMTDIR_INCOMECODE_FULL, DATA)) or 
         %sysfunc(exist(work_stg.EOD_PMTDIR_INCOMECODE_FULL, VIEW))); 
   
   %if &etls_tableExist %then 
   %do;/* table exists  */ 
      %let etls_hasPreExistingConstraint=0; 
      
      proc datasets lib = work nolist nowarn memtype = (data view);
         delete etls_commands etls_commands_F;
      quit;
      
      %let etls_otherTablesReferToThisTable=0;
      
      %macro etls_CIContents(table=,workTableOut=,inDSOptions=);
         %put NOTE: Building table listing Constraints and Indexes for: &table;
         proc datasets lib=work nolist; delete &workTableOut; quit;
         proc contents data=&table&inDSOptions out2=&workTableOut noprint; run;
         
         data &workTableOut;
            length name $60 type $20 icown idxUnique idxNoMiss $3 recreate $600;
            name = '';
            type = '';
            icown = '';
            idxUnique = '';
            idxNoMiss = '';
            recreate = '';
            set &workTableOut;
            ref = '';
            type=upcase(type);
            if type eq 'REFERENTIAL' then
            do;
               put "WARNING%QUOTE(:) Target table is referenced by constraints in"
                    " another table: " ref;
               call symput('etls_otherTablesReferToThisTable','1');
               delete;
            end;
            if type='INDEX' and ICOwn eq 'YES' then delete;
         run;
         %rcSet(&syserr); 
         
      %mend etls_CIContents;
      
      %etls_CIContents(table=work_stg.EOD_PMTDIR_INCOMECODE_FULL, workTableOut=etls_commands, inDSOptions=);
      
      %if &etls_otherTablesReferToThisTable %then 
         %put WARNING%QUOTE(:) Replacing entire table will fail. Consider an alternate load technique or revising constraints.; 
      %else 
      %do; /* okay - remove foreign keys  */ 
      
         data etls_commands_F; 
            set etls_commands; 
            if upcase(type)="FOREIGN KEY" then 
            do; 
               command='ic delete '||trim(name)||';';
               output etls_commands_F; 
            end; 
         run; 
         
         %put %str(NOTE: Removing foreign keys before dropping table...);
         data _null_;
            set etls_commands_F end=eof;
            if _n_=1 then 
               call execute('proc datasets nolist lib=work_stg;modify EOD_PMTDIR_INCOMECODE_FULL;');
            call execute(command);
            if eof then call execute('; quit;');
         run;
         %rcSet(&syserr); 
         
   %end; /* okay - remove foreign keys  */ 
   
   proc datasets lib = work nolist nowarn memtype = (data view);
      delete etls_commands etls_commands_F;
   quit;
   
   /*---- Drop a table  ----*/ 
   %put %str(NOTE: Dropping table ...);
   proc datasets lib = work_stg nolist nowarn memtype = (data view);
      delete EOD_PMTDIR_INCOMECODE_FULL;
   quit;
   
   %rcSet(&syserr); 
   
   %let etls_tableExist = 0;
   
%end; /* table exists  */ 

/*---- Create a new table  ----*/ 
%if (&etls_tableExist eq 0) %then 
%do;  /* if table does not exist  */ 

   %put %str(NOTE: Creating table ...);
   
   data work_stg.EOD_PMTDIR_INCOMECODE_FULL;
      attrib INCOMECODENUMBER length = $5
         format = $5.
         informat = $5.
         label = 'INCOMECODENUMBER'; 
      attrib INCOMECODENAME length = $255
         format = $255.
         informat = $255.
         label = 'INCOMECODENAME'; 
      attrib SOURCE_SYSTEM_CD length = $3
         format = $3.
         label = 'SOURCE_SYSTEM_CD'; 
      attrib ETL_AVAILABLE_DTTM length = 8
         format = DATETIME20.
         informat = DATETIME20.
         label = 'ETL_AVAILABLE_DTTM'; 
      attrib ETL_RESOURCE_ID length = 8
         format = 21.
         informat = 21.
         label = 'ETL_RESOURCE_ID'; 
      attrib ETL_EXTRACT_ID length = 8
         label = 'ETL_EXTRACT_ID'; 
      call missing(of _all_);
      stop;
   run;
   
   %rcSet(&syserr); 
   
%end;  /* if table does not exist  */ 

/*---- Append  ----*/ 
%put %str(NOTE: Appending data ...);

proc append base = work_stg.EOD_PMTDIR_INCOMECODE_FULL 
   data = &etls_lastTable (&etls_tableOptions)  force ; 
 run; 

%rcSet(&syserr); 

/*---- proceed with any post-load constraints and indexing  ----*/ 
%if (%sysfunc(getoption(OBS))=0 and &syserr ne 0) %then 
   %put %str(NOTE: Constraint and index processing skipped because of errors. );
%else 
%do; 
   /*---- Create the integrity constraints for a table  ----*/ 
   %put %str(NOTE: Creating integrity constraints ...);
   proc datasets library=work_stg nolist;
      modify EOD_PMTDIR_INCOMECODE_FULL;
         ic create not null (INCOMECODENUMBER);
         ic create not null (SOURCE_SYSTEM_CD);
         ic create not null (ETL_AVAILABLE_DTTM);
         ic create not null (ETL_RESOURCE_ID);
         ic create not null (ETL_EXTRACT_ID);
   quit;
   
   %rcSet(&syserr); 
   
%end; /* proceed with any post-load constraints and indexing  */ 
 
%mend etls_loader;
%etls_loader;



/** Step end Table Loader **/

/*==========================================================================* 
 * Step:            ��������� ������                      A52HJCF1.AT002F4E * 
 * Transform:       ��������� ������                                        * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A52HJCF1.AT002F4E);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 0; 
%let _OUTPUT_count = 0; 


/*****************************************************************
* ������:
*   $Id: transform_job_finish.sas 4063:34040bca471f 2015-06-19 09:28:58Z rusane $
*
******************************************************************
* �����ǵ���:
*   ������������ ����� ������ ETL.
*
* ���������:
*   ���
*
******************************************************************/

%macro transform_job_finish;
   %etl_job_finish;
%mend transform_job_finish;

%transform_job_finish;


%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/** Step end ��������� ������ **/

%let etls_endTime = %sysfunc(datetime(),datetime.);

