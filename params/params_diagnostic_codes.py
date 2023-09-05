# Databricks notebook source
### ICD-10
### SNOMED
#### Diagnostic and inclusion codes

# COMMAND ----------

## PATIENT COHORT - CVDP ELIGIBILITY
CVDP_COHORT_CODES = ['CVDPCX001','CVDPCX002']

## CVD CODES
ALL_CVD_CODES = ['I01', 'I02', 'I03', 'I04', 'I05', 'I06', 'I07', 'I08', 'I09',
                           'I10', 'I11', 'I12', 'I13', 'I14', 'I15',
                           'I20', 'I21', 'I22', 'I23', 'I24', 'I25',
                           'I60', 'I61', 'I62', 'I63', 'I64', 'I65',
                           'I66', 'I67', 'I68', 'I69']
STROKE_CODES = ['I61','I63','I64']
HEARTATTACK_CODES = ['I21','I22']

##DIAGNOSTIC FLAG CODES
PAD_CODE = ['PAD_COD']
AAA_CODE = ['AAA_COD']
HF_CODE = ['HF_COD']
CHD_CODE = ['CHD_COD']
STROKE_CODE = ['STRK_COD']
TIA_CODE = ['TIA_COD']
DIABETES_CODE = ['DMTYPE1AUDIT_COD','DMTYPE2AUDIT_COD']
CKD_CODE = ['CKD_COD']
AF_CODE = ['AFIB_COD']
HYP_CODE = ['HYP_COD']
FH_SCREEN_CODE = ['DULIPID_COD', 'SBROOME_COD']
NDH_CODE = ['IGT_COD', 'NDH_COD', 'PRD_COD']

#RESOLVED CODES
CKD_RES_CODE = ['CKDRES_COD']
AF_RES_CODE = ['AFIBRES_COD']
HYP_RES_CODE = ['HYPRES_COD']

#SNOMED CODES
FH_SNOMED_CODES = ['238079002','473145005','513831000000107','698600006','867261000000106','398036000','403829002','445010006','238081000','403831006','238078005', '403830007','397915002','767133009']

# BLOOD PRESSURE CODES (FOR HYPERTENSION)
BP_CODE = ['BP_COD']

## INDICATOR SNOMED CODES
SYSTOLIC_BP_CODES = ['314441002','314442009','314444005','314445006','314446007','400974009',
		'707303003','12929001','251070002','314438006','314447003','72313002','81010002','407556006',
		'314439003','163030003','271649006','314443004','18050000','18352002','314440001','314448008',
		'314449000','314464000','399304008','407554009','716579001','413606001','198081000000101']

DIASTOLIC_BP_CODES = ['1091811000000102','314454009','314462001','314465004','407555005',
		'53813002','174255007','314452008','407557002','42689008','716632005','314461008',
		'400975005','163031004','271650006','314451001','314453003','314455005','314460009',
		'23154005','314456006','314457002','314458007','314459004','49844009', '42689008',
		'23154005', '49844009','413605002','198091000000104']

# SMOKING STATUS CODES
CURRENT_SMOKER_CODES = ['160603005','160606002','160613002','160619003','230056004','230057008',
                  '230058003','230059006','230060001','230062009','230065006','266918002',
                  '446172000','449868002','56578002','56771006','59978006','65568007',
                  '134406006','160604004','160605003','160612007','160616005','203191000000107',
                  '225934006','230063004','230064005','266920004','266929003','308438006',
                  '394871007','394872000','394873005','401159003','413173009','428041000124106',
                  '77176002','82302008','836001000000109','1092481000000104','266927001',
                  '365982000','782516008','401201003','110483000']

EX_SMOKER_CODES = ['1092041000000104','1092071000000105','1092091000000109','160617001','160620009',
             '160625004','228486009','266921000','266922007','266923002','360890004','735128000',
             '1092031000000108','1092111000000104','1092131000000107','160621008','266924008',
             '266925009','266928006','281018007','360900008','48031000119106','492191000000103',
             '53896009','735112005','8517006','449369001','105541001','449345000','449368009',
             '517211000000106','405746006','160618006','105540000','87739003','105539002',
             '360918006','360929005','8392000']

NEVER_SMOKED_CODES = ['221000119102','266919005']

# SMOKING INTEVENTION CODES
SMOKING_INTERVENTION_CODES = ['SMOKINGINT_COD', 'PHARMDRUG_COD']