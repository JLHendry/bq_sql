select *
from `data-engineering-prod.u_james_hendry.printComms_rca_20191218` pc


select distinct
  pc.AccountId,
  pc.Account_Global_ID,
  pc.CreatedAt,
  '!!!!!!!!' as space1,
  gp.gpAccountGlotoId,
  --gp.gpSourceFieldGt,
  gp.gpAccountNo,
  --gp.gpSourceFieldSf,
  --gp.gpGlobalAccountId,
  gp.printComms,
  gp.gpEventCreatedAt,
  '!!!!!!!!' as space2,
  --ac.acGlobalAccountId,
  ac.acPrintComms,
  ac.acInsertTime,
  '!!!!!!!!' as space3,
  --sdg.sdgACCTNO,
  sdg.sdgInsertTime

from `data-engineering-prod.u_james_hendry.printComms_rca_20191218` pc

join (
  select
    accountId as gpAccountGlotoId,
    kg.key as gpSourceFieldGt,
    kg.value as gpAccountNo,
    ksf.key as gpSourceFieldSf,
    ksf.value as gpGlobalAccountId,
    ss.name as printComms ,
    eventMetadata.createdAt as gpEventCreatedAt
  from `global-topics-prod.psr_vulnerabilities_confidential.psr_vulnerabilities_v1` gp
  join gp.sourceMetadata.id kg
  join gp.sourceMetadata.id ksf
  left join gp.services.services ss
  where kg.key = 'ACCTNO'
  and ksf.key = 'globalAccountId'
  and ss.name = 'printCommunications' ) gp on pc.AccountId = gp.gpAccountNo

join (
  select
    globalAccountId as acGlobalAccountId,
    services.communications.print as acPrintComms,
    --metadata as acMetadata,
    kafkaData.insertTime as acInsertTime
  from `data-engineering-prod.auto_capture_v2_secure.psr_entry_update_v1`  ) ac on pc.Account_Global_ID =  ac.acGlobalAccountId

join (
  select
    cast(ACCTNO as string) as sdgACCTNO,
ACEXTNALREF,
    kafkaData.insertTime as sdgInsertTime
  from `source-data-mirror-prod.genprod_events.dataAvailability_genprod_dbo_ACCOUNTS_sourceMirror_v1` sdg  ) sdg on pc.AccountId = sdg.sdgACCTNO

order by
  pc.AccountId,
  sdg.sdgInsertTime desc,
  ac.acInsertTime desc,
  gp.gpEventCreatedAt desc


select pc.*,ra.Account_No, Account_Global_ID
from `data-engineering-prod.u_james_hendry.printComms_20191218` pc
join `data-engineering-prod.reporting_crm.v_Account` ra on pc.accountId = ra.Account_No