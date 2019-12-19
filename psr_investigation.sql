
/*Add the globalAccountId from v_Account to the Account_No from the Orion list
select pc.*, Account_Global_ID
from `data-engineering-prod.u_james_hendry.printComms_20191218` pc
join `data-engineering-prod.reporting_crm.v_Account` ra on pc.accountId = ra.Account_No
*/

/*Join the Orion list (from above) with the global topic, the auto-capture CIP topic and the gentrack Account SDM table*/
select distinct
  pc.AccountId,
  pc.Account_Global_ID,
  pc.CreatedAt,
  '!!!!!!!!' as space1,
  gp.gpAccountGlotoId,
  gp.gpAccountNo,
  gp.printComms,
  gp.gpEventCreatedAt,
  '!!!!!!!!' as space2,
  ac.acPrintComms,
  ac.acInsertTime,
  '!!!!!!!!' as space3,
  sdg.sdgInsertTime

--Orion list
from `data-engineering-prod.u_james_hendry.printComms_rca_20181218` pc

--global topic
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
  and ss.name = 'printCommunications'
   ) gp on pc.AccountId = gp.gpAccountNo

--Auto-capture'd CIP topic
join (
  select
    globalAccountId as acGlobalAccountId,
    services.communications.print as acPrintComms,
    --metadata as acMetadata,
    kafkaData.insertTime as acInsertTime
  from `data-engineering-prod.auto_capture_v2_secure.psr_entry_update_v1`
  ) ac on pc.Account_Global_ID =  ac.acGlobalAccountId

--Gentrack Account SDM topic
join (
  select
    cast(ACCTNO as string) as sdgACCTNO,
ACEXTNALREF,
    kafkaData.insertTime as sdgInsertTime
  from `source-data-mirror-prod.genprod_events.dataAvailability_genprod_dbo_ACCOUNTS_sourceMirror_v1` sdg
  ) sdg on pc.AccountId = sdg.sdgACCTNO

order by
  pc.AccountId,
  sdg.sdgInsertTime desc,
  ac.acInsertTime desc,
  gp.gpEventCreatedAt desc

