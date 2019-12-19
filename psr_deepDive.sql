
select distinct
  gp.gpAccountGlotoId,
  gp.gpAccountNo,
  gp.printComms,
  gp.gpEventCreatedAt,
  '!!!!!!!!' as space2,
  ac.acPrintComms,
  ac.acInsertTime,
  '!!!!!!!!' as space3,
  sdg.sdgInsertTime

--global topic
from (
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
   ) gp

--Auto-capture'd CIP topic
join (
  select
    globalAccountId as acGlobalAccountId,
    services.communications.print as acPrintComms,
    --metadata as acMetadata,
    kafkaData.insertTime as acInsertTime
  from `data-engineering-prod.auto_capture_v2_secure.psr_entry_update_v1`
  ) ac on gp.gpGlobalAccountId =  ac.acGlobalAccountId

--Gentrack Account SDM topic
join (
  select
    cast(ACCTNO as string) as sdgACCTNO,
ACEXTNALREF,
    kafkaData.insertTime as sdgInsertTime
  from `source-data-mirror-prod.genprod_events.dataAvailability_genprod_dbo_ACCOUNTS_sourceMirror_v1` sdg
  ) sdg on gp.gpAccountNo = sdg.sdgACCTNO

where gpAccountNo = '3127167'

order by
  gp.gpAccountNo,
  sdg.sdgInsertTime desc,
  ac.acInsertTime desc,
  gp.gpEventCreatedAt desc
