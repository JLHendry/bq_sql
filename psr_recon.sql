
--global topic
with global_psr as (
  select
    accountId as gpAccountGlotoId,
    kg.key as gpSourceFieldGt,
    kg.value as gpAccountNo,
    ksf.key as gpSourceFieldSf,
    ksf.value as gpGlobalAccountId,
    case ss.name
        when 'printCommunications' then true
        else null
    end as gpPrintComms,
    eventMetadata.createdAt as gpEventCreatedAt
  from `global-topics-prod.psr_vulnerabilities_confidential.psr_vulnerabilities_v1` gp
  join gp.sourceMetadata.id kg
  join gp.sourceMetadata.id ksf
  left join gp.services.services ss
  where kg.key = 'ACCTNO'
  and ksf.key = 'globalAccountId'
  and ss.name = 'printCommunications'
  ),

--Auto-capture'd CIP topic
cip_psr_all as (
  select
    globalAccountId as acGlobalAccountId,
    services.communications.print as acPrintComms,
    kafkaData.insertTime as acInsertTime,
    RANK() OVER(PARTITION BY globalAccountId ORDER BY kafkaData.insertTime DESC) AS rank
  from `data-engineering-prod.auto_capture_v2_secure.psr_entry_update_v1`
  order by kafkaData.insertTime desc
  ),

cip_psr_latest as (
  select
    acGlobalAccountId,
    acPrintComms,
    acInsertTime
  from cip_psr_all
  where rank = 1
  )

select *
from cip_psr_latest cl
full outer join global_psr gp on acGlobalAccountId = gpGlobalAccountId
where acPrintComms <> gpPrintComms