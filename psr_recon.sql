
--global topic
with global_psr as (
  select
    accountId as gpAccountGlotoId,
    kg.key as gpSourceFieldGt,
    kg.value as gpAccountNo,
    ksf.key as gpSourceFieldSf,
    ksf.value as gpGlobalAccountId,
    case ss.name when 'printCommunications' then true else null end as gpPrintComms,
    case ss.name when 'LargePrintCommunications' then true else null end as gpLargePrintComms,
    case ss.name when 'printBlackAndWhiteCommunications' then true else null end as gpprintBlackAndWhiteCommunications,
    case ss.name when 'largePrintBlackAndWhiteCommunications' then true else null end as gplargePrintBlackAndWhiteCommunications,
    case ss.name when 'audioCommunications' then true else null end as gpaudioCommunications,
    case ss.name when 'brailleCommunications' then true else null end as gpbrailleCommunications,
    --case gp.nomineeScheme.surname when null then false else true end as gpNomineeScheme,
    eventMetadata.createdAt as gpEventCreatedAt
  from `global-topics-prod.psr_vulnerabilities_confidential.psr_vulnerabilities_v1` gp
  join gp.sourceMetadata.id kg
  join gp.sourceMetadata.id ksf
  left join gp.services.services ss
  where kg.key = 'ACCTNO'
  and ksf.key = 'globalAccountId'
  and (
    gp.nomineeScheme is not null
    or ss.name in (
    'printCommunications',
    'largePrintCommunications',
    'printBlackAndWhiteCommunications',
    'largePrintBlackAndWhiteCommunications',
    'audioCommunications',
    'brailleCommunications' ))
  ),

--Auto-capture'd CIP topic
cip_psr_all as (
  select
    globalAccountId as acGlobalAccountId,
    services.communications.print as acPrintComms,
    services.communications.largePrint as acLargePrint,
    services.communications.printBlackAndWhite as acBlackAndWhite,
    services.communications.largePrintBlackAndWhite as acLargePrintBlackAndWhite,
    services.communications.audio as acAudio,
    services.communications.braille as acBraille,
    --services.nomineeScheme as acNomineeScheme,
    kafkaData.insertTime as acInsertTime,
    RANK() OVER(PARTITION BY globalAccountId ORDER BY kafkaData.insertTime DESC) AS rank
  from `data-engineering-prod.auto_capture_v2_secure.psr_entry_update_v1`
  ),

cip_psr_latest as (
  select
    acGlobalAccountId,
    acPrintComms,
    acLargePrint,
    acBlackAndWhite,
    acLargePrintBlackAndWhite,
    acAudio,
    acBraille,
    --acNomineeScheme,
    acInsertTime
  from cip_psr_all
  where rank = 1
  )

select
    cl.acGlobalAccountId,
    cl.acPrintComms,
    gp.gpPrintComms,
    cl.acLargePrint,
    gp.gpLargePrintComms,
    cl.acBlackAndWhite,
    gp.gpprintBlackAndWhiteCommunications,
    cl.acLargePrintBlackAndWhite,
    gp.gplargePrintBlackAndWhiteCommunications,
    cl.acAudio,
    gp.gpaudioCommunications,
    cl.acBraille,
    gp.gpbrailleCommunications

from cip_psr_latest cl
full outer join global_psr gp on acGlobalAccountId = gpGlobalAccountId
where acPrintComms <> gpPrintComms
    or acLargePrint <> acLargePrint
    or acBlackAndWhite <> acBlackAndWhite
    or acLargePrintBlackAndWhite <> acLargePrintBlackAndWhite
    or acAudio <> acAudio
    or acBraille <> acBraille