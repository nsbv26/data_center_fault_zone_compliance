SELECT                                          
    h.name as ESXiHost
    ,s.alias as Array
    ,s.tier as Tier
    ,cf.provisionedCapacityMB / 1048576 as AllocatedTB
    ,cf.usedCapacityMB / 1048576 as UsedTB
    
FROM
    dwh_capacity.host_dimension h
    ,dwh_capacity.chargeback_fact cf
    ,dwh_capacity.storage_dimension s
    ,dwh_capacity.date_dimension

WHERE
    date_dimension.latest = 1
	AND cf.mappedbyVM = 1
	AND h.`tk` = cf.hostTk
	AND `date_dimension`.`tk` = cf.`dateTk`
	and s.tk = cf.storageTk
--	and h.name <> 'N/A' and s.name <> 'N/A'
	and s.family <> 'SVC'
	and s.microcodeVersion  not like '%ONTAP%' and s.microcodeVersion not like '%7-Mode%'		#/* ONTAP volumes don't appear in chargeback_fact so we need to handle them separately */#

    
        
       
        