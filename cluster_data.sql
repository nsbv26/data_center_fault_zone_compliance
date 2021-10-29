SELECT a.vcenter
    ,a.Cluster
    ,a.FullName
    ,a.Model
    ,a.CPUModel
    ,a.MemorySize
    ,b.VMUuid
	,EVCModeCluster as EVC

      
      
  FROM [VMInventory].[dbo].[HostHW] a
  JOIN  [VMInventory].[dbo].[VMInv] b ON b.ESXiHost = a.FullName


  
  where a.Cluster not like '%ESXC%'
  and b.IsActive = 1