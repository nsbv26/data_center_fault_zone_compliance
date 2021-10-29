SELECT [entity] as Cluster, [FZ1], [FZ2], [Linux], [Windows] FROM
(SELECT [entity], TagName FROM dbo.VCTagAssignment
	where EntityType like 'ClusterComputeResource') AS t1 
PIVOT (MAX(TagName) FOR TagName IN ([FZ1], [FZ2], [Linux], [Windows]) ) AS t2
