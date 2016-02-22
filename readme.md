原理:
基于phoenix的local二级索引开发,phoenix的二级索引管理方式不变,
而是采用regionobserver的方式,在对数据表进行索引字段put 或 row/索引所在的family/索引qualifier维护delete操作时,
更新local二级索引表.

支持客户端采用hbase api对数据表进行批量put/delete操作,
但是单次批量操作中,不能对数据表同一row进行put和delete两种操作.

regionobserver在更新local二级索引表时并不保证与数据表一致,因为更新local二级索引在数据表更新成功之后,
如果更新local二级索引失败,会报错(也写日志),但是数据表已经更新成功,客户端可以进行重试,
但是因为数据表已经更新,所以local二级索引表中老的数据并没有删除成功,导致出现"孤岛"数据,
但是对读取数据并不影响,建议定期对local二级索引进行重建.
