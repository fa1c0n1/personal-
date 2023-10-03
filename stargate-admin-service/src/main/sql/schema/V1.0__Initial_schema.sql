db.scheduler.createIndex({_group: 1, _instanceId: 1, _status: 1}, { unique: true });
db.scheduler.createIndex({_status: 1, _updatedOn: 1, _retryCount: 1}, { unique: false });
db.scheduler.createIndex({_group: 1, _status: 1}, { unique: false });
db.locks.createIndex({lockId:1}, {unique: true})
db.locks.createIndex({lockId:1, lockedBy:1, lockedOn: 1}, {unique: true})
db.locks.insert({"lockId":"SchedulerService.executeSequentialRestJob", "lockedBy": null, "lockedOn": ISODate("2020-01-01T01:01:01.000-06:00")})
db.job.createIndex({pipelineId:1}, { unique: true })
db.job.createIndex({pipelineGroup:1, streamName: 1}, { unique: true })
db.job.createIndex({pipelineGroup:1, appId: 1}, { unique: false })
db.checkpoint.createIndex({_pipelineId:1, _stateId: 1}, { unique: false })