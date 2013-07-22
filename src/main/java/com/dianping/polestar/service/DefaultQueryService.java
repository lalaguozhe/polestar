package com.dianping.polestar.service;

import com.dianping.polestar.engine.HiveCommandQueryEngine;
import com.dianping.polestar.engine.IQueryEngine;
import com.dianping.polestar.entity.Query;
import com.dianping.polestar.entity.QueryResult;
import com.dianping.polestar.entity.QueryStatus;
import com.dianping.polestar.jobs.JobContext;
import com.dianping.polestar.jobs.JobManager;

public class DefaultQueryService implements IQueryService {

	private static final DefaultQueryService INSTANCE = new DefaultQueryService();

	private IQueryEngine hiveCmdEngine = HiveCommandQueryEngine.getInstance();

	public static IQueryService getInstance() {
		return INSTANCE;
	}

	@Override
	public QueryResult postQuery(Query query) {
		QueryResult queryRes = null;
		long startTime = System.currentTimeMillis();
		if ("hive".equalsIgnoreCase(query.getMode())) {
			queryRes = hiveCmdEngine.postQuery(query);
		} else if ("shark".equalsIgnoreCase(query.getMode())) {

		}
		queryRes.setExecTime((System.currentTimeMillis() - startTime) / 1000);
		return queryRes;
	}

	@Override
	public QueryStatus getStatusInfo(String id) {
		QueryStatus status = new QueryStatus();
		JobContext jobctx = JobManager.getJobContextById(id);
		if (jobctx != null) {
			status.setMessage(jobctx.getStderr().toString());
			status.setSuccess(true);
		} else {
			status.setSuccess(false);
		}
		return status;
	}
}
