package com.dianping.polestar.engine;

import java.io.File;

import org.apache.log4j.Logger;

import com.dianping.polestar.entity.Query;
import com.dianping.polestar.entity.QueryResult;
import com.dianping.polestar.jobs.EnvironmentConstants;
import com.dianping.polestar.jobs.Job;
import com.dianping.polestar.jobs.JobContext;
import com.dianping.polestar.jobs.JobManager;
import com.dianping.polestar.jobs.ProcessJob;
import com.dianping.polestar.jobs.Utilities;

public class HiveCommandQueryEngine implements IQueryEngine {
	public static final Logger LOG = Logger
			.getLogger(HiveCommandQueryEngine.class);

	private static final String HIVE_COMMAND_FORMAT = "set hive.cli.print.header=true;use %s;%s;";
	private static final IQueryEngine INSTANCE = new HiveCommandQueryEngine();

	public static IQueryEngine getInstance() {
		return INSTANCE;
	}

	@Override
	public QueryResult postQuery(Query query) {
		JobContext jobCtx = new JobContext();
		jobCtx.setId(query.getId());
		jobCtx.setUsername(query.getUsername());
		jobCtx.setPasswd(query.getPassword());
		jobCtx.setStoreResult(query.isStoreResult());
		jobCtx.setResLimitNum(query.getResLimitNum());
		jobCtx.setCommands(new String[] {
				"hive",
				"-e",
				String.format(HIVE_COMMAND_FORMAT, query.getDatabase(),
						query.getSql()) });
		jobCtx.setWorkDir(EnvironmentConstants.WORKING_DIRECTORY_ROOT
				+ File.separator + query.getId());

		Job job = new ProcessJob(jobCtx);
		JobManager.putJob(query.getId(), job);
		JobManager.putJobContext(query.getId(), jobCtx);
		QueryResult queryRes = new QueryResult();
		queryRes.setId(query.getId());
		try {
			int exitCode = job.run();
			LOG.info("exitcode:" + exitCode + " job-id:" + jobCtx.getId());
			if (0 == exitCode) {
				queryRes.setSuccess(true);
				if (jobCtx.isStoreResult()) {
					queryRes.setResultFilePath(jobCtx.getLocalDataPath());
				} else {
					Utilities.fillInColumnsAndData(jobCtx.getStdout()
							.toString(), queryRes);
				}
			} else {
				queryRes.setErrorMsg(jobCtx.getStderr().toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return queryRes;
	}
}
