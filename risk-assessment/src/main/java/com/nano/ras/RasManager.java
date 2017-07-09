package com.nano.ras;

import javax.enterprise.concurrent.ManagedScheduledExecutorService;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.nano.ras.tools.ApplicationBean;
import com.nano.ras.tools.QueryManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 *
 * @author Diyan
 */

@Startup
@Singleton
public class RasManager {
		
	private Logger log = Logger.getLogger(getClass());

	private QueryManager queryManager ;
	private RasProcessor processor ;
	private ApplicationBean applicationBean ;
	
	private int pause = 5;

	public RasManager() {
		// TODO Auto-generated constructor stub
	}

	@Inject
	public RasManager(QueryManager queryManager, 
			RasProcessor rasProcessor, ApplicationBean applicationBean) {
		// TODO Auto-generated constructor stub

		this.queryManager = queryManager;
		this.processor = rasProcessor;
		this.applicationBean = applicationBean;
	}

	@Resource
	private ManagedScheduledExecutorService managedScheduledExecutorService;

	@PostConstruct
	public void init(){

		log.info("Commencing managed scheduled task");
		runAssessment();
	}
	
	/**
	 * Fetch {@link Subscriber} MSISDN in batch of 25K.
	 * Iterate list asynchronously and forward to JMS destination (also asynchronously)
	 * Break for x hours for every 250k records fetched to allow time for asynchronous
	 * threads to complete processing and refreshing of database view.
	 * At this rate, application should achieve roughly Y million assessments daily.
	 * 
	 */
	private void runAssessment(){
		
		managedScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

			private Logger log = Logger.getLogger(getClass());
			
			private int startPosition = 0;
			private int fetchSize = applicationBean.getFetchSize();

			@Override
			public void run() {
				// TODO Auto-generated method stub

				log.info("Starting ras job mass task execution");
				
				List<String> subscribers = queryManager.getMsisdnFromView(startPosition, fetchSize);
				
				while (!subscribers.isEmpty()) {
					try {
						processor.start(subscribers);
						startPosition += fetchSize;
						subscribers = queryManager.getMsisdnFromView(startPosition, fetchSize);
						log.info("startPosition:" + startPosition);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						log.error("", e);
					}
				}
			}
		}, 1, pause, TimeUnit.MINUTES);
	}

}