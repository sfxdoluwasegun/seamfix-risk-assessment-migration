package com.nano.ras;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ejb.Asynchronous;
import javax.ejb.Stateless;
import javax.inject.Inject;

import org.apache.commons.lang.time.StopWatch;
import org.jboss.ejb3.annotation.TransactionTimeout;
import org.jboss.logging.Logger;

import com.nano.jpa.entity.Subscriber;
import com.nano.ras.tools.DbManager;

@Stateless
public class RasProcessor {
	
	private Logger log = Logger.getLogger(getClass());
	
	@Inject
	private Assessment assessment ;
	
	@Inject
	private DbManager queryManager ;

	/**
	 * Iterate through list and forward for asynchronous assessment.
	 * 
	 * @param subscribers list of subscriber MSISDNs.
	 */
	@Asynchronous
	@TransactionTimeout(unit = TimeUnit.MINUTES, value = 300)
	public void start(List<String> subscribers) {

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		subscribers.forEach(msisdn -> initAssessment(msisdn));

		stopWatch.stop();
		log.info("finished iterating through subscriber list" +  + stopWatch.getTime() + "ms");
	}
	
	/**
	 * Initialize assessment process.
	 * 
	 * @param msisdn subscriber unique MSISDN
	 */
	public void initAssessment(String msisdn){

		Subscriber subscriber = queryManager.createSubscriber(msisdn);

		try {
			if(subscriber.getAssessment() == null)
				assessment.performFreshAssessment(subscriber);
			else
				assessment.reAssessment(subscriber);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}
	}
	
}
