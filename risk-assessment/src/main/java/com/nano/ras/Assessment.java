package com.nano.ras;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.ejb.Asynchronous;
import javax.ejb.Stateless;
import javax.inject.Inject;

import org.apache.commons.lang.time.StopWatch;
import org.jboss.logging.Logger;

import com.nano.jpa.entity.Subscriber;
import com.nano.jpa.entity.SubscriberHistory;
import com.nano.jpa.entity.SubscriberState;
import com.nano.jpa.entity.ras.BorrowableAmount;
import com.nano.jpa.entity.ras.RasCriteria;
import com.nano.jpa.entity.ras.SmsMessage;
import com.nano.jpa.entity.ras.SubscriberAssessment;
import com.nano.jpa.enums.SmsMessageId;
import com.nano.ras.tools.ApplicationBean;
import com.nano.ras.tools.DbManager;

@Stateless
public class Assessment {

	private Logger log = Logger.getLogger(getClass());

	@Inject
	private DbManager queryManager ;

	@Inject
	private ApplicationBean applicationBean;
	
	@PostConstruct
	public void init(){
		
		if (applicationBean.getBorrowableAmounts() == null)
			applicationBean.setBorrowableAmounts(queryManager.getBorrowableAmountListDesc());
	}

	/**
	 * Reassess {@link Subscriber} based on RAS criteria.
	 * 
	 * @param subscriber subscriber details
	 */
	@Asynchronous
	public void reAssessment(Subscriber subscriber) {
		// TODO Auto-generated method stub

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		boolean eligible = false;
                
		SubscriberAssessment subscriberAssessment = subscriber.getAssessment();
		long minutes = new Timestamp(subscriberAssessment.getLastProcessed().getTime()).toLocalDateTime().until(LocalDateTime.now(), ChronoUnit.MINUTES);
		log.info("Time of last assessment:" + minutes);
		if (minutes < 60L){
			stopWatch.stop();
			return;
		}

		subscriberAssessment.setLastProcessed(Timestamp.valueOf(LocalDateTime.now()));

		SubscriberState subscriberState = queryManager.getSubscriberStateByMsisdn(subscriber.getMsisdn());
		if(subscriberState != null)
			subscriberAssessment.setTariffPlan(subscriberState.getPayType());

		List<SubscriberHistory> subscriberHistories = queryManager.getSubscriberHistoryByMsisdn(subscriber.getMsisdn());
		if (subscriberHistories == null || subscriberHistories.isEmpty()){
			stopWatch.stop();
			return;
		}
		
		/*for (BorrowableAmount borrowableAmount : applicationBean.getBorrowableAmounts()) {
			Map<String, Object> map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, borrowableAmount, subscriberHistories);
			subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
			if ((boolean) map.get("eligible")){
				eligible = true;
				break;
			}
		}*/
		int position = applicationBean.getAmountPosition()-1;
		BorrowableAmount borrowableAmount = applicationBean.getBorrowableAmounts().get(position);
		Map<String, Object> map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, borrowableAmount, subscriberHistories);
		subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
		if ((boolean) map.get("eligible")){
			eligible = true;
			for(int i=(position+1); i<applicationBean.getBorrowableAmounts().size();i++){
				map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, applicationBean.getBorrowableAmounts().get(i), subscriberHistories);
				subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
				if ((boolean) map.get("eligible")){
					eligible = true;
					continue;
				}
				else{
					break;
				}
			}
		}
		else{
			eligible = false;
			for(int i=(position-1); i>=0; i--){
				map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, applicationBean.getBorrowableAmounts().get(i), subscriberHistories);
				subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
				if ((boolean) map.get("eligible")){
					eligible = true;
					continue;
				}
				else{
					break;
				}
			}

		}
		
		
		if (!eligible)
			queryManager.update(subscriberAssessment);

		if (subscriberState == null)
			queryManager.createSubscriberState(subscriber.getMsisdn(), BigDecimal.ZERO);

		stopWatch.stop();
		log.info("Re assessment for subscriber:" + subscriber.getMsisdn() + " completed in - " + stopWatch.getTime() + "ms");
	}

	/**
	 * Perform first Subscriber assessment based on RAS criteria.
	 * 
	 * @param subscriber subscriber details
	 */
	@Asynchronous
	public void performFreshAssessment(Subscriber subscriber) {
		// TODO Auto-generated method stub

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		boolean eligible = true;
        
		SubscriberState subscriberState = queryManager.getSubscriberStateByMsisdn(subscriber.getMsisdn());
		SubscriberAssessment subscriberAssessment = queryManager.createNewAssessment(subscriber, subscriberState);

		List<SubscriberHistory> subscriberHistories = queryManager.getSubscriberHistoryByMsisdn(subscriber.getMsisdn());
		if (subscriberHistories == null || subscriberHistories.isEmpty()){
			stopWatch.stop();
			return;
		}

		/*for (BorrowableAmount borrowableAmount : applicationBean.getBorrowableAmounts()) {
			Map<String, Object> map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, borrowableAmount, subscriberHistories);
			subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
			if ((boolean) map.get("eligible")){
				eligible = true;
				break;
			}
		}*/

		int position = applicationBean.getAmountPosition()-1;
		
			BorrowableAmount borrowableAmount = applicationBean.getBorrowableAmounts().get(position);
			Map<String, Object> map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, borrowableAmount, subscriberHistories);
			subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
			if ((boolean) map.get("eligible")){
				eligible = true;
				for(int i=(position+1); i<applicationBean.getBorrowableAmounts().size();i++){
					map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, applicationBean.getBorrowableAmounts().get(i), subscriberHistories);
					subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
					if ((boolean) map.get("eligible")){
						eligible = true;
						continue;
					}
					else{
						break;
					}
				}
			}
			else{
				eligible = false;
				for(int i=(position-1); i>=0; i--){
					map = assessSubscriberEligibilityForAmount(subscriber, subscriberAssessment, applicationBean.getBorrowableAmounts().get(i), subscriberHistories);
					subscriberAssessment = (SubscriberAssessment) map.get("subscriberAssessment");
					if ((boolean) map.get("eligible")){
						eligible = true;
						continue;
					}
					else{
						break;
					}
				}

			}
		
		
		if (!eligible)
			queryManager.updateWithNewTransaction(subscriberAssessment);

		if (subscriberState == null)
			queryManager.createSubscriberState(subscriber.getMsisdn(), BigDecimal.ZERO);

		stopWatch.stop();
		log.info("Fresh assessment for subscriber:" + subscriber.getMsisdn() + " completed in - " + stopWatch.getTime() + "ms");
	}

	/**
	 * Determine if {@link Subscriber} is eligible for {@link BorrowableAmount} argument.
	 * 
	 * @param subscriber subscriber details
	 * @param subscriberAssessment subscribers assessment info
	 * @param borrowableAmount configured amount which could be borrowed
	 * @param subscriberHistories list of subscribers transactions
	 * @return true if eligible
	 */
	private Map<String, Object> assessSubscriberEligibilityForAmount(Subscriber subscriber, 
			SubscriberAssessment subscriberAssessment, BorrowableAmount borrowableAmount, 
			List<SubscriberHistory> subscriberHistories){

		boolean status = true;
		subscriberAssessment = refreshSubscriberAssessment(subscriber, subscriberAssessment);
		RasCriteria rasCriteria = borrowableAmount.getCriteria();

		Map<String, Object> map = blacklistStatus(subscriberAssessment, status);
		boolean blacklist = (boolean) map.get("eligible");

		if (!blacklist)
			status = false;

		map = tarrifPlan((SubscriberAssessment) map.get("subscriberAssessment"), status);
		boolean tarrifPlan = (boolean) map.get("eligible");

		if (!tarrifPlan)
			status = false;

		map = ageOnNetwork(subscriber, (SubscriberAssessment) map.get("subscriberAssessment"), 
				rasCriteria, borrowableAmount, subscriberHistories, status);
		boolean ageOnNetwork = (boolean) map.get("eligible");

		if (!ageOnNetwork)
			status = false;

		map = numberOfTopupsForSpecifiedDuration((SubscriberAssessment) map.get("subscriberAssessment"), 
				rasCriteria, borrowableAmount, subscriberHistories, status);
		boolean numberOfTopUps = (boolean) map.get("eligible");

		if (!numberOfTopUps)
			status = false;

		map = cumulativeTopupAmountForSpecifiedDuration((SubscriberAssessment) map.get("subscriberAssessment"), 
				borrowableAmount, rasCriteria, subscriberHistories, status);
		boolean topUpAmount = (boolean) map.get("eligible");

		if (blacklist 
				&& tarrifPlan
				&& ageOnNetwork
				&& numberOfTopUps
				&& topUpAmount){
			queryManager.updateWithNewTransaction((SubscriberAssessment) map.get("subscriberAssessment"));
			return initializeResponse(true, subscriberAssessment);
		}

		return initializeResponse(false, subscriberAssessment);
	}

	/**
	 * Refresh SubscriberAssessment record in anticipation of new assessment.
	 * 
	 * @param subscriber subscribers detail
	 * @param subscriberAssessment subscribers assessment info
	 * @return {@link SubscriberAssessment}
	 */
	private SubscriberAssessment refreshSubscriberAssessment(Subscriber subscriber, 
			SubscriberAssessment subscriberAssessment){

		Long daysOnNetwork = 0L;

		java.util.Date activation = subscriber.getActivation();
		if (activation != null){
			daysOnNetwork = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis() - activation.getTime());
		}else{
			Timestamp timestamp = queryManager.getEarliestSubscriberHistoryTimeByMsisdn(subscriber.getMsisdn());
			daysOnNetwork = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis() - timestamp.getTime());
		}

		log.debug("daysOnNetwork:" + daysOnNetwork);

		subscriberAssessment.setAgeOnNetwork(daysOnNetwork.intValue());
		subscriberAssessment.setNumberOfTopUps(0);
		subscriberAssessment.setTotalTopUpValue(0);

		return subscriberAssessment;
	}

	/**
	 * Determine if {@link Subscriber} re-charge value meets required criteria for the {@link BorrowableAmount}.
	 * 
	 * @param subscriberAssessment subscribers assessment info
	 * @param borrowableAmount configured amount which could be borrowed
	 * @param rasCriteria criteria defined for amount
	 * @param subscriberHistories subscribers transactions
	 * @param status assessment status so far
	 * @return true if {@link Subscriber} re-charge value satisfies minimum top-up value requirement
	 */
	private Map<String, Object> cumulativeTopupAmountForSpecifiedDuration(SubscriberAssessment subscriberAssessment, 
			BorrowableAmount borrowableAmount, RasCriteria rasCriteria, 
			List<SubscriberHistory> subscriberHistories, boolean status){
		
		if (!applicationBean.isAssessTopupAmount())
			return initializeResponse(true, subscriberAssessment);

		for (SubscriberHistory subscriberHistory : subscriberHistories) {
			Long days = getDays(subscriberHistory.getRechargeTime());
			if(days <= rasCriteria.getMinTopUpsDuration())
				subscriberAssessment.setTotalTopUpValue(subscriberAssessment.getTotalTopUpValue() + (subscriberHistory.getRechargeForPrepaid().multiply(new BigDecimal("100.00")).intValue()));
		}

		if(subscriberAssessment.getTotalTopUpValue().compareTo(rasCriteria.getMinTopUpValue()) >= 0){
			if (status){
				subscriberAssessment.setSmsMessage(null);
				subscriberAssessment.setMaxBorrowableAmount(borrowableAmount);
			}
			return initializeResponse(true, subscriberAssessment);
		}

		SmsMessage smsMessage = getSmsMessage(subscriberAssessment);
		smsMessage.setTopupsAmountLeft(rasCriteria.getMinTopUpValue() - subscriberAssessment.getTotalTopUpValue());
		smsMessage.setMessageId(SmsMessageId.MSG_TOPUPS_AMOUNT_SCORING);
		smsMessage.setMinAllowedDays(rasCriteria.getMinTopUpsDuration());
		smsMessage.setMinAllowedTopupsAmount(rasCriteria.getMinTopUpValue());

		subscriberAssessment.setMaxBorrowableAmount(null);
		subscriberAssessment.setSmsMessage(smsMessage);

		return initializeResponse(false, subscriberAssessment);
	}

	/**
	 * Determine if {@link Subscriber} has recharged the required number of times for this {@link BorrowableAmount}.
	 * 
	 * @param subscriberAssessment subscribers assessment info
	 * @param rasCriteria criteria defined for amount
	 * @param borrowableAmount configured amount which could be borrowed
	 * @param subscriberHistories subscribers transactions
	 * @param status assessment status so far
	 * @return true if {@link Subscriber} satisfies minimum number of top-ups requirement
	 */
	private Map<String, Object> numberOfTopupsForSpecifiedDuration(SubscriberAssessment subscriberAssessment, 
			RasCriteria rasCriteria, BorrowableAmount borrowableAmount, 
			List<SubscriberHistory> subscriberHistories, boolean status){
		
		if (!applicationBean.isAssessTopupFrequency())
			return initializeResponse(true, subscriberAssessment);

		for (SubscriberHistory subscriberHistory : subscriberHistories) {
			Long days = getDays(subscriberHistory.getRechargeTime());
			if(days <= rasCriteria.getMinTopUpsDuration()) // gets top ups that happened with the specified days limit
				subscriberAssessment.setNumberOfTopUps(subscriberAssessment.getNumberOfTopUps() + 1);
		}

		if(subscriberAssessment.getNumberOfTopUps().compareTo(rasCriteria.getMinTopUps()) >= 0){
			if (status){
				subscriberAssessment.setSmsMessage(null);
				subscriberAssessment.setMaxBorrowableAmount(borrowableAmount);
			}
			return initializeResponse(true, subscriberAssessment);
		}

		SmsMessage smsMessage = getSmsMessage(subscriberAssessment);
		smsMessage.setTopupsLeft(rasCriteria.getMinTopUps() - subscriberAssessment.getNumberOfTopUps());
		smsMessage.setMessageId(SmsMessageId.MSG_TOPUPS_AMOUNT_SCORING_SIMULATE);
		smsMessage.setMinAllowedTopups(rasCriteria.getMinTopUps());
		smsMessage.setMinAllowedDays(rasCriteria.getMinTopUpsDuration());

		subscriberAssessment.setMaxBorrowableAmount(null);
		subscriberAssessment.setSmsMessage(smsMessage);

		return initializeResponse(false, subscriberAssessment);
	}

	/**
	 * Determine if {@link Subscriber} has spent the required age on the network.
	 * 
	 * @param subscriber subscriber details
	 * @param subscriberAssessment subscribers assessment info
	 * @param rasCriteria criteria defined for amount
	 * @param borrowableAmount configured amount which could be borrowed
	 * @param subscriberHistories subscribers transactions
	 * @param status assessment status so far
	 * @return true if {@link Subscriber} satisfies age on network requirement
	 */
	private Map<String, Object> ageOnNetwork(Subscriber subscriber, 
			SubscriberAssessment subscriberAssessment, 
			RasCriteria rasCriteria, BorrowableAmount borrowableAmount, 
			List<SubscriberHistory> subscriberHistories, boolean status){
		
		if (!applicationBean.isAssessAgeOnNetwork())
			return initializeResponse(true, subscriberAssessment);

		if(subscriberAssessment.getAgeOnNetwork().compareTo(rasCriteria.getMinAgeOnNetwork()) >= 0){
			if (status){
				subscriberAssessment.setMaxBorrowableAmount(borrowableAmount);
				subscriberAssessment.setSmsMessage(null);
			}
			return initializeResponse(true, subscriberAssessment);
		}

		SmsMessage smsMessage = getSmsMessage(subscriberAssessment);
		smsMessage.setDaysLeft(rasCriteria.getMinAgeOnNetwork() - subscriberAssessment.getAgeOnNetwork());
		smsMessage.setMessageId(SmsMessageId.MSG_NETWORK_LIFETIME_SCORING);
		smsMessage.setMinAllowedDays(rasCriteria.getMinTopUpsDuration());

		subscriberAssessment.setMaxBorrowableAmount(null);
		subscriberAssessment.setSmsMessage(smsMessage);

		return initializeResponse(false, subscriberAssessment);
	}

	/**
	 * Confirm {@link Subscriber} blacklist status on the network.
	 * 
	 * @param subscriberAssessment subscribers assessment info
	 * @param status assessment status so far
	 * @return true if {@link Subscriber} is not black listed
	 */
	private Map<String, Object> blacklistStatus(SubscriberAssessment subscriberAssessment, 
			boolean status){

		if (!applicationBean.isAssessBlacklistStatus())
			return initializeResponse(true, subscriberAssessment);
		
		return initializeResponse(true, subscriberAssessment);
	}

	/**
	 * Confirm {@link Subscriber} tarrifPlan conforms to expected criteria.
	 * 
	 * @param subscriberAssessment subscribers assessment info
	 * @param status assessment status so far
	 * @return true if {@link Subscriber} tariff plan satisfies criteria requirement
	 */
	private Map<String, Object> tarrifPlan(SubscriberAssessment subscriberAssessment, 
			boolean status){

		if (!applicationBean.isAssessTarrifplan())
			return initializeResponse(true, subscriberAssessment);
		
		return initializeResponse(true, subscriberAssessment);
	}

	/**
	 * Create response for Assessment APIs.
	 * 
	 * @param eligible eligibility status
	 * @param subscriberAssessment subscribers assessment info
	 * @return Map containing eligibility status and updated {@link SubscriberAssessment}
	 */
	private Map<String, Object> initializeResponse(Boolean eligible, 
			SubscriberAssessment subscriberAssessment){

		Map<String, Object> map = new HashMap<>();
		map.put("eligible", eligible);
		map.put("subscriberAssessment", subscriberAssessment);

		return map;
	}

	/**
	 * Fetch existing SmsMessage or initialize new instance.
	 * 
	 * @param subscriberAssessment subscribers assessment info
	 * @return {@link SmsMessage}
	 */
	protected SmsMessage getSmsMessage(SubscriberAssessment subscriberAssessment){

		return subscriberAssessment.getSmsMessage() == null ? new SmsMessage() : subscriberAssessment.getSmsMessage();
	}

	/**
	 * Calculate number of days between rechargeTime day and today.
	 * 
	 * @param rechargeTime recharge time
	 * @return long
	 */
	protected Long getDays(Timestamp rechargeTime){

		return ChronoUnit.DAYS.between(new Date(rechargeTime.getTime()).toLocalDate(), LocalDate.now());
		//return TimeUnit.MILLISECONDS.toDays(rechargeTime.getTime() - subscriberAssessment.getAssessmentInitTime().getTime());
	}

}