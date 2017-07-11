package com.nano.ras.tools;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.ejb.AccessTimeout;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.jboss.logging.Logger;

import com.nano.jpa.entity.Subscriber;
import com.nano.jpa.entity.SubscriberHistory;
import com.nano.jpa.entity.SubscriberHistory_;
import com.nano.jpa.entity.SubscriberState;
import com.nano.jpa.entity.SubscriberState_;
import com.nano.jpa.entity.Subscriber_;
import com.nano.jpa.entity.ras.BorrowableAmount;
import com.nano.jpa.entity.ras.BorrowableAmount_;
import com.nano.jpa.entity.ras.SubscriberAssessment;
import com.nano.jpa.entity.ras.SubscriberAssessment_;
import com.nano.jpa.enums.ActiveStatus;
import com.nano.jpa.enums.PayType;

@Singleton
@AccessTimeout(unit = TimeUnit.MINUTES, value = 3)
public class DbManager {
	
	private Logger log = Logger.getLogger(getClass());

	private CriteriaBuilder criteriaBuilder ;

	@PersistenceContext
	private EntityManager entityManager ;

	@PostConstruct
	public void init(){
		criteriaBuilder = entityManager.getCriteriaBuilder();
	}
	
	/**
	 * Merge the state of the given entity into the current {@link PersistenceContext}.
	 * 
	 * @param entity entity instance
	 * @return the managed instance that the state was merged to
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public <T> Object updateWithNewTransaction(T entity){

		entityManager.merge(entity);
		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}

		return null;
	}
	
	/**
	 * Merge the state of the given entity into the current {@link PersistenceContext}.
	 * 
	 * @param entity
	 * @return the managed instance that the state was merged to
	 */
	public <T> Object update(T entity){

		entityManager.merge(entity);
		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}

		return null;
	}
	
	/**
	 * Fetch persisted entity instance by it {@link PrimaryKey}.
	 * 
	 * @param clazz class type reference
	 * @param pk primary key of entity
	 * @return persisted entity instance
	 */
	public <T> Object getByPk(Class<T> clazz, 
			long pk){

		CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(clazz);
		Root<T> root = criteriaQuery.from(clazz);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.and(criteriaBuilder.equal(root.get("pk"), pk)), 
				criteriaBuilder.equal(root.get("deleted"), false));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("No " + clazz.getCanonicalName() + " exists with the pk:" + pk);
		}

		return null;
	}
	
	/**
	 * Persist entity and add entity instance to {@link EntityManager}.
	 * 
	 * @param entity entity instance
	 * @return persisted entity instance
	 */
	public <T> Object create(T entity){

		entityManager.persist(entity);

		try {
			return entity;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("", e);
		}
		return null;
	}
	
	/**
	 * Fetch ordered {@link BorrowableAmount} list.
	 * 
	 * @return list of borrowableAmounts
	 */
	public List<BorrowableAmount> getBorrowableAmountListDesc(){

		CriteriaQuery<BorrowableAmount> criteriaQuery = criteriaBuilder.createQuery(BorrowableAmount.class);
		Root<BorrowableAmount> root = criteriaQuery.from(BorrowableAmount.class);

		criteriaQuery.select(root);
		criteriaQuery.orderBy(criteriaBuilder.asc(root.get(BorrowableAmount_.amount)));

		try {
			return entityManager.createQuery(criteriaQuery).getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("No borrowableAmount instance found");
		}

		return Collections.emptyList();
	}
	
	/**
	 * Creates or fetches a unique {@link Subscriber} record.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return {@link Subscriber}
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public Subscriber createSubscriber(String msisdn){

		Subscriber subscriber = getSubscriberByMsisdn(formatMisisdn(msisdn));

		if (subscriber != null)
			return subscriber;

		subscriber = new Subscriber();
		subscriber.setInDebt(false);
		subscriber.setAutoRecharge(false);
		subscriber.setMsisdn(formatMisisdn(msisdn));

		return (Subscriber) create(subscriber);
	}
	
	/**
	 * Format MSISDN to acceptable syntax.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return formatted MSISDN
	 */
	public String formatMisisdn(String msisdn){

		if (msisdn.startsWith("234"))
			msisdn = "0" + msisdn.substring(3, msisdn.length());

		if (msisdn.startsWith("+234"))
			msisdn = "0" + msisdn.substring(4, msisdn.length());

		if (!msisdn.startsWith("0"))
			msisdn = "0" + msisdn;

		return msisdn;
	}
	
	/**
	 * Fetch {@link Subscriber} by MSISDN property.
	 * 
	 * @param msisdn subscriber uique MSISDN
	 * @return {@link Subscriber}
	 */
	public Subscriber getSubscriberByMsisdn(String msisdn){

		CriteriaQuery<Subscriber> criteriaQuery = criteriaBuilder.createQuery(Subscriber.class);
		Root<Subscriber> root = criteriaQuery.from(Subscriber.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(Subscriber_.msisdn), formatMisisdn(msisdn)));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("No subscriber instance was found with msisdn:" + msisdn);;
		}

		return null;
	}
	
	/**
	 * Fetch {@link SubscriberState} by MSISDN.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return {@link SubscriberState}
	 */
	public SubscriberState getSubscriberStateByMsisdn(String msisdn){

		CriteriaQuery<SubscriberState> criteriaQuery = criteriaBuilder.createQuery(SubscriberState.class);
		Root<SubscriberState> root = criteriaQuery.from(SubscriberState.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(SubscriberState_.msisdn), formatMisisdn(msisdn)));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriberState instance was found with msisdn:" + msisdn);
		}

		return null;
	}
	
	/**
	 * Fetch {@link SubscriberHistory} by {@link Subscriber} property.
	 *
	 * @param msisdn subscriber unique MSISDN
	 * @return list of {@link SubscriberHistory}
	 */
	public List<SubscriberHistory> getSubscriberHistoryByMsisdn(String msisdn){
		
		CriteriaQuery<SubscriberHistory> criteriaQuery = criteriaBuilder.createQuery(SubscriberHistory.class);
		Root<SubscriberHistory> root = criteriaQuery.from(SubscriberHistory.class);
		
		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(SubscriberHistory_.msisdn), msisdn));
		
		try {
			return entityManager.createQuery(criteriaQuery).getResultList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriberHistory instance found for msisdn:" + msisdn);
		}
		
		return Collections.emptyList();
	}
	
	/**
	 * Create {@link SubscriberState}.
	 * 
	 * @param msisdn
	 * @param currentBalance
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public void createSubscriberState(String msisdn, 
			BigDecimal currentBalance) {
		// TODO Auto-generated method stub

		SubscriberState subscriberState = new SubscriberState();
		subscriberState.setActiveStatus(ActiveStatus.ACTIVE);
		subscriberState.setBlacklisted(false);
		subscriberState.setCurrentBalance(currentBalance);
		subscriberState.setLastUpdated(Timestamp.valueOf(LocalDateTime.now()));
		subscriberState.setMsisdn(formatMisisdn(msisdn));
		subscriberState.setPayType(PayType.PREPAID);
		
		create(subscriberState);
	}
	
	/**
	 * Create a fresh SubscriberAssessment.
	 * 
	 * @param subscriber
	 * @param subscriberState
	 * @return {@link SubscriberAssessment}
	 */
	@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
	public SubscriberAssessment createNewAssessment(Subscriber subscriber, 
			SubscriberState subscriberState){

		SubscriberAssessment subscriberAssessment = getSubscriberAssessmentBySubscriber(subscriber);
		if (subscriberAssessment != null)
			return subscriberAssessment;

		subscriberAssessment = new SubscriberAssessment();
		subscriberAssessment.setAgeOnNetwork(0);
		subscriberAssessment.setInDebt(subscriber.isInDebt());
		subscriberAssessment.setLastProcessed(Timestamp.valueOf(LocalDateTime.now()));
		subscriberAssessment.setNumberOfTopUps(0);
		subscriberAssessment.setSubscriber(subscriber);
		subscriberAssessment.setTopUpDuration(0);
		subscriberAssessment.setTopUpValueDuration(0);
		subscriberAssessment.setTotalTopUpValue(0);

		if(subscriberState != null)
			subscriberAssessment.setTariffPlan(subscriberState.getPayType());

		return (SubscriberAssessment) create(subscriberAssessment);
	}
	
	/**
	 * Fetch SubscriberAssessment by {@link Subscriber}.
	 * 
	 * @param subscriber subscriber details
	 * @return {@link SubscriberAssessment}
	 */
	public SubscriberAssessment getSubscriberAssessmentBySubscriber(Subscriber subscriber) {
		// TODO Auto-generated method stub

		CriteriaQuery<SubscriberAssessment> criteriaQuery = criteriaBuilder.createQuery(SubscriberAssessment.class);
		Root<SubscriberAssessment> root = criteriaQuery.from(SubscriberAssessment.class);

		criteriaQuery.select(root);
		criteriaQuery.where(criteriaBuilder.equal(root.get(SubscriberAssessment_.subscriber), subscriber));

		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriberAssessment instance found for subscriber:" + subscriber.getPk());
		}

		return null;
	}
	
	/**
	 * Fetch earliest Time stamp from {@link SubscriberHistory} by {@link Subscriber} property.
	 *
	 * @param msisdn
	 * @return {@link Timestamp}
	 */
	public Timestamp getEarliestSubscriberHistoryTimeByMsisdn(String msisdn){
		
		CriteriaQuery<Timestamp> criteriaQuery = criteriaBuilder.createQuery(Timestamp.class);
		Root<SubscriberHistory> root = criteriaQuery.from(SubscriberHistory.class);
		
		criteriaQuery.select(criteriaBuilder.least(root.get(SubscriberHistory_.rechargeTime)));
		criteriaQuery.where(criteriaBuilder.equal(root.get(SubscriberHistory_.msisdn), msisdn));
		
		try {
			return entityManager.createQuery(criteriaQuery).getSingleResult();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("No subscriberHistory instance found for msisdn:" + msisdn);
		}
		
		return null;
	}

}
