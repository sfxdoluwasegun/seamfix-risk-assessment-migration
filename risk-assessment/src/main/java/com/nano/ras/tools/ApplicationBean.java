package com.nano.ras.tools;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import com.nano.jpa.entity.ras.BorrowableAmount;

/**
 * Application scoped bean for managing global Get and Set methods.
 * 
 * @author segz
 *
 */

@ApplicationScoped
public class ApplicationBean {
	
	private int fetchSize = 25000 ;
	private int breakTime = 5;
	
	private boolean assessTopupFrequency = true ;
	private boolean assessTopupAmount = true ;
	private boolean assessAgeOnNetwork = true ;
	private boolean assessBlacklistStatus = true ;
	private boolean assessTarrifplan = true ;
	
	private List<BorrowableAmount> borrowableAmounts ;

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public int getBreakTime() {
		return breakTime;
	}

	public void setBreakTime(int breakTime) {
		this.breakTime = breakTime;
	}

	public List<BorrowableAmount> getBorrowableAmounts() {
		return borrowableAmounts;
	}

	public void setBorrowableAmounts(List<BorrowableAmount> borrowableAmounts) {
		this.borrowableAmounts = borrowableAmounts;
	}

	public boolean isAssessTarrifplan() {
		return assessTarrifplan;
	}

	public void setAssessTarrifplan(boolean assessTarrifplan) {
		this.assessTarrifplan = assessTarrifplan;
	}

	public boolean isAssessBlacklistStatus() {
		return assessBlacklistStatus;
	}

	public void setAssessBlacklistStatus(boolean assessBlacklistStatus) {
		this.assessBlacklistStatus = assessBlacklistStatus;
	}

	public boolean isAssessAgeOnNetwork() {
		return assessAgeOnNetwork;
	}

	public void setAssessAgeOnNetwork(boolean assessAgeOnNetwork) {
		this.assessAgeOnNetwork = assessAgeOnNetwork;
	}

	public boolean isAssessTopupAmount() {
		return assessTopupAmount;
	}

	public void setAssessTopupAmount(boolean assessTopupAmount) {
		this.assessTopupAmount = assessTopupAmount;
	}

	public boolean isAssessTopupFrequency() {
		return assessTopupFrequency;
	}

	public void setAssessTopupFrequency(boolean assessTopupFrequency) {
		this.assessTopupFrequency = assessTopupFrequency;
	}

}