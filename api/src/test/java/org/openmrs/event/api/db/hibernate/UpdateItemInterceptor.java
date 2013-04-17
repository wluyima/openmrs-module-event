package org.openmrs.event.api.db.hibernate;

import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.Transaction;
import org.openmrs.EncounterType;
import org.openmrs.api.EncounterService;
import org.openmrs.api.context.Context;
import org.openmrs.module.event.advice.EventBehaviorTest;
import org.springframework.stereotype.Component;

/**
 * A Test Hibernate {@link Interceptor}, this interceptor's methods always get called after those of
 * {@link HibernateEventInterceptor} because of its bean name
 */
@Component(EventBehaviorTest.TEST_INTERCEPTOR_BEAN_ID)
public class UpdateItemInterceptor extends EmptyInterceptor {
	
	private static final long serialVersionUID = 1L;
	
	public static final Integer ENCOUNTER_TYPE_ID = 1;
	
	public static final String NEW_ENCOUNTER_NAME = "some random encounter type";
	
	//This interceptor is disabled by default, so a test that 
	//needs to use it has to enabled it explicitly via this field
	public static boolean isEnabled = false;
	
	public static boolean skip = false;
	
	/**
	 * @see org.hibernate.EmptyInterceptor#afterTransactionCompletion(org.hibernate.Transaction)
	 */
	@Override
	public void afterTransactionCompletion(Transaction tx) {
		if (isEnabled && !skip) {
			System.out.println("-> In test interceptor...");
			try {
				EncounterService es = Context.getEncounterService();
				EncounterType et = es.getEncounterType(ENCOUNTER_TYPE_ID);
				et.setName(NEW_ENCOUNTER_NAME);
				skip = true;//we need to avoid going in a loop
				es.saveEncounterType(et);
				tx.commit();
			}
			finally {
				skip = false;
			}
		}
	}
}
