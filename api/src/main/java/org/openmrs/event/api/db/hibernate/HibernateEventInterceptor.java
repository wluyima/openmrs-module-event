package org.openmrs.event.api.db.hibernate;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.Transaction;
import org.hibernate.collection.PersistentCollection;
import org.hibernate.type.Type;
import org.openmrs.OpenmrsObject;
import org.openmrs.Retireable;
import org.openmrs.Voidable;
import org.openmrs.event.Event;
import org.openmrs.event.Event.Action;

/**
 * A hibernate {@link Interceptor} implementation, intercepts any database inserts, updates and
 * deletes in a single hibernate session and fires the necessary events. Any changes/inserts/deletes
 * made to the DB that are not made through the application won't be detected by the module.
 *
 * We use a Stack here to handle any nested transactions that may occur within a single thread
 */
public class HibernateEventInterceptor extends EmptyInterceptor {
	
	private static final long serialVersionUID = 1L;
	
	protected final Log log = LogFactory.getLog(HibernateEventInterceptor.class);
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> inserts = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> updates = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> deletes = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> retiredObjects = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> unretiredObjects = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> voidedObjects = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> unvoidedObjects = new ThreadLocal<Stack<HashSet<OpenmrsObject>>>();
	
	// ======= Convenience accessor methods for the ThreadLocals that do the necessary initialization ===========
	/**
	 * @return the inserts
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getInserts() {
		if (inserts.get() == null) {
			inserts.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (inserts.get().isEmpty())
			inserts.get().push(new HashSet<OpenmrsObject>());
		
		return inserts;
	}
	
	/**
	 * @return the updates
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getUpdates() {
		if (updates.get() == null) {
			updates.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (updates.get().isEmpty())
			updates.get().push(new HashSet<OpenmrsObject>());
		
		return updates;
	}
	
	/**
	 * @return the deletes
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getDeletes() {
		if (deletes.get() == null) {
			deletes.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (deletes.get().isEmpty())
			deletes.get().push(new HashSet<OpenmrsObject>());
		
		return deletes;
	}
	
	/**
	 * @return the retiredObjects
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getRetiredObjects() {
		if (retiredObjects.get() == null) {
			retiredObjects.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (retiredObjects.get().isEmpty())
			retiredObjects.get().push(new HashSet<OpenmrsObject>());
		
		return retiredObjects;
	}
	
	/**
	 * @return the unretiredObjects
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getUnretiredObjects() {
		if (unretiredObjects.get() == null) {
			unretiredObjects.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (unretiredObjects.get().isEmpty())
			unretiredObjects.get().push(new HashSet<OpenmrsObject>());
		
		return unretiredObjects;
	}
	
	/**
	 * @return the voidedObjects
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getVoidedObjects() {
		if (voidedObjects.get() == null) {
			voidedObjects.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (voidedObjects.get().isEmpty())
			voidedObjects.get().push(new HashSet<OpenmrsObject>());
		
		return voidedObjects;
	}
	
	/**
	 * @return the unvoidedObjects
	 */
	private ThreadLocal<Stack<HashSet<OpenmrsObject>>> getUnvoidedObjects() {
		if (unvoidedObjects.get() == null) {
			unvoidedObjects.set(new Stack<HashSet<OpenmrsObject>>());
		}
		if (unvoidedObjects.get().isEmpty())
			unvoidedObjects.get().push(new HashSet<OpenmrsObject>());
		
		return unvoidedObjects;
	}
	
	// ======= END Convenience accessor methods ===========
	
	/**
	 * @see org.hibernate.EmptyInterceptor#onSave(java.lang.Object, java.io.Serializable,
	 *      java.lang.Object[], java.lang.String[], org.hibernate.type.Type[])
	 */
	@Override
	public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
		if (entity instanceof OpenmrsObject) {
			getInserts().get().peek().add((OpenmrsObject) entity);
		}
		
		//tells hibernate that there are no changes made here that 
		//need to be propagated to the persistent object and DB
		return false;
	}
	
	/**
	 * @see org.hibernate.EmptyInterceptor#onFlushDirty(java.lang.Object, java.io.Serializable,
	 *      java.lang.Object[], java.lang.Object[], java.lang.String[], org.hibernate.type.Type[])
	 */
	@Override
	public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState,
	                            String[] propertyNames, Type[] types) {
		
		if (entity instanceof OpenmrsObject) {
			OpenmrsObject object = (OpenmrsObject) entity;
			getUpdates().get().peek().add(object);
			//Fire events for retired/unretired and voided/unvoided objects
			if (entity instanceof Retireable || entity instanceof Voidable) {
				for (int i = 0; i < propertyNames.length; i++) {
					String auditableProperty = (entity instanceof Retireable) ? "retired" : "voided";
					if (auditableProperty.equals(propertyNames[i])) {
						boolean previousValue = false;
						if (previousState != null && previousState[i] != null)
							previousValue = Boolean.valueOf(previousState[i].toString());
						
						boolean currentValue = false;
						if (currentState != null && currentState[i] != null)
							currentValue = Boolean.valueOf(currentState[i].toString());
						
						if (previousValue != currentValue) {
							if ("retired".equals(auditableProperty)) {
								if (previousValue)
									getUnretiredObjects().get().peek().add(object);
								else
									getRetiredObjects().get().peek().add(object);
							} else {
								if (previousValue)
									getUnvoidedObjects().get().peek().add(object);
								else
									getVoidedObjects().get().peek().add(object);
							}
						}
						
						break;
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * @see org.hibernate.EmptyInterceptor#onDelete(java.lang.Object, java.io.Serializable,
	 *      java.lang.Object[], java.lang.String[], org.hibernate.type.Type[])
	 */
	@Override
	public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
		if (entity instanceof OpenmrsObject) {
			getDeletes().get().peek().add((OpenmrsObject) entity);
		}
	}
	
	/**
	 * @see org.hibernate.EmptyInterceptor#onCollectionUpdate(java.lang.Object,
	 *      java.io.Serializable)
	 */
	@Override
	public void onCollectionUpdate(Object collection, Serializable key) throws CallbackException {
		if (collection != null) {
			//If a collection element has been added/removed, fire an update event for the parent entity
			Object owningObject = ((PersistentCollection) collection).getOwner();
			if (owningObject instanceof OpenmrsObject) {
				getUpdates().get().peek().add((OpenmrsObject) owningObject);
			}
		}
	}
	
	/**
	 * @see org.hibernate.EmptyInterceptor#afterTransactionCompletion(org.hibernate.Transaction)
	 */
	@Override
	public void afterTransactionCompletion(Transaction tx) {
		
		try {
			if (tx.wasCommitted()) {
				for (OpenmrsObject delete : getDeletes().get().peek()) {
					Event.fireAction(Action.PURGED.name(), delete);
				}
				for (OpenmrsObject insert : getInserts().get().peek()) {
					Event.fireAction(Action.CREATED.name(), insert);
				}
				for (OpenmrsObject update : getUpdates().get().peek()) {
					Event.fireAction(Action.UPDATED.name(), update);
				}
				for (OpenmrsObject retired : getRetiredObjects().get().peek()) {
					Event.fireAction(Action.RETIRED.name(), retired);
				}
				for (OpenmrsObject unretired : getUnretiredObjects().get().peek()) {
					Event.fireAction(Action.UNRETIRED.name(), unretired);
				}
				for (OpenmrsObject voided : getVoidedObjects().get().peek()) {
					Event.fireAction(Action.VOIDED.name(), voided);
				}
				for (OpenmrsObject unvoided : getUnvoidedObjects().get().peek()) {
					Event.fireAction(Action.UNVOIDED.name(), unvoided);
				}
			}
		}
		finally {
			//cleanup if necessary
			if (inserts.get() != null && !inserts.get().isEmpty())
				inserts.get().pop();
			
			if (updates.get() != null && !updates.get().isEmpty())
				updates.get().pop();
			
			if (deletes.get() != null && !deletes.get().isEmpty())
				deletes.get().pop();
			
			if (retiredObjects.get() != null && !retiredObjects.get().isEmpty())
				retiredObjects.get().pop();
			
			if (unretiredObjects.get() != null && !unretiredObjects.get().isEmpty())
				unretiredObjects.get().pop();
			
			if (voidedObjects.get() != null && !voidedObjects.get().isEmpty())
				voidedObjects.get().pop();
			
			if (unvoidedObjects.get() != null && !unvoidedObjects.get().isEmpty())
				unvoidedObjects.get().pop();
		}
	}
}
