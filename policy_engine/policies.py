import logging
from sqlalchemy.exc import IntegrityError
from db import SessionLocal # No longer import engine, Base here
from models import Policy

# Removed create_db_tables() function from here

def load_default_policies_from_db(default_policies_data):
    """
    Loads default policies into the database if the 'policies' table is empty.
    This function is intended to be called by init_db.py.
    """
    db = SessionLocal()
    try:
        if db.query(Policy).count() == 0:
            logging.info("No policies found in DB. Loading default policies.")
            for policy_data in default_policies_data:
                try:
                    policy = Policy(
                        name=policy_data["name"],
                        condition=policy_data["condition"],
                        action=policy_data["action"],
                        enabled=policy_data.get("enabled", True)
                    )
                    db.add(policy)
                    db.commit()
                    logging.info(f"Default policy '{policy_data['name']}' loaded.")
                except IntegrityError:
                    db.rollback()
                    logging.warning(f"Default policy '{policy_data['name']}' already exists (integrity error), skipping.")
                except Exception as e:
                    db.rollback()
                    logging.error(f"Error loading default policy '{policy_data['name']}': {e}")
        else:
            logging.info("Policies already exist in DB. Skipping default policy load.")
    finally:
        db.close()

def get_all_policies():
    """Returns all currently configured policies from the database."""
    db = SessionLocal()
    try:
        policies = db.query(Policy).all()
        return [p.to_dict() for p in policies]
    finally:
        db.close()

def add_policy(policy_data):
    """Adds a new policy to the database."""
    db = SessionLocal()
    try:
        if not all(k in policy_data for k in ["name", "condition", "action"]):
            raise ValueError("Policy must contain 'name', 'condition', and 'action'")

        policy = Policy(
            name=policy_data["name"],
            condition=policy_data["condition"],
            action=policy_data["action"],
            enabled=policy_data.get("enabled", True)
        )
        db.add(policy)
        db.commit()
        db.refresh(policy)
        logging.info(f"Policy '{policy.name}' added to DB.")
        return policy.to_dict()
    except IntegrityError:
        db.rollback()
        raise ValueError(f"Policy with name '{policy_data['name']}' already exists.")
    except Exception as e:
        db.rollback()
        logging.error(f"Error adding policy to DB: {e}")
        raise
    finally:
        db.close()

def update_policy(policy_name, new_data):
    """Updates an existing policy in the database."""
    db = SessionLocal()
    try:
        policy = db.query(Policy).filter(Policy.name == policy_name).first()
        if not policy:
            raise ValueError(f"Policy '{policy_name}' not found.")

        for key, value in new_data.items():
            if hasattr(policy, key):
                setattr(policy, key, value)

        db.commit()
        db.refresh(policy)
        logging.info(f"Policy '{policy_name}' updated in DB.")
        return policy.to_dict()
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating policy '{policy_name}' in DB: {e}")
        raise
    finally:
        db.close()

def delete_policy(policy_name):
    """Deletes a policy by name from the database."""
    db = SessionLocal()
    try:
        policy = db.query(Policy).filter(Policy.name == policy_name).first()
        if not policy:
            raise ValueError(f"Policy '{policy_name}' not found.")

        db.delete(policy)
        db.commit()
        logging.info(f"Policy '{policy_name}' deleted from DB.")
        return True
    except Exception as e:
        db.rollback()
        logging.error(f"Error deleting policy '{policy_name}' from DB: {e}")
        raise
    finally:
        db.close()

def evaluate_policies(outage_event):
    """
    Evaluates active policies against a given outage event.
    Retrieves policies from the database.
    Returns a list of actions to be taken.
    """
    actions = []
    db = SessionLocal()
    try:
        # Fetch only enabled policies for evaluation
        active_policies = db.query(Policy).filter(Policy.enabled == True).all()

        event_context = {
            "risk_score": outage_event.get("risk_score", 0.0),
            "status": outage_event.get("status", "UNKNOWN"),
            "component_type": outage_event.get("component_type", "UNKNOWN"),
        }

        for policy in active_policies:
            try:
                # WARNING: Using eval is generally unsafe. For production,
                # consider a more robust and secure expression evaluator
                # library (e.g., asteval, symengine, or a custom parser).
                condition_met = eval(policy.condition, {}, event_context)

                if condition_met:
                    actions.append(policy.action)
                    logging.info(f"Policy '{policy.name}' triggered by event: {outage_event}")
            except Exception as e:
                logging.error(f"Error evaluating policy '{policy.name}': {e}")
    finally:
        db.close()
    return actions
