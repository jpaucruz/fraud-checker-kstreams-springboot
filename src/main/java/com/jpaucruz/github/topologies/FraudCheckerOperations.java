package com.jpaucruz.github.topologies;

import com.jpaucruz.github.model.Fraud;
import com.jpaucruz.github.model.Movement;

public class FraudCheckerOperations {

    public final static float ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD = 200;
    public final static int MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD = 3;
    public final static int MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD = 4;
    public final static int ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD = 1;

    static Fraud aggMovement(Movement movement, Fraud fraudMovement){
        fraudMovement.addMovement(movement);
        fraudMovement.addDevice(movement);
        fraudMovement.addSites(movement);
        return fraudMovement;
    }

    static Fraud aggMerge(Fraud fraudA, Fraud fraudB){
        return fraudB;
    }

    static boolean isOnlineFraud(Fraud fraudMovement){
        return (fraudMovement.getMovements().size() > MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && fraudMovement.getTotalAmount() > ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD);
    }

    static boolean isPhysicalFraud(Fraud fraudMovement){
        return (fraudMovement.getMovements().size() > MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD || fraudMovement.getDevices().size() > ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD);
    }
    
}
