package ma.s2m.fraudmanager.helpers;

import ma.s2m.auth.impl.VirtualRecordTransaction;

public class TransactionDummyHelper {

    public static VirtualRecordTransaction dummyTransaction() {
        VirtualRecordTransaction t = new VirtualRecordTransaction();
        t.setTransactionNo("txn-123");
        t.setAmount(100.0);
        t.setCardId("card-123");
        t.setMerchant("merchant-123");
        return t;
    }

}
