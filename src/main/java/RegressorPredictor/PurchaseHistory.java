package RegressorPredictor;

import java.math.BigInteger;
import java.util.List;

public class PurchaseHistory {
    private List<Integer> history;

    public PurchaseHistory(){}

    public List<Integer> getHistory() {
        return history;
    }

    public void setHistory(List<Integer> history) {
        this.history = history;
    }

    @Override
    public String toString() {
        return "PurchaseHistory{" +
                "history=" + history +
                '}';
    }
}
