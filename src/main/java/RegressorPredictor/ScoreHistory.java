package RegressorPredictor;

import java.util.List;

public class ScoreHistory {
    private List<Float> history;

    public ScoreHistory(){}

    public List<Float> getHistory() {
        return history;
    }

    public void setHistory(List<Float> history) {
        this.history = history;
    }

    @Override
    public String toString() {
        return "PurchaseHistory{" +
                "history=" + history +
                '}';
    }
}
