package mammoth.jclient;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ResultSet implements Serializable {
    /**
     * ID to serial
     */
    private static final long serialVersionUID = 10009L;

    public enum ScoreMode {
        ADD, PROD,
    }

    public ResultSet(ScoreMode m) {
        mode = m;
    }

    // Result Set Size
    private AtomicLong size = new AtomicLong(0);
    private ScoreMode mode;

    private List<Result> results;
    private HashMap<String, Result> __results = new HashMap<String, Result>();

    public static class Result implements Serializable {
        /**
         * ID to serial
         */
        private static final long serialVersionUID = 100008L;
        private String value;
        private float score;
        private float auxScore;

        public Result(String value, float score) {
            this.value = value;
            this.score = score;
        }

        public Result(float score) {
            this.score = score;
        }

        public Result(float score, float auxScore) {
            this.score = score;
            this.auxScore = auxScore;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public float getScore() {
            return score;
        }

        public void setScore(float score) {
            this.score = score;
        }

        public float getAuxScore() {
            return auxScore;
        }

        public void setAuxScore(float score, ScoreMode mode) {
            if (score >= 0) {
                switch (mode) {
                    case ADD:
                        auxScore += score;
                        break;
                    case PROD:
                        auxScore *= score;
                        break;
                }
            }
        }
    }

    public void updateScore(Result r, float score) {
        if (score >= 0) {
            synchronized (r) {
                switch (mode) {
                    case ADD:
                        r.setScore(score + r.getScore());
                        break;
                    case PROD:
                        r.setScore(score * r.getScore());
                        break;
                }
            }
        }
    }


    public long getSize() {
        return size.get();
    }

    public void setSize(long size) {
        this.size.set(size);
    }

    public void addToResults(Result r) {
        synchronized (this) {
            if (__results.containsKey(r.value)) {
                updateScore(__results.get(r.value), r.score);
            } else {
                __results.put(r.value, r);
                size.incrementAndGet();
            }
        }
    }

    public void addAll(ResultSet rs) {
        // add Results to this set
        synchronized (this) {
            if (rs.getSize() > 0) {
                for (Result r : rs.getResults()) {
                    addToResults(r);
                }
            }
        }
    }

    private final static Comparator<Result> comp = new Comparator<Result>() {
        @Override
        public int compare(Result a, Result b) {
            return ((b.score - a.score) > 0) ? -1 : 1;
        }
    };

    protected void doSort() {
        synchronized (this) {
            if (__results.size() > 0) {
                results = new LinkedList<Result>();
                results.addAll(__results.values());
                Collections.sort(results, comp);
            }
        }
    }

    public List<Result> getResults() {
        doSort();
        return results;
    }

    public void shrink(int size) {
        doSort();
        // shrink result set to new size
        if (results != null && results.size() > size) {
            synchronized (this) {
                results.subList(size, results.size()).clear();
            }
        }
    }

    public ScoreMode getMode() {
        return mode;
    }

    public void setMode(ScoreMode mode) {
        this.mode = mode;
    }

    public String toString() {
        String str = "";

        getResults();
        if (results != null && results.size() > 0) {
            for (Result r : results) {
                str += " -> " + r.value + " : " + r.score + "\n";
            }
        }
        return str;
    }
}
