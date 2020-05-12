package mammoth.server;

import net.semanticmetadata.lire.ImageSearchHits;
import net.semanticmetadata.lire.impl.SimpleResult;
import org.apache.lucene.document.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class ZHImageSearchHits implements ImageSearchHits {
    int maximumHits = 2500;
    float maxDistance = -1.0F;
    ArrayList<SimpleResult> results;

    public ZHImageSearchHits(Collection<SimpleResult> results, float maxDistance, int maxHits) {
        this.maximumHits = maxHits;
        this.results = new ArrayList<SimpleResult>(results.size());
        this.results.addAll(results);
        if (maxDistance > this.maxDistance)
            this.maxDistance = maxDistance;
    }

    @Override
    public int length() {
        return results.size();
    }

    @Override
    public float score(int position) {
        return ((SimpleResult) this.results.get(position)).getDistance();
    }

    @Override
    public Document doc(int position) {
        return ((SimpleResult) this.results.get(position)).getDocument();
    }

    public void Merge(ImageSearchHits data) {
        for (int j = 0; j < data.length(); j++) {
            int i = 0;
            for (i = 0; i < length(); i++)
                if (score(i) > data.score(j))
                    break;
            // BUG-XXX: set indexNum to id(J) here
            SimpleResult element = new SimpleResult(data.score(j), data.doc(j), j);
            this.results.add(i, element);
        }
        for (int i = this.maximumHits; i < this.results.size(); ) {
            this.results.remove(i);
        }
        if (this.results.size() > 0)
            this.maxDistance = ((SimpleResult) this.results.get(length() - 1)).getDistance();
    }

    public void normalization() {
        for (Iterator<SimpleResult> iterator = this.results.iterator(); iterator.hasNext(); ) {
            SimpleResult result = (SimpleResult) iterator.next();
            result.setDistance(result.getDistance() / this.maxDistance);
        }
    }
}
